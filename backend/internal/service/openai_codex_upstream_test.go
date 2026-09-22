package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func enabledOpenAICodexTextRelaySettings() *SettingService {
	repo := &codexTicketSettingRepo{codexPolicyMigrationRepoStub: &codexPolicyMigrationRepoStub{values: map[string]string{
		SettingKeyOpenAICodexTextRelayEnabled: "true",
		SettingKeyOpenAICodexTextRelayBaseURL: "https://relay.example.com/backend-api/codex",
	}}}
	return NewSettingService(repo, &config.Config{})
}

func TestOpenAICodexTextRelayRequiresRuntimeSwitch(t *testing.T) {
	repo := &codexTicketSettingRepo{codexPolicyMigrationRepoStub: &codexPolicyMigrationRepoStub{values: map[string]string{
		SettingKeyOpenAICodexTextRelayEnabled: "false",
		SettingKeyOpenAICodexTextRelayBaseURL: "https://relay.example.com/backend-api/codex",
	}}}
	settings := NewSettingService(repo, &config.Config{})
	svc := &OpenAIGatewayService{settingService: settings}

	require.Equal(t, chatgptCodexURL, svc.openAICodexResponsesURL())

	repo.values[SettingKeyOpenAICodexTextRelayEnabled] = "true"
	settings.InvalidateOpenAICodexTextRelaySettingsCache()
	require.Equal(t, "https://relay.example.com/backend-api/codex/responses", svc.openAICodexResponsesURL())

	wsURL, err := svc.buildOpenAIResponsesWSURL(&Account{Type: AccountTypeOAuth})
	require.NoError(t, err)
	require.Equal(t, "wss://relay.example.com/backend-api/codex/responses", wsURL)
}

func TestOpenAICodexTextRelayRoutesHTTPAndCompactRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{settingService: enabledOpenAICodexTextRelaySettings()}
	account := &Account{
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Credentials: map[string]any{"chatgpt_account_id": "account-1"},
	}

	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{name: "responses", path: "/v1/responses", want: "https://relay.example.com/backend-api/codex/responses"},
		{name: "compact", path: "/v1/responses/compact", want: "https://relay.example.com/backend-api/codex/responses/compact"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"model":"gpt-5.6-sol","input":"ping"}`)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(body))

			req, err := svc.buildUpstreamRequest(c.Request.Context(), c, account, body, "token", false, "", false)
			require.NoError(t, err)
			require.Equal(t, tc.want, req.URL.String())
			require.Empty(t, req.Host)
			require.Equal(t, "relay.example.com", req.URL.Host)
		})
	}
}

func TestOpenAICodexTextRelayDoesNotRouteImages(t *testing.T) {
	body := []byte(`{"model":"gpt-image-2","prompt":"draw"}`)
	c, _ := newOpenAIImagesTestContext(t, body)
	upstream := &httpUpstreamRecorder{resp: openAIImagesJSONResponse()}
	svc := newOpenAIImagesTestService(upstream)
	svc.settingService = enabledOpenAICodexTextRelaySettings()
	parsed, err := svc.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)

	result, err := svc.ForwardImages(context.Background(), c, directImagesTestAccount(), body, parsed, "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, upstream.lastReq)
	require.Equal(t, "https://chatgpt.com/backend-api/codex/images/generations", upstream.lastReq.URL.String())
	require.Equal(t, "chatgpt.com", upstream.lastReq.Host)
}

func TestOpenAICodexTextRelayDoesNotRouteTicketHarvest(t *testing.T) {
	upstream := &codexTicketFuncUpstream{do: func(req *http.Request) (*http.Response, error) {
		require.Equal(t, chatgptCodexURL, req.URL.String())
		require.Equal(t, "chatgpt.com", req.Host)
		require.Empty(t, req.Header.Get("Cookie"))
		headers := make(http.Header)
		headers.Set(openAICodexTurnStateHeader, fakeCodexTicketState(292))
		headers.Add("Set-Cookie", "__cflb=route-test; Path=/; HttpOnly; Secure")
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     headers,
			Body:       io.NopCloser(strings.NewReader(codexTicketProbeSuccessSSE("gpt-5.6-sol"))),
		}, nil
	}}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true}, upstream)
	svc.settingService = enabledOpenAICodexTextRelaySettings()

	result, err := svc.fireOpenAICodexTicketProbe(
		context.Background(),
		ticketTestAccount(41),
		"token",
		"gpt-5.6-sol",
		"http://proxy.example:8080",
		5*time.Second,
	)
	require.NoError(t, err)
	require.Equal(t, openAICodexTicketProbeVerified, result.Verdict)
}

func TestSetOpenAICodexRequestHostUsesTargetAuthority(t *testing.T) {
	official, err := http.NewRequest(http.MethodPost, chatgptCodexURL, nil)
	require.NoError(t, err)
	setOpenAICodexRequestHost(official)
	require.Equal(t, "chatgpt.com", official.Host)

	relay, err := http.NewRequest(http.MethodPost, "https://relay.example.com/backend-api/codex/responses", nil)
	require.NoError(t, err)
	relay.Host = "chatgpt.com"
	setOpenAICodexRequestHost(relay)
	require.Empty(t, relay.Host)
	require.Equal(t, "relay.example.com", relay.URL.Host)
}
