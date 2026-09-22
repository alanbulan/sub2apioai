package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestCodexTicketProbeRequiresRouteCookie(t *testing.T) {
	response := func(withRouteCookie bool) *http.Response {
		header := http.Header{}
		header.Set(openAICodexTurnStateHeader, fakeCodexTicketState(292))
		header.Add("Set-Cookie", "__cf_bm=bot-test; Path=/; HttpOnly; Secure")
		if withRouteCookie {
			header.Add("Set-Cookie", "__cflb=route-test; Path=/; HttpOnly; Secure")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(strings.NewReader(codexTicketProbeSuccessSSE("gpt-6-astra"))),
		}
	}

	missing, err := inspectOpenAICodexTicketProbeResponse(response(false), "gpt-6-astra", 292)
	require.NoError(t, err)
	require.Equal(t, openAICodexTicketProbeMissingCookie, missing.Verdict)
	require.False(t, missing.verified())

	ready, err := inspectOpenAICodexTicketProbeResponse(response(true), "gpt-6-astra", 292)
	require.NoError(t, err)
	require.True(t, ready.verified())
	require.Equal(t, "route-test", ready.RouteCookies["__cflb"])
}

func TestApplyCodexTicketMergesRouteCookies(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:      true,
		TargetLength: 292,
		FailClosed:   true,
	}, nil)
	account := ticketTestAccount(41)
	now := time.Now()
	svc.storeOpenAICodexTicket(context.Background(), account, &openAICodexTicket{
		AccountID:  account.ID,
		Model:      "gpt-6-astra",
		State:      fakeCodexTicketState(292),
		Length:     292,
		CapturedAt: now,
		ExpiresAt:  now.Add(time.Hour),
	}, nil)

	header := http.Header{"Cookie": []string{"client=keep; __cflb=stale"}}
	require.NoError(t, svc.applyOpenAICodexTicket(context.Background(), account, "gpt-6-astra", header))
	request := &http.Request{Header: header}
	values := map[string]string{}
	for _, cookie := range request.Cookies() {
		values[cookie.Name] = cookie.Value
	}
	require.Equal(t, "keep", values["client"])
	require.Equal(t, "route-test", values["__cflb"])
	require.Equal(t, "origin-test", values["__oailb"])
}

func TestCodexTicketPolicyClampsLegacyHourState(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:      true,
		TargetLength: 292,
		FailClosed:   true,
	}, nil)
	account := ticketTestAccount(41)
	capturedAt := time.Now().Add(-241 * time.Second)
	account.Extra = map[string]any{
		openAICodexRouteCookieExtraKey: &openAICodexRouteCookie{
			AccountID:  account.ID,
			Values:     map[string]string{"__cflb": "legacy-route"},
			CapturedAt: capturedAt,
			ExpiresAt:  capturedAt.Add(time.Hour),
		},
	}
	svc.storeOpenAICodexTicket(context.Background(), account, &openAICodexTicket{
		AccountID:  account.ID,
		Model:      "gpt-6-astra",
		State:      fakeCodexTicketState(292),
		Length:     292,
		CapturedAt: capturedAt,
		ExpiresAt:  capturedAt.Add(time.Hour),
	}, nil)

	header := http.Header{}
	err := svc.applyOpenAICodexTicket(context.Background(), account, "gpt-6-astra", header)
	require.ErrorIs(t, err, ErrOpenAICodexTicketUnavailable)
	require.Empty(t, header.Get(openAICodexTurnStateHeader))
	require.Empty(t, header.Get("Cookie"))
}

func TestCodexTicketRefreshSchedulesOneProbeForSharedCookie(t *testing.T) {
	now := time.Now()
	account := ticketTestAccount(41)
	account.Extra[openAICodexRouteCookieExtraKey] = &openAICodexRouteCookie{
		AccountID:  account.ID,
		Values:     map[string]string{"__cflb": "route-test"},
		CapturedAt: now.Add(-181 * time.Second),
		ExpiresAt:  now.Add(59 * time.Second),
	}
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	var calls atomic.Int64
	upstream := &codexTicketFuncUpstream{do: func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return codexTicketResponse(), nil
	}}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:              true,
		HarvestProxyURL:      "http://proxy.example.com:8080",
		Models:               []string{"gpt-6-astra", "gpt-5.6-sol"},
		TTLSeconds:           240,
		RefreshBeforeSeconds: 60,
	}, upstream)
	svc.accountRepo = repo
	for _, model := range svc.openAICodexTicketConfig().Models {
		svc.storeOpenAICodexTicket(context.Background(), account, &openAICodexTicket{
			AccountID:  account.ID,
			Model:      model,
			State:      fakeCodexTicketState(292),
			Length:     292,
			CapturedAt: now,
			ExpiresAt:  now.Add(240 * time.Second),
		}, nil)
	}

	svc.refreshOpenAICodexTickets(context.Background())

	require.Equal(t, int64(1), calls.Load())
}
