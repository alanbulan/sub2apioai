package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestSettingsCodexTicketProxyWriteReadAndHotReload(t *testing.T) {
	key := service.SettingKeyOpenAICodexTicketHarvestProxyURL
	oldProxy := "http://user:old-secret@old.example.com:8080"
	newProxy := "socks5h://user:new-secret@new.example.com:1080"
	h, repo := newStepUpSwitchTestHandler(t, map[string]string{key: oldProxy})
	require.Equal(t, oldProxy, h.settingService.GetOpenAICodexTicketHarvestProxyURL(context.Background()))
	rec := doUpdateSettings(t, h, map[string]any{key: newProxy}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, newProxy, repo.values[key])
	require.Equal(t, newProxy, h.settingService.GetOpenAICodexTicketHarvestProxyURL(context.Background()))
	require.NotContains(t, rec.Body.String(), "new-secret")
	require.Contains(t, rec.Body.String(), `"openai_codex_ticket_harvest_proxy_configured":true`)
	// Omission, empty input and the masked GET value all preserve the real secret.
	for _, body := range []map[string]any{{"site_name": "updated"}, {key: ""}, {key: service.MaskProxyURL(newProxy)}} {
		rec = doUpdateSettings(t, h, body, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, newProxy, repo.values[key])
	}
	get := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(get)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/settings", nil)
	h.GetSettings(c)
	require.Equal(t, http.StatusOK, get.Code)
	require.NotContains(t, get.Body.String(), "new-secret")
	require.Contains(t, get.Body.String(), "new.example.com")
}

func TestSettingsCodexTicketProxyPoolPreservesMaskedSecrets(t *testing.T) {
	storedPool := []service.OpenAICodexTicketProxy{{
		ID:       "primary",
		Name:     "old name",
		URL:      "http://user:pool-secret@pool.example.com:8080",
		Enabled:  true,
		Priority: 0,
		Weight:   10,
	}}
	rawPool, err := service.MarshalOpenAICodexTicketProxyPool(storedPool)
	require.NoError(t, err)
	h, repo := newStepUpSwitchTestHandler(t, map[string]string{
		service.SettingKeyOpenAICodexTicketProxyPool: rawPool,
	})
	masked := service.MaskOpenAICodexTicketProxyPool(storedPool)
	masked[0].Name = "new name"
	masked[0].Weight = 25
	rec := doUpdateSettings(t, h, map[string]any{
		service.SettingKeyOpenAICodexTicketProxyPool: masked,
	}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "pool-secret")

	var persisted []service.OpenAICodexTicketProxy
	require.NoError(t, json.Unmarshal([]byte(repo.values[service.SettingKeyOpenAICodexTicketProxyPool]), &persisted))
	require.Len(t, persisted, 1)
	require.Equal(t, storedPool[0].URL, persisted[0].URL)
	require.Equal(t, "new name", persisted[0].Name)
	require.Equal(t, 25, persisted[0].Weight)
}

func TestSettingsCodexTicketExplicitEmptyPoolClearsLegacyProxy(t *testing.T) {
	legacyURL := "http://user:legacy-secret@legacy.example.com:8080"
	h, repo := newStepUpSwitchTestHandler(t, map[string]string{
		service.SettingKeyOpenAICodexTicketHarvestProxyURL: legacyURL,
	})
	rec := doUpdateSettings(t, h, map[string]any{
		service.SettingKeyOpenAICodexTicketProxyPool: []any{},
	}, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "[]", repo.values[service.SettingKeyOpenAICodexTicketProxyPool])
	require.Empty(t, repo.values[service.SettingKeyOpenAICodexTicketHarvestProxyURL])
	require.NotContains(t, rec.Body.String(), "legacy-secret")
}

func TestSettingsCodexTicketRejectsInvalidRetryPolicy(t *testing.T) {
	h, repo := newStepUpSwitchTestHandler(t, map[string]string{
		service.SettingKeyOpenAICodexTicketRetryIntervalSeconds: "6",
	})
	rec := doUpdateSettings(t, h, map[string]any{
		service.SettingKeyOpenAICodexTicketRetryIntervalSeconds: 0,
	}, nil)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Equal(t, "6", repo.values[service.SettingKeyOpenAICodexTicketRetryIntervalSeconds])
}

func TestSettingsCodexTicketRejectInvalidProxyWithoutLeakingPassword(t *testing.T) {
	key := service.SettingKeyOpenAICodexTicketHarvestProxyURL
	h, repo := newStepUpSwitchTestHandler(t, map[string]string{key: "http://previous.example.com:8080"})
	rec := doUpdateSettings(t, h, map[string]any{key: "ftp://user:invalid-secret@proxy.example.com:21"}, nil)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.NotContains(t, rec.Body.String(), "invalid-secret")
	require.Equal(t, "http://previous.example.com:8080", repo.values[key])
}
