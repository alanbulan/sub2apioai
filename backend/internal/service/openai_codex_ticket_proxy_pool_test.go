package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func testCodexProxy(id string, priority, weight int) OpenAICodexTicketProxy {
	return OpenAICodexTicketProxy{
		ID:       id,
		Name:     id,
		URL:      "http://user:secret@" + id + ".example:8080",
		Enabled:  true,
		Priority: priority,
		Weight:   weight,
	}
}

func TestOpenAICodexTicketProxyPoolUsesPriorityBeforeWeight(t *testing.T) {
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.ProxyPool = []OpenAICodexTicketProxy{
		testCodexProxy("backup", 10, 100),
		testCodexProxy("primary", 0, 1),
	}
	var runtime openAICodexTicketProxyPoolRuntime
	selected, ok := runtime.selectProxy("account-model", settings, time.Now())
	require.True(t, ok)
	t.Cleanup(selected.release)
	require.Equal(t, "primary", selected.proxy.ID)
}

func TestOpenAICodexTicketProxyPoolSmoothWeightedDistribution(t *testing.T) {
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.ProxyPool = []OpenAICodexTicketProxy{
		testCodexProxy("light", 0, 1),
		testCodexProxy("heavy", 0, 3),
	}
	var runtime openAICodexTicketProxyPoolRuntime
	counts := map[string]int{}
	for i := 0; i < 40; i++ {
		selected, ok := runtime.selectProxy("key-"+string(rune(i+1)), settings, time.Now())
		require.True(t, ok)
		counts[selected.proxy.ID]++
		selected.release()
	}
	require.Equal(t, 10, counts["light"])
	require.Equal(t, 30, counts["heavy"])
}

func TestOpenAICodexTicketProxyPoolFailsOverPerKeyAndPriorityTier(t *testing.T) {
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.ProxyPool = []OpenAICodexTicketProxy{
		testCodexProxy("first", 0, 100),
		testCodexProxy("second", 0, 1),
		testCodexProxy("backup", 10, 100),
	}
	var runtime openAICodexTicketProxyPoolRuntime
	ids := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		selected, ok := runtime.selectProxy("same-key", settings, time.Now())
		require.True(t, ok)
		ids = append(ids, selected.proxy.ID)
		selected.release()
	}
	require.ElementsMatch(t, []string{"first", "second"}, ids[:2])
	require.Equal(t, "backup", ids[2])

	selected, ok := runtime.selectProxy("other-key", settings, time.Now())
	require.True(t, ok)
	selected.release()
	require.Contains(t, []string{"first", "second"}, selected.proxy.ID)
}

func TestOpenAICodexTicketPoolFingerprintKeepsBackoffAcrossNodeChanges(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{}, nil)
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.ProxyPool = []OpenAICodexTicketProxy{
		testCodexProxy("first", 0, 1),
		testCodexProxy("second", 0, 1),
	}
	key := openAICodexTicketKey(41, "gpt-6-astra")
	now := time.Now()
	svc.recordOpenAICodexTicketProbeFailureWithSettings(key, settings, now, time.Time{})

	first, _ := svc.openaiCodexTicketProxyPool.selectProxy(key, settings, now)
	first.release()
	second, _ := svc.openaiCodexTicketProxyPool.selectProxy(key, settings, now)
	second.release()
	require.NotEqual(t, first.proxy.ID, second.proxy.ID)
	require.True(t, svc.openAICodexTicketProbeBackedOffForSettings(key, settings, now))

	changed := settings
	changed.ProxyPool = append([]OpenAICodexTicketProxy(nil), settings.ProxyPool...)
	changed.ProxyPool[0].Weight = 2
	require.False(t, svc.openAICodexTicketProbeBackedOffForSettings(key, changed, now))
}

func TestOpenAICodexTicketProxyPoolMaskedCredentialReconciliation(t *testing.T) {
	previous := []OpenAICodexTicketProxy{testCodexProxy("primary", 0, 10)}
	next := MaskOpenAICodexTicketProxyPool(previous)
	next[0].Name = "renamed"
	next[0].Weight = 20
	resolved, err := ReconcileOpenAICodexTicketProxyPool(next, previous)
	require.NoError(t, err)
	require.Equal(t, previous[0].URL, resolved[0].URL)
	require.Equal(t, "renamed", resolved[0].Name)
	require.Equal(t, 20, resolved[0].Weight)

	next[0].ID = "new-id"
	_, err = ReconcileOpenAICodexTicketProxyPool(next, previous)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
}

func TestOpenAICodexTicketProxyPoolLegacyMigrationAndExplicitEmpty(t *testing.T) {
	legacyURL := "http://user:secret@legacy.example:8080"
	legacy := resolveOpenAICodexTicketProxyPool(map[string]string{
		SettingKeyOpenAICodexTicketHarvestProxyURL: legacyURL,
	}, "")
	require.Len(t, legacy, 1)
	require.Equal(t, legacyURL, legacy[0].URL)

	explicitEmpty := resolveOpenAICodexTicketProxyPool(map[string]string{
		SettingKeyOpenAICodexTicketProxyPool:       "[]",
		SettingKeyOpenAICodexTicketHarvestProxyURL: legacyURL,
	}, "")
	require.Empty(t, explicitEmpty)
}

func TestOpenAICodexTicketRuntimeSettingsLoadPolicyAndPool(t *testing.T) {
	pool := []OpenAICodexTicketProxy{testCodexProxy("primary", 0, 10)}
	rawPool, err := json.Marshal(pool)
	require.NoError(t, err)
	repo := &codexTicketSettingRepo{codexPolicyMigrationRepoStub: &codexPolicyMigrationRepoStub{values: map[string]string{
		SettingKeyOpenAICodexTicketProxyPool:                  string(rawPool),
		SettingKeyOpenAICodexTicketRetryCount:                 "12",
		SettingKeyOpenAICodexTicketRetryIntervalSeconds:       "45",
		SettingKeyOpenAICodexTicketSteadyRetryIntervalSeconds: "900",
		SettingKeyOpenAICodexTicketManualRetryCooldownSeconds: "75",
	}}}
	settings := NewSettingService(repo, &config.Config{})
	got := settings.GetOpenAICodexTicketRuntimeSettings(context.Background(), "")
	require.Equal(t, 12, got.RetryCount)
	require.Equal(t, 45, got.RetryIntervalSeconds)
	require.Equal(t, 900, got.SteadyRetryIntervalSeconds)
	require.Equal(t, 75, got.ManualRetryCooldownSeconds)
	require.Equal(t, pool, got.ProxyPool)
}
