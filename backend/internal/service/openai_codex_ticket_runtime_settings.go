package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	OpenAICodexTicketRetryCountDefault                 = 8
	OpenAICodexTicketRetryIntervalSecondsDefault       = 60
	OpenAICodexTicketSteadyRetryIntervalSecondsDefault = 1800
	OpenAICodexTicketManualRetryCooldownSecondsDefault = 60
	OpenAICodexTicketTTLSecondsDefault                 = 240
	OpenAICodexTicketRefreshBeforeSecondsDefault       = 60

	openAICodexTicketProxyPoolMaxSize        = 32
	openAICodexTicketRetryCountMax           = 100
	openAICodexTicketRetryIntervalMax        = 24 * 60 * 60
	openAICodexTicketManualCooldownMax       = 24 * 60 * 60
	openAICodexTicketTTLSecondsMax           = 24 * 60 * 60
	openAICodexTicketRuntimeSettingsCacheTTL = 5 * time.Second
	openAICodexTicketLegacyProxyID           = "legacy"
	openAICodexTicketConfigProxyID           = "config"
)

var openAICodexTicketProxyIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// OpenAICodexTicketProxy is one independently selectable ticket-harvest exit.
// List order is retained and used as the deterministic tie-breaker.
type OpenAICodexTicketProxy struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	URL      string `json:"url"`
	Enabled  bool   `json:"enabled"`
	Priority int    `json:"priority"`
	Weight   int    `json:"weight"`
}

// OpenAICodexTicketRuntimeSettings is the hot-reloadable policy consumed by the
// harvester. Durations are persisted as seconds so the admin API stays simple.
type OpenAICodexTicketRuntimeSettings struct {
	ProxyPool                  []OpenAICodexTicketProxy
	RetryCount                 int
	RetryIntervalSeconds       int
	SteadyRetryIntervalSeconds int
	ManualRetryCooldownSeconds int
	TTLSeconds                 int
	RefreshBeforeSeconds       int
}

func DefaultOpenAICodexTicketRuntimeSettings() OpenAICodexTicketRuntimeSettings {
	return OpenAICodexTicketRuntimeSettings{
		RetryCount:                 OpenAICodexTicketRetryCountDefault,
		RetryIntervalSeconds:       OpenAICodexTicketRetryIntervalSecondsDefault,
		SteadyRetryIntervalSeconds: OpenAICodexTicketSteadyRetryIntervalSecondsDefault,
		ManualRetryCooldownSeconds: OpenAICodexTicketManualRetryCooldownSecondsDefault,
		TTLSeconds:                 OpenAICodexTicketTTLSecondsDefault,
		RefreshBeforeSeconds:       OpenAICodexTicketRefreshBeforeSecondsDefault,
	}
}

func (s OpenAICodexTicketRuntimeSettings) RetryInterval() time.Duration {
	return time.Duration(normalizeOpenAICodexTicketRetryInterval(s.RetryIntervalSeconds)) * time.Second
}

func (s OpenAICodexTicketRuntimeSettings) SteadyRetryInterval() time.Duration {
	return time.Duration(normalizeOpenAICodexTicketSteadyRetryInterval(s.SteadyRetryIntervalSeconds)) * time.Second
}

func (s OpenAICodexTicketRuntimeSettings) ManualRetryCooldown() time.Duration {
	return time.Duration(normalizeOpenAICodexTicketManualCooldown(s.ManualRetryCooldownSeconds)) * time.Second
}

func (s OpenAICodexTicketRuntimeSettings) TTL() time.Duration {
	return time.Duration(normalizeOpenAICodexTicketTTL(s.TTLSeconds)) * time.Second
}

func (s OpenAICodexTicketRuntimeSettings) RefreshBefore() time.Duration {
	ttl := normalizeOpenAICodexTicketTTL(s.TTLSeconds)
	return time.Duration(normalizeOpenAICodexTicketRefreshBefore(s.RefreshBeforeSeconds, ttl)) * time.Second
}

func (s OpenAICodexTicketRuntimeSettings) EnabledProxyCount() int {
	count := 0
	for _, proxy := range s.ProxyPool {
		if proxy.Enabled && strings.TrimSpace(proxy.URL) != "" {
			count++
		}
	}
	return count
}

func (s OpenAICodexTicketRuntimeSettings) Fingerprint() [sha256.Size]byte {
	// Marshal cannot fail for this scalar-only structure. Keeping policy fields in
	// the fingerprint makes an edited retry policy take effect immediately.
	b, _ := json.Marshal(s)
	return sha256.Sum256(b)
}

func normalizeOpenAICodexTicketRetryCount(value int) int {
	if value < 1 || value > openAICodexTicketRetryCountMax {
		return OpenAICodexTicketRetryCountDefault
	}
	return value
}

func normalizeOpenAICodexTicketRetryInterval(value int) int {
	if value < 5 || value > openAICodexTicketRetryIntervalMax {
		return OpenAICodexTicketRetryIntervalSecondsDefault
	}
	return value
}

func normalizeOpenAICodexTicketSteadyRetryInterval(value int) int {
	if value < 30 || value > openAICodexTicketRetryIntervalMax {
		return OpenAICodexTicketSteadyRetryIntervalSecondsDefault
	}
	return value
}

func normalizeOpenAICodexTicketManualCooldown(value int) int {
	if value < 5 || value > openAICodexTicketManualCooldownMax {
		return OpenAICodexTicketManualRetryCooldownSecondsDefault
	}
	return value
}

func normalizeOpenAICodexTicketTTL(value int) int {
	if value < 30 || value > openAICodexTicketTTLSecondsMax {
		return OpenAICodexTicketTTLSecondsDefault
	}
	return value
}

func normalizeOpenAICodexTicketRefreshBefore(value, ttl int) int {
	if value < 5 || value >= ttl {
		fallback := OpenAICodexTicketRefreshBeforeSecondsDefault
		if fallback >= ttl {
			fallback = ttl / 4
		}
		if fallback < 5 {
			fallback = 5
		}
		return fallback
	}
	return value
}

func parseOpenAICodexTicketSettingInt(values map[string]string, key string, fallback int) int {
	raw := strings.TrimSpace(values[key])
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseOpenAICodexTicketRuntimePolicyWithFallback(values map[string]string, fallback OpenAICodexTicketRuntimeSettings) OpenAICodexTicketRuntimeSettings {
	settings := fallback
	settings.RetryCount = normalizeOpenAICodexTicketRetryCount(parseOpenAICodexTicketSettingInt(values, SettingKeyOpenAICodexTicketRetryCount, settings.RetryCount))
	settings.RetryIntervalSeconds = normalizeOpenAICodexTicketRetryInterval(parseOpenAICodexTicketSettingInt(values, SettingKeyOpenAICodexTicketRetryIntervalSeconds, settings.RetryIntervalSeconds))
	settings.SteadyRetryIntervalSeconds = normalizeOpenAICodexTicketSteadyRetryInterval(parseOpenAICodexTicketSettingInt(values, SettingKeyOpenAICodexTicketSteadyRetryIntervalSeconds, settings.SteadyRetryIntervalSeconds))
	settings.ManualRetryCooldownSeconds = normalizeOpenAICodexTicketManualCooldown(parseOpenAICodexTicketSettingInt(values, SettingKeyOpenAICodexTicketManualRetryCooldownSeconds, settings.ManualRetryCooldownSeconds))
	settings.TTLSeconds = normalizeOpenAICodexTicketTTL(parseOpenAICodexTicketSettingInt(values, SettingKeyOpenAICodexTicketTTLSeconds, settings.TTLSeconds))
	settings.RefreshBeforeSeconds = normalizeOpenAICodexTicketRefreshBefore(parseOpenAICodexTicketSettingInt(values, SettingKeyOpenAICodexTicketRefreshBeforeSeconds, settings.RefreshBeforeSeconds), settings.TTLSeconds)
	return settings
}

func parseOpenAICodexTicketRuntimePolicy(values map[string]string) OpenAICodexTicketRuntimeSettings {
	return parseOpenAICodexTicketRuntimePolicyWithFallback(values, DefaultOpenAICodexTicketRuntimeSettings())
}

func normalizeOpenAICodexTicketProxyPool(pool []OpenAICodexTicketProxy) ([]OpenAICodexTicketProxy, error) {
	if len(pool) > openAICodexTicketProxyPoolMaxSize {
		return nil, fmt.Errorf("codex ticket proxy pool may contain at most %d entries", openAICodexTicketProxyPoolMaxSize)
	}
	result := make([]OpenAICodexTicketProxy, 0, len(pool))
	seen := make(map[string]struct{}, len(pool))
	for index, item := range pool {
		item.ID = strings.TrimSpace(item.ID)
		item.Name = strings.TrimSpace(item.Name)
		item.URL = strings.TrimSpace(item.URL)
		if !openAICodexTicketProxyIDPattern.MatchString(item.ID) {
			return nil, fmt.Errorf("codex ticket proxy %d has an invalid id", index+1)
		}
		if _, exists := seen[item.ID]; exists {
			return nil, fmt.Errorf("codex ticket proxy ids must be unique")
		}
		seen[item.ID] = struct{}{}
		if len(item.Name) > 100 {
			return nil, fmt.Errorf("codex ticket proxy %d name is too long", index+1)
		}
		if item.URL == "" {
			return nil, fmt.Errorf("codex ticket proxy %d URL is required", index+1)
		}
		if err := ValidateOpenAICodexTicketHarvestProxyURL(item.URL); err != nil {
			return nil, fmt.Errorf("codex ticket proxy %d: %w", index+1, err)
		}
		if item.Priority < 0 || item.Priority > 100 {
			return nil, fmt.Errorf("codex ticket proxy %d priority must be between 0 and 100", index+1)
		}
		if item.Weight < 1 || item.Weight > 100 {
			return nil, fmt.Errorf("codex ticket proxy %d weight must be between 1 and 100", index+1)
		}
		result = append(result, item)
	}
	return result, nil
}

func ValidateOpenAICodexTicketProxyPool(pool []OpenAICodexTicketProxy) error {
	_, err := normalizeOpenAICodexTicketProxyPool(pool)
	return err
}

func MarshalOpenAICodexTicketProxyPool(pool []OpenAICodexTicketProxy) (string, error) {
	normalized, err := normalizeOpenAICodexTicketProxyPool(pool)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("marshal codex ticket proxy pool: %w", err)
	}
	return string(b), nil
}

func decodeOpenAICodexTicketProxyPool(raw string) ([]OpenAICodexTicketProxy, error) {
	var pool []OpenAICodexTicketProxy
	if err := json.Unmarshal([]byte(raw), &pool); err != nil {
		return nil, errors.New("invalid codex ticket proxy pool JSON")
	}
	return normalizeOpenAICodexTicketProxyPool(pool)
}

func syntheticOpenAICodexTicketProxy(id, name, rawURL string) []OpenAICodexTicketProxy {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" || ValidateOpenAICodexTicketHarvestProxyURL(rawURL) != nil {
		return []OpenAICodexTicketProxy{}
	}
	return []OpenAICodexTicketProxy{{
		ID:       id,
		Name:     name,
		URL:      rawURL,
		Enabled:  true,
		Priority: 0,
		Weight:   100,
	}}
}

func OpenAICodexTicketLegacyProxyPool(rawURL string) []OpenAICodexTicketProxy {
	return syntheticOpenAICodexTicketProxy(openAICodexTicketLegacyProxyID, "Legacy proxy", rawURL)
}

// resolveOpenAICodexTicketProxyPool keeps old installations operational. A
// non-empty pool setting is authoritative, including the explicit JSON value
// "[]". Only an absent setting falls back to the legacy DB value and then YAML.
func resolveOpenAICodexTicketProxyPool(values map[string]string, fallbackURL string) []OpenAICodexTicketProxy {
	if raw, exists := values[SettingKeyOpenAICodexTicketProxyPool]; exists && strings.TrimSpace(raw) != "" {
		if pool, err := decodeOpenAICodexTicketProxyPool(raw); err == nil {
			return pool
		}
		return []OpenAICodexTicketProxy{}
	}
	if legacy := strings.TrimSpace(values[SettingKeyOpenAICodexTicketHarvestProxyURL]); legacy != "" {
		return syntheticOpenAICodexTicketProxy(openAICodexTicketLegacyProxyID, "Legacy proxy", legacy)
	}
	return syntheticOpenAICodexTicketProxy(openAICodexTicketConfigProxyID, "Configured proxy", fallbackURL)
}

func MaskOpenAICodexTicketProxyPool(pool []OpenAICodexTicketProxy) []OpenAICodexTicketProxy {
	if len(pool) == 0 {
		return []OpenAICodexTicketProxy{}
	}
	masked := make([]OpenAICodexTicketProxy, 0, len(pool))
	for _, item := range pool {
		item.URL = MaskProxyURL(item.URL)
		masked = append(masked, item)
	}
	return masked
}

// ReconcileOpenAICodexTicketProxyPool replaces API password masks with the
// stored URL having the same stable ID. It never accepts a mask for a new ID.
func ReconcileOpenAICodexTicketProxyPool(next, previous []OpenAICodexTicketProxy) ([]OpenAICodexTicketProxy, error) {
	previousByID := make(map[string]OpenAICodexTicketProxy, len(previous))
	for _, item := range previous {
		previousByID[item.ID] = item
	}
	resolved := make([]OpenAICodexTicketProxy, len(next))
	for i, item := range next {
		item.ID = strings.TrimSpace(item.ID)
		item.URL = strings.TrimSpace(item.URL)
		if IsMaskedProxyURL(item.URL) {
			stored, ok := previousByID[item.ID]
			if !ok || strings.TrimSpace(stored.URL) == "" {
				return nil, fmt.Errorf("codex ticket proxy %d has no stored credential to preserve", i+1)
			}
			item.URL = stored.URL
		}
		resolved[i] = item
	}
	return normalizeOpenAICodexTicketProxyPool(resolved)
}

func validateOpenAICodexTicketRuntimePolicy(settings OpenAICodexTicketRuntimeSettings) error {
	if settings.RetryCount < 1 || settings.RetryCount > openAICodexTicketRetryCountMax {
		return fmt.Errorf("%s must be between 1 and %d", SettingKeyOpenAICodexTicketRetryCount, openAICodexTicketRetryCountMax)
	}
	if settings.RetryIntervalSeconds < 5 || settings.RetryIntervalSeconds > openAICodexTicketRetryIntervalMax {
		return fmt.Errorf("%s must be between 5 and %d", SettingKeyOpenAICodexTicketRetryIntervalSeconds, openAICodexTicketRetryIntervalMax)
	}
	if settings.SteadyRetryIntervalSeconds < 30 || settings.SteadyRetryIntervalSeconds > openAICodexTicketRetryIntervalMax {
		return fmt.Errorf("%s must be between 30 and %d", SettingKeyOpenAICodexTicketSteadyRetryIntervalSeconds, openAICodexTicketRetryIntervalMax)
	}
	if settings.ManualRetryCooldownSeconds < 5 || settings.ManualRetryCooldownSeconds > openAICodexTicketManualCooldownMax {
		return fmt.Errorf("%s must be between 5 and %d", SettingKeyOpenAICodexTicketManualRetryCooldownSeconds, openAICodexTicketManualCooldownMax)
	}
	if settings.TTLSeconds < 30 || settings.TTLSeconds > openAICodexTicketTTLSecondsMax {
		return fmt.Errorf("%s must be between 30 and %d", SettingKeyOpenAICodexTicketTTLSeconds, openAICodexTicketTTLSecondsMax)
	}
	if settings.RefreshBeforeSeconds < 5 || settings.RefreshBeforeSeconds >= settings.TTLSeconds {
		return fmt.Errorf("%s must be between 5 and one second less than %s", SettingKeyOpenAICodexTicketRefreshBeforeSeconds, SettingKeyOpenAICodexTicketTTLSeconds)
	}
	return nil
}

func ValidateOpenAICodexTicketRuntimePolicy(settings OpenAICodexTicketRuntimeSettings) error {
	return validateOpenAICodexTicketRuntimePolicy(settings)
}

type cachedOpenAICodexTicketRuntimeSettings struct {
	settings  OpenAICodexTicketRuntimeSettings
	expiresAt int64
}

func (s *SettingService) GetOpenAICodexTicketRuntimeSettings(ctx context.Context, fallback OpenAICodexTicketRuntimeSettings) OpenAICodexTicketRuntimeSettings {
	fallback.RetryCount = normalizeOpenAICodexTicketRetryCount(fallback.RetryCount)
	fallback.RetryIntervalSeconds = normalizeOpenAICodexTicketRetryInterval(fallback.RetryIntervalSeconds)
	fallback.SteadyRetryIntervalSeconds = normalizeOpenAICodexTicketSteadyRetryInterval(fallback.SteadyRetryIntervalSeconds)
	fallback.ManualRetryCooldownSeconds = normalizeOpenAICodexTicketManualCooldown(fallback.ManualRetryCooldownSeconds)
	fallback.TTLSeconds = normalizeOpenAICodexTicketTTL(fallback.TTLSeconds)
	fallback.RefreshBeforeSeconds = normalizeOpenAICodexTicketRefreshBefore(fallback.RefreshBeforeSeconds, fallback.TTLSeconds)
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil || s == nil || s.settingRepo == nil {
		return fallback
	}
	if cached, ok := s.openAICodexTicketRuntimeCache.Load().(*cachedOpenAICodexTicketRuntimeSettings); ok && cached != nil && time.Now().UnixNano() < cached.expiresAt {
		return cached.settings
	}

	resultCh := s.openAICodexTicketRuntimeSF.DoChan("openai_codex_ticket_runtime", func() (any, error) {
		if cached, ok := s.openAICodexTicketRuntimeCache.Load().(*cachedOpenAICodexTicketRuntimeSettings); ok && cached != nil && time.Now().UnixNano() < cached.expiresAt {
			return cached.settings, nil
		}
		dbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		keys := []string{
			SettingKeyOpenAICodexTicketProxyPool,
			SettingKeyOpenAICodexTicketHarvestProxyURL,
			SettingKeyOpenAICodexTicketRetryCount,
			SettingKeyOpenAICodexTicketRetryIntervalSeconds,
			SettingKeyOpenAICodexTicketSteadyRetryIntervalSeconds,
			SettingKeyOpenAICodexTicketManualRetryCooldownSeconds,
			SettingKeyOpenAICodexTicketTTLSeconds,
			SettingKeyOpenAICodexTicketRefreshBeforeSeconds,
		}
		values, err := s.settingRepo.GetMultiple(dbCtx, keys)
		if err != nil {
			if cached, ok := s.openAICodexTicketRuntimeCache.Load().(*cachedOpenAICodexTicketRuntimeSettings); ok && cached != nil {
				return cached.settings, nil
			}
			return fallback, nil
		}
		settings := parseOpenAICodexTicketRuntimePolicyWithFallback(values, fallback)
		fallbackURL := ""
		if len(fallback.ProxyPool) == 1 {
			fallbackURL = fallback.ProxyPool[0].URL
		}
		settings.ProxyPool = resolveOpenAICodexTicketProxyPool(values, fallbackURL)
		s.openAICodexTicketRuntimeCache.Store(&cachedOpenAICodexTicketRuntimeSettings{
			settings:  settings,
			expiresAt: time.Now().Add(openAICodexTicketRuntimeSettingsCacheTTL).UnixNano(),
		})
		return settings, nil
	})
	select {
	case <-ctx.Done():
		return fallback
	case result := <-resultCh:
		if settings, ok := result.Val.(OpenAICodexTicketRuntimeSettings); ok && result.Err == nil {
			return settings
		}
		return fallback
	}
}

func (s *SettingService) InvalidateOpenAICodexTicketRuntimeSettingsCache() {
	if s == nil {
		return
	}
	s.openAICodexTicketRuntimeSF.Forget("openai_codex_ticket_runtime")
	s.openAICodexTicketRuntimeCache.Store(&cachedOpenAICodexTicketRuntimeSettings{})
}
