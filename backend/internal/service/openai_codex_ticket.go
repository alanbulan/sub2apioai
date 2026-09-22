package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

const (
	openAICodexTicketExtraKeyPrefix  = "codex_turn_ticket:"
	openAICodexAstraMinVersion       = "0.153.4"
	openAICodexTicketStatePrefix     = "gAAAAA"
	openAICodexTicketDefaultModel    = "gpt-6-astra"
	openAICodexTicketDefaultSolModel = "gpt-5.6-sol"
	openAICodexTicketFinalRetryLead  = time.Minute
	openAICodexTicketMinExpiryRetry  = 15 * time.Second
	openAICodexTicketMaxExpiryRetry  = time.Minute
	openAICodexTicketManualTimeout   = 15 * time.Second

	// A proxy URL containing this token gets an independent sticky session for
	// every account/model pair. The token is replaced locally and is never sent
	// to the proxy provider.
	openAICodexTicketProxySessionPlaceholder = "__SESSION__"
	openAICodexTicketProxySessionIDLength    = 12
)

var (
	// ErrOpenAICodexTicketUnavailable 表示该号该模型没有可用的 292 门票，
	// 且 fail_closed 禁止裸打业务请求。
	ErrOpenAICodexTicketUnavailable = errors.New("codex turn-state ticket unavailable")

	ErrOpenAICodexTicketRetryDisabled       = errors.New("codex ticket harvesting is disabled")
	ErrOpenAICodexTicketRetryUnsupported    = errors.New("account does not support codex ticket harvesting")
	ErrOpenAICodexTicketRetryInvalidModel   = errors.New("model is not configured for codex ticket harvesting")
	ErrOpenAICodexTicketRetryIneligible     = errors.New("account is not schedulable")
	ErrOpenAICodexTicketRetryQuotaExhausted = errors.New("account quota is exhausted")
	ErrOpenAICodexTicketRetryNoProxy        = errors.New("codex ticket harvest proxy is not configured")
	ErrOpenAICodexTicketRetryAlreadyReady   = errors.New("codex ticket is already ready")
	ErrOpenAICodexTicketRetryInProgress     = errors.New("codex ticket probe is already in progress")
	ErrOpenAICodexTicketRetryCooldown       = errors.New("codex ticket manual retry is cooling down")
)

type openAICodexTicket struct {
	AccountID  int64     `json:"account_id"`
	Model      string    `json:"model"`
	State      string    `json:"state"`
	Length     int       `json:"length"`
	CapturedAt time.Time `json:"captured_at"`
	ExpiresAt  time.Time `json:"expires_at"`
	Attempts   int       `json:"attempts"`
}

type openAICodexTicketProbeBackoff struct {
	Failures         int
	RetryAt          time.Time
	ProxyFingerprint [sha256.Size]byte
	Phase            openAICodexTicketRetryPhase
	LastProbeReason  openAICodexTicketProbeVerdict
	LastHTTPStatus   int
	LastStateLength  int
	LastServedModel  string
}

type openAICodexTicketRetryPhase uint8

const (
	openAICodexTicketRetryPhaseMissing openAICodexTicketRetryPhase = iota + 1
	openAICodexTicketRetryPhaseRenewal
	openAICodexTicketRetryPhaseExpiredRecovery
)

type openAICodexTicketProxySession struct {
	ProxyFingerprint [sha256.Size]byte
	ID               string
	Rotations        int
}

func openAICodexTicketKey(accountID int64, model string) string {
	return fmt.Sprintf("%d\x00%s", accountID, strings.TrimSpace(model))
}

func openAICodexTicketExtraKey(model string) string {
	return openAICodexTicketExtraKeyPrefix + strings.TrimSpace(model)
}

func normalizeOpenAICodexTicketModel(model string) string {
	return strings.TrimSpace(model)
}

func openAICodexTicketProxyFingerprint(proxyURL string) [sha256.Size]byte {
	return sha256.Sum256([]byte(strings.TrimSpace(proxyURL)))
}

func openAICodexTicketProxyUsesSessions(proxyTemplate string) bool {
	return strings.Count(proxyTemplate, openAICodexTicketProxySessionPlaceholder) == 1
}

func newOpenAICodexTicketProxySessionID() string {
	id := strings.ReplaceAll(uuid.NewString(), "-", "")
	return id[:openAICodexTicketProxySessionIDLength]
}

func (s *OpenAIGatewayService) openAICodexTicketProbeProxyURL(key, proxyTemplate string) string {
	proxyTemplate = strings.TrimSpace(proxyTemplate)
	if s == nil || !openAICodexTicketProxyUsesSessions(proxyTemplate) {
		if s != nil {
			s.openaiCodexTicketProxySessions.Delete(key)
		}
		return proxyTemplate
	}

	fingerprint := openAICodexTicketProxyFingerprint(proxyTemplate)
	if raw, ok := s.openaiCodexTicketProxySessions.Load(key); ok {
		if state, valid := raw.(openAICodexTicketProxySession); valid && state.ProxyFingerprint == fingerprint && state.ID != "" {
			return strings.Replace(proxyTemplate, openAICodexTicketProxySessionPlaceholder, state.ID, 1)
		}
	}
	state := openAICodexTicketProxySession{
		ProxyFingerprint: fingerprint,
		ID:               newOpenAICodexTicketProxySessionID(),
	}
	s.openaiCodexTicketProxySessions.Store(key, state)
	return strings.Replace(proxyTemplate, openAICodexTicketProxySessionPlaceholder, state.ID, 1)
}

// rotateOpenAICodexTicketProxySession changes only the failed account/model's
// sticky session. Probe backoff remains keyed by the unchanged template, so a
// rotation cannot bypass rate limiting and create a retry loop.
func (s *OpenAIGatewayService) rotateOpenAICodexTicketProxySession(key, proxyTemplate string) int {
	proxyTemplate = strings.TrimSpace(proxyTemplate)
	if s == nil || !openAICodexTicketProxyUsesSessions(proxyTemplate) {
		return 0
	}
	fingerprint := openAICodexTicketProxyFingerprint(proxyTemplate)
	rotations := 1
	if raw, ok := s.openaiCodexTicketProxySessions.Load(key); ok {
		if previous, valid := raw.(openAICodexTicketProxySession); valid && previous.ProxyFingerprint == fingerprint {
			rotations = previous.Rotations + 1
		}
	}
	s.openaiCodexTicketProxySessions.Store(key, openAICodexTicketProxySession{
		ProxyFingerprint: fingerprint,
		ID:               newOpenAICodexTicketProxySessionID(),
		Rotations:        rotations,
	})
	return rotations
}

func (s *OpenAIGatewayService) markOpenAICodexTicketProxySessionSuccessful(key, proxyTemplate string) {
	if s == nil || !openAICodexTicketProxyUsesSessions(proxyTemplate) {
		return
	}
	fingerprint := openAICodexTicketProxyFingerprint(proxyTemplate)
	if raw, ok := s.openaiCodexTicketProxySessions.Load(key); ok {
		if state, valid := raw.(openAICodexTicketProxySession); valid && state.ProxyFingerprint == fingerprint {
			state.Rotations = 0
			s.openaiCodexTicketProxySessions.Store(key, state)
		}
	}
}

func (s *OpenAIGatewayService) openAICodexTicketProbeBackedOffForSettings(key string, settings OpenAICodexTicketRuntimeSettings, now time.Time) bool {
	if s == nil {
		return false
	}
	raw, ok := s.openaiCodexTicketProbeBackoffs.Load(key)
	if !ok {
		return false
	}
	state, ok := raw.(openAICodexTicketProbeBackoff)
	if !ok || state.ProxyFingerprint != settings.Fingerprint() {
		s.openaiCodexTicketProbeBackoffs.Delete(key)
		return false
	}
	return now.Before(state.RetryAt)
}

func openAICodexTicketRuntimeSettingsForProxyURL(proxyURL string) OpenAICodexTicketRuntimeSettings {
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.ProxyPool = syntheticOpenAICodexTicketProxy(openAICodexTicketLegacyProxyID, "Legacy proxy", proxyURL)
	return settings
}

// Compatibility wrapper retained for focused unit tests and older internal
// callers. Production harvesting passes the full runtime settings instead.
func (s *OpenAIGatewayService) openAICodexTicketProbeBackedOff(key, proxyURL string, now time.Time) bool {
	return s.openAICodexTicketProbeBackedOffForSettings(key, openAICodexTicketRuntimeSettingsForProxyURL(proxyURL), now)
}

func (s *OpenAIGatewayService) recordOpenAICodexTicketProbeFailure(key, proxyURL string, now, ticketExpiresAt time.Time) openAICodexTicketProbeBackoff {
	return s.recordOpenAICodexTicketProbeFailureWithSettings(key, openAICodexTicketRuntimeSettingsForProxyURL(proxyURL), now, ticketExpiresAt)
}

func openAICodexTicketRetryPhaseAt(now, ticketExpiresAt time.Time) openAICodexTicketRetryPhase {
	if ticketExpiresAt.IsZero() {
		return openAICodexTicketRetryPhaseMissing
	}
	if now.Before(ticketExpiresAt) {
		return openAICodexTicketRetryPhaseRenewal
	}
	return openAICodexTicketRetryPhaseExpiredRecovery
}

func (s *OpenAIGatewayService) recordOpenAICodexTicketProbeFailureWithSettings(key string, settings OpenAICodexTicketRuntimeSettings, now, ticketExpiresAt time.Time) openAICodexTicketProbeBackoff {
	fingerprint := settings.Fingerprint()
	phase := openAICodexTicketRetryPhaseAt(now, ticketExpiresAt)
	failures := 1
	if raw, ok := s.openaiCodexTicketProbeBackoffs.Load(key); ok {
		if previous, valid := raw.(openAICodexTicketProbeBackoff); valid && previous.ProxyFingerprint == fingerprint && previous.Phase == phase {
			failures = previous.Failures + 1
		}
	}
	retryCount := normalizeOpenAICodexTicketRetryCount(settings.RetryCount)
	delay := settings.RetryInterval()
	if failures > retryCount {
		delay = settings.SteadyRetryInterval()
	}
	retryAt := openAICodexTicketProbeRetryAt(now, ticketExpiresAt, delay)
	state := openAICodexTicketProbeBackoff{
		Failures:         failures,
		RetryAt:          retryAt,
		ProxyFingerprint: fingerprint,
		Phase:            phase,
	}
	s.openaiCodexTicketProbeBackoffs.Store(key, state)
	return state
}

func (s *OpenAIGatewayService) recordOpenAICodexTicketProbeDiagnostic(
	account *Account,
	model, key string,
	settings OpenAICodexTicketRuntimeSettings,
	reason openAICodexTicketProbeVerdict,
	status, stateLength int,
	servedModel string,
) openAICodexTicketProbeBackoff {
	state := s.recordOpenAICodexTicketProbeMissWithSettings(account, model, key, settings, s.openAICodexTicketConfig().TargetLength)
	state.LastProbeReason = reason
	state.LastHTTPStatus = status
	state.LastStateLength = stateLength
	state.LastServedModel = safeOpenAICodexTicketObservedModel(servedModel)
	s.openaiCodexTicketProbeBackoffs.Store(key, state)
	return state
}

// Keep renewal aggressive only around expiry, then return stale tickets to a
// low-traffic steady-state retry cadence.
func openAICodexTicketProbeRetryAt(now, ticketExpiresAt time.Time, normalDelay time.Duration) time.Time {
	if ticketExpiresAt.IsZero() {
		return now.Add(normalDelay)
	}

	remaining := ticketExpiresAt.Sub(now)
	if remaining > openAICodexTicketFinalRetryLead {
		retryAt := now.Add(normalDelay)
		finalPreExpiryAttempt := ticketExpiresAt.Add(-openAICodexTicketFinalRetryLead)
		if retryAt.After(finalPreExpiryAttempt) {
			return finalPreExpiryAttempt
		}
		return retryAt
	}
	if remaining > 0 {
		delay := remaining / 2
		if delay < openAICodexTicketMinExpiryRetry {
			delay = openAICodexTicketMinExpiryRetry
		}
		if delay > openAICodexTicketMaxExpiryRetry {
			delay = openAICodexTicketMaxExpiryRetry
		}
		return now.Add(delay)
	}
	return now.Add(normalDelay)
}

func (s *OpenAIGatewayService) recordOpenAICodexTicketProbeMiss(account *Account, model, key, proxyURL string, targetLen int, useSessionRetry bool) openAICodexTicketProbeBackoff {
	return s.recordOpenAICodexTicketProbeMissWithSettings(account, model, key, openAICodexTicketRuntimeSettingsForProxyURL(proxyURL), targetLen)
}

func (s *OpenAIGatewayService) recordOpenAICodexTicketProbeMissWithSettings(account *Account, model, key string, settings OpenAICodexTicketRuntimeSettings, targetLen int) openAICodexTicketProbeBackoff {
	now := time.Now()
	ticketExpiresAt := time.Time{}
	if ticket := s.lookupOpenAICodexTicket(account, model); ticket.matches(targetLen) {
		ticketExpiresAt = ticket.effectiveExpiresAt(settings.TTL())
	}
	return s.recordOpenAICodexTicketProbeFailureWithSettings(key, settings, now, ticketExpiresAt)
}

func extractOpenAICodexTicketModel(body []byte) string {
	return normalizeOpenAICodexTicketModel(gjson.GetBytes(body, "model").String())
}

func (s *OpenAIGatewayService) openAICodexTicketConfig() config.OpenAICodexTicketConfig {
	cfg := config.OpenAICodexTicketConfig{}
	if s != nil && s.cfg != nil {
		cfg = s.cfg.Gateway.OpenAICodexTicket
	}
	if cfg.TargetLength <= 0 {
		cfg.TargetLength = 292
	}
	if cfg.TTLSeconds <= 0 {
		cfg.TTLSeconds = OpenAICodexTicketTTLSecondsDefault
	}
	if cfg.RefreshBeforeSeconds <= 0 {
		cfg.RefreshBeforeSeconds = OpenAICodexTicketRefreshBeforeSecondsDefault
	}
	if cfg.HarvestProbeIntervalSeconds <= 0 {
		cfg.HarvestProbeIntervalSeconds = 6
	}
	if cfg.HarvestAttemptTimeoutSeconds <= 0 {
		cfg.HarvestAttemptTimeoutSeconds = 25
	}
	if len(cfg.Models) == 0 {
		cfg.Models = []string{openAICodexTicketDefaultModel, openAICodexTicketDefaultSolModel}
	}
	return cfg
}

func (s *OpenAIGatewayService) openAICodexTicketProxyRotateURL() string {
	raw := strings.TrimSpace(s.openAICodexTicketConfig().HarvestProxyRotateURL)
	if raw == "" {
		return ""
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return ""
	}
	return raw
}

func (s *OpenAIGatewayService) openAICodexTicketCanRotateFixedProxy(proxyTemplate string) bool {
	return s != nil && s.httpUpstream != nil && !openAICodexTicketProxyUsesSessions(proxyTemplate) && s.openAICodexTicketProxyRotateURL() != ""
}

// rotateOpenAICodexTicketFixedProxy asks a fixed-proxy provider to change its
// shared exit. It runs only after all probes in a cycle have completed, so one
// model cannot change the IP underneath another model's in-flight request.
func (s *OpenAIGatewayService) rotateOpenAICodexTicketFixedProxy(ctx context.Context, proxyTemplate string) (int, error) {
	rotateURL := s.openAICodexTicketProxyRotateURL()
	if rotateURL == "" || !s.openAICodexTicketCanRotateFixedProxy(proxyTemplate) {
		return 0, errors.New("fixed proxy rotation is not configured")
	}
	runtimeSettings := s.openAICodexTicketRuntimeSettingsContext(ctx)
	enabled := enabledOpenAICodexTicketProxies(runtimeSettings.ProxyPool)
	if len(enabled) != 1 || openAICodexTicketProxyFingerprint(enabled[0].URL) != openAICodexTicketProxyFingerprint(proxyTemplate) {
		return 0, errors.New("harvest proxy changed before rotation")
	}

	rotateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(rotateCtx, http.MethodGet, rotateURL, nil)
	if err != nil {
		return 0, err
	}
	req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAIHarvest))
	req.Close = true
	resp, err := s.httpUpstream.Do(req, proxyTemplate, 0, 1)
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, errors.New("nil proxy rotation response")
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return resp.StatusCode, fmt.Errorf("proxy rotation returned HTTP %d", resp.StatusCode)
	}
	return resp.StatusCode, nil
}

func (s *OpenAIGatewayService) openAICodexTicketGatedModel(model string) bool {
	model = normalizeOpenAICodexTicketModel(model)
	if model == "" || !s.openAICodexTicketEnabled() {
		return false
	}
	for _, item := range s.openAICodexTicketConfig().Models {
		if normalizeOpenAICodexTicketModel(item) == model {
			return true
		}
	}
	return false
}

// OpenAICodexTicketStatus 是给管理端看的门票摘要，不含 state blob。
type OpenAICodexTicketStatus struct {
	Model                string     `json:"model"`
	Length               int        `json:"length,omitempty"`
	Ready                bool       `json:"ready"`
	RemainingSeconds     int64      `json:"remaining_seconds"`
	Blocked              bool       `json:"blocked"`
	ExpiresAt            *time.Time `json:"expires_at,omitempty"`
	RetryAt              *time.Time `json:"retry_at,omitempty"`
	RetryInSeconds       int64      `json:"retry_in_seconds,omitempty"`
	Probing              bool       `json:"probing"`
	ManualRetryAllowed   bool       `json:"manual_retry_allowed"`
	ManualRetryInSeconds int64      `json:"manual_retry_in_seconds,omitempty"`
	LastProbeReason      string     `json:"last_probe_reason,omitempty"`
	LastProbeHTTPStatus  int        `json:"last_probe_http_status,omitempty"`
	LastProbeStateLength int        `json:"last_probe_state_length,omitempty"`
	LastProbeServedModel string     `json:"last_probe_served_model,omitempty"`
}

func OpenAICodexTicketStatuses(account *Account, cfg config.OpenAICodexTicketConfig, now time.Time) []OpenAICodexTicketStatus {
	if !cfg.Enabled || !isOpenAICodexTicketAccount(account) {
		return nil
	}
	models, targetLen := cfg.Models, cfg.TargetLength
	if len(models) == 0 {
		models = []string{openAICodexTicketDefaultModel, openAICodexTicketDefaultSolModel}
	}
	if targetLen <= 0 {
		targetLen = 292
	}
	ttlSeconds := cfg.TTLSeconds
	if ttlSeconds <= 0 {
		ttlSeconds = OpenAICodexTicketTTLSecondsDefault
	}
	ttl := time.Duration(ttlSeconds) * time.Second
	var routeCookie *openAICodexRouteCookie
	if account != nil && account.Extra != nil {
		routeCookie = parseOpenAICodexRouteCookieFromAny(account.ID, account.Extra[openAICodexRouteCookieExtraKey])
	}
	routeCookieReady := routeCookie.valid(now, ttl)
	routeCookieExpiry := routeCookie.effectiveExpiresAt(ttl)
	out := make([]OpenAICodexTicketStatus, 0, len(models))
	for _, model := range models {
		model = normalizeOpenAICodexTicketModel(model)
		if model == "" {
			continue
		}
		status := OpenAICodexTicketStatus{Model: model}
		ticket := parseOpenAICodexTicketFromAny(0, model, nil)
		if account != nil && account.Extra != nil {
			ticket = parseOpenAICodexTicketFromAny(account.ID, model, account.Extra[openAICodexTicketExtraKey(model)])
		}
		if ticket.valid(now, targetLen, ttl) && routeCookieReady {
			status.Ready = true
			status.Length = ticket.Length
			expiresAt := ticket.effectiveExpiresAt(ttl)
			if routeCookieExpiry.Before(expiresAt) {
				expiresAt = routeCookieExpiry
			}
			remaining := openAICodexTicketSecondsUntil(now, expiresAt)
			status.RemainingSeconds = remaining
			exp := expiresAt
			status.ExpiresAt = &exp
		} else if ticket.valid(now, targetLen, ttl) && !routeCookieReady {
			status.Length = ticket.Length
			status.LastProbeReason = string(openAICodexTicketProbeMissingCookie)
		}
		status.Blocked = cfg.FailClosed && !status.Ready
		out = append(out, status)
	}
	return out
}

// OpenAICodexTicketStatuses adds process-local retry/probe state to the
// persisted ticket summary. No ticket blob or proxy detail is exposed.
func (s *OpenAIGatewayService) OpenAICodexTicketStatuses(account *Account, cfg config.OpenAICodexTicketConfig, now time.Time) []OpenAICodexTicketStatus {
	statuses := OpenAICodexTicketStatuses(account, cfg, now)
	if s == nil || len(statuses) == 0 || account == nil {
		return statuses
	}

	targetLen := cfg.TargetLength
	if targetLen <= 0 {
		targetLen = 292
	}
	runtimeSettings := s.openAICodexTicketRuntimeSettingsContext(context.Background())
	ttl := runtimeSettings.TTL()
	routeCookie := s.lookupOpenAICodexRouteCookie(account)
	routeCookieReady := routeCookie.valid(now, ttl)
	routeCookieExpiry := routeCookie.effectiveExpiresAt(ttl)
	manualEligible := runtimeSettings.EnabledProxyCount() > 0 && s.httpUpstream != nil && openAICodexTicketProbeEligible(account, now)
	for i := range statuses {
		status := &statuses[i]
		status.Ready = false
		status.Blocked = cfg.FailClosed
		status.RemainingSeconds = 0
		status.ExpiresAt = nil
		if ticket := s.lookupOpenAICodexTicket(account, status.Model); ticket.valid(now, targetLen, ttl) && routeCookieReady {
			status.Ready = true
			status.Blocked = false
			status.Length = ticket.Length
			expiresAt := ticket.effectiveExpiresAt(ttl)
			if routeCookieExpiry.Before(expiresAt) {
				expiresAt = routeCookieExpiry
			}
			status.RemainingSeconds = openAICodexTicketSecondsUntil(now, expiresAt)
			status.ExpiresAt = &expiresAt
		} else if ticket != nil && ticket.valid(now, targetLen, ttl) && !routeCookieReady {
			status.Length = ticket.Length
			status.LastProbeReason = string(openAICodexTicketProbeMissingCookie)
		}

		key := openAICodexTicketKey(account.ID, status.Model)
		if raw, ok := s.openaiCodexTicketProbeBackoffs.Load(key); ok {
			if state, valid := raw.(openAICodexTicketProbeBackoff); valid &&
				state.ProxyFingerprint == runtimeSettings.Fingerprint() {
				if !status.Ready {
					status.LastProbeReason = string(state.LastProbeReason)
					status.LastProbeHTTPStatus = state.LastHTTPStatus
					status.LastProbeStateLength = state.LastStateLength
					status.LastProbeServedModel = state.LastServedModel
				}
				if state.RetryAt.After(now) {
					retryAt := state.RetryAt
					status.RetryAt = &retryAt
					status.RetryInSeconds = openAICodexTicketSecondsUntil(now, retryAt)
				}
			}
		}
		_, status.Probing = s.openaiCodexTicketProbing.Load(key)
		status.ManualRetryInSeconds = s.openAICodexTicketManualRetryRemaining(key, now)
		status.ManualRetryAllowed = !status.Ready && manualEligible && !status.Probing && status.ManualRetryInSeconds == 0
	}
	return statuses
}

func openAICodexTicketSecondsUntil(now, at time.Time) int64 {
	remaining := at.Sub(now)
	if remaining <= 0 {
		return 0
	}
	return int64((remaining + time.Second - 1) / time.Second)
}

func (s *OpenAIGatewayService) openAICodexTicketManualRetryRemaining(key string, now time.Time) int64 {
	if s == nil {
		return 0
	}
	raw, ok := s.openaiCodexTicketManualRetries.Load(key)
	if !ok {
		return 0
	}
	retryAt, ok := raw.(time.Time)
	if !ok || !retryAt.After(now) {
		s.openaiCodexTicketManualRetries.Delete(key)
		return 0
	}
	return openAICodexTicketSecondsUntil(now, retryAt)
}

func (s *OpenAIGatewayService) openAICodexTicketEnabled() bool {
	return s.openAICodexTicketEnabledContext(context.Background())
}

func (s *OpenAIGatewayService) openAICodexTicketEnabledContext(ctx context.Context) bool {
	if s == nil {
		return false
	}
	fallback := s.cfg != nil && s.cfg.Gateway.OpenAICodexTicket.Enabled
	if s.settingService != nil {
		return s.settingService.GetOpenAICodexTicketEnabled(ctx, fallback)
	}
	return fallback
}

func (s *OpenAIGatewayService) openAICodexTicketHarvestProxyURL() string {
	return s.openAICodexTicketHarvestProxyURLContext(context.Background())
}

func (s *OpenAIGatewayService) openAICodexTicketHarvestProxyURLContext(ctx context.Context) string {
	if s.settingService != nil {
		if proxy := s.settingService.GetOpenAICodexTicketHarvestProxyURL(ctx); proxy != "" {
			return proxy
		}
	}
	return strings.TrimSpace(s.openAICodexTicketConfig().HarvestProxyURL)
}

func (s *OpenAIGatewayService) openAICodexTicketRuntimeSettingsContext(ctx context.Context) OpenAICodexTicketRuntimeSettings {
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	if s != nil {
		cfg := s.openAICodexTicketConfig()
		settings.TTLSeconds = normalizeOpenAICodexTicketTTL(cfg.TTLSeconds)
		settings.RefreshBeforeSeconds = normalizeOpenAICodexTicketRefreshBefore(cfg.RefreshBeforeSeconds, settings.TTLSeconds)
		settings.ProxyPool = resolveOpenAICodexTicketProxyPool(map[string]string{}, strings.TrimSpace(cfg.HarvestProxyURL))
	}
	if s != nil && s.settingService != nil {
		return s.settingService.GetOpenAICodexTicketRuntimeSettings(ctx, settings)
	}
	return settings
}

func (t *openAICodexTicket) effectiveExpiresAt(ttl time.Duration) time.Time {
	if t == nil {
		return time.Time{}
	}
	return openAICodexEffectiveExpiry(t.CapturedAt, t.ExpiresAt, ttl)
}

func (t *openAICodexTicket) valid(now time.Time, targetLen int, ttl time.Duration) bool {
	if !t.matches(targetLen) {
		return false
	}
	expiresAt := t.effectiveExpiresAt(ttl)
	return !expiresAt.IsZero() && now.Before(expiresAt)
}

func (t *openAICodexTicket) matches(targetLen int) bool {
	if t == nil {
		return false
	}
	state := strings.TrimSpace(t.State)
	return len(state) == targetLen && t.Length == targetLen && strings.HasPrefix(state, openAICodexTicketStatePrefix)
}

func (t *openAICodexTicket) needsRefreshFor(now time.Time, targetLen int, ttl, refreshBefore time.Duration) bool {
	if !t.valid(now, targetLen, ttl) {
		return true
	}
	return !t.effectiveExpiresAt(ttl).After(now.Add(refreshBefore))
}

func (s *OpenAIGatewayService) lookupOpenAICodexTicket(account *Account, model string) *openAICodexTicket {
	if s == nil || account == nil || account.ID <= 0 {
		return nil
	}
	model = normalizeOpenAICodexTicketModel(model)
	if model == "" {
		return nil
	}
	key := openAICodexTicketKey(account.ID, model)
	var mem *openAICodexTicket
	if raw, ok := s.openaiCodexTickets.Load(key); ok {
		mem, _ = raw.(*openAICodexTicket)
	}
	var extra *openAICodexTicket
	if account.Extra != nil {
		extra = parseOpenAICodexTicketFromAny(account.ID, model, account.Extra[openAICodexTicketExtraKey(model)])
	}
	if extra != nil && (mem == nil || extra.CapturedAt.After(mem.CapturedAt)) {
		s.openaiCodexTickets.Store(key, extra)
		return extra
	}
	if mem != nil {
		return mem
	}
	return nil
}

func parseOpenAICodexTicketFromAny(accountID int64, model string, raw any) *openAICodexTicket {
	if raw == nil {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var ticket openAICodexTicket
	if err := json.Unmarshal(b, &ticket); err != nil {
		return nil
	}
	ticket.AccountID = accountID
	if strings.TrimSpace(model) != "" {
		ticket.Model = model
	}
	ticket.State = strings.TrimSpace(ticket.State)
	if ticket.Length == 0 {
		ticket.Length = len(ticket.State)
	}
	if ticket.State == "" {
		return nil
	}
	return &ticket
}

func (s *OpenAIGatewayService) storeOpenAICodexTicket(ctx context.Context, account *Account, ticket *openAICodexTicket, routeCookie *openAICodexRouteCookie) {
	if s == nil || account == nil || ticket == nil || account.ID <= 0 {
		return
	}
	model := normalizeOpenAICodexTicketModel(ticket.Model)
	ticket.Model = model
	ticket.AccountID = account.ID
	s.openaiCodexTickets.Store(openAICodexTicketKey(account.ID, model), ticket)
	updates := map[string]any{openAICodexTicketExtraKey(model): ticket}
	if routeCookie != nil {
		routeCookie.AccountID = account.ID
		s.openaiCodexRouteCookies.Store(account.ID, routeCookie)
		updates[openAICodexRouteCookieExtraKey] = routeCookie
	}
	if s.accountRepo == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.accountRepo.UpdateExtra(ctx, account.ID, updates); err != nil {
		logger.L().Warn("openai_codex_ticket persist failed",
			zap.Int64("account_id", account.ID),
			zap.String("model", model),
			zap.Error(err),
		)
	}
}

// applyOpenAICodexTicket 在出站请求上覆盖 x-codex-turn-state。
// 请求路径只注入已捕获的有效门票，不现场打票；无票则返回
// ErrOpenAICodexTicketUnavailable。打票由后台 harvester 完成。
func (s *OpenAIGatewayService) applyOpenAICodexTicket(ctx context.Context, account *Account, model string, h http.Header) error {
	if s == nil || h == nil || !isOpenAICodexTicketAccount(account) || !s.openAICodexTicketEnabledContext(ctx) {
		return nil
	}
	model = normalizeOpenAICodexTicketModel(model)
	if model == "" || !s.openAICodexTicketGatedModel(model) {
		return nil
	}
	cfg := s.openAICodexTicketConfig()
	runtimeSettings := s.openAICodexTicketRuntimeSettingsContext(ctx)
	ttl := runtimeSettings.TTL()
	now := time.Now()
	ticket := s.lookupOpenAICodexTicket(account, model)
	routeCookie := s.lookupOpenAICodexRouteCookie(account)
	if ticket.valid(now, cfg.TargetLength, ttl) && routeCookie.valid(now, ttl) {
		h.Set(openAICodexTurnStateHeader, ticket.State)
		applyOpenAICodexRouteCookieHeader(h, routeCookie)
		return nil
	}
	if !cfg.FailClosed {
		return nil
	}
	return ErrOpenAICodexTicketUnavailable
}

// openAICodexTicketOutboundModel 预测本请求真正出站的模型名，也就是
// applyOpenAICodexTicket 注入时读到的 body.model。
//
// 调度门控与注入必须按同一个模型名判定门票。普通请求下二者同源：Forward 的
// upstreamModel 与本函数都走 resolveOpenAIAccountUpstreamModelForRequest，且
// Forward 会把 body.model 改写成该值后才注入。但 /responses/compact 例外——
// Forward 会把出站模型进一步改写为 compact 映射或 gateway.openai_compact_model
// （默认非空），此时若门控仍按客户端原始模型判定，就会把「实际出站是非门控
// 模型、根本不需要票」的 compact 请求整片误拦成不可调度。
func (s *OpenAIGatewayService) openAICodexTicketOutboundModel(account *Account, requestedModel string, requireCompact bool) string {
	model := strings.TrimSpace(requestedModel)
	if account == nil || model == "" {
		return model
	}
	if !account.IsOpenAI() {
		return canonicalOpenAIAccountSchedulingModel(account, model)
	}
	_, upstreamModel := resolveOpenAIForwardMappedModels(account, model, requireCompact)
	if requireCompact {
		// 与 Forward 同序：compact 兜底模型优先于普通/compact 映射结果。
		if compactModel := strings.TrimSpace(s.resolveOpenAICompactFallbackModel(account, model)); compactModel != "" {
			upstreamModel = compactModel
		}
	}
	if upstreamModel = strings.TrimSpace(upstreamModel); upstreamModel != "" {
		return upstreamModel
	}
	return model
}

// outboundModel 必须是真正会发给上游的模型名（openAICodexTicketOutboundModel），
// 不是客户端原始模型：注入侧读的是出站 body.model，两侧口径必须一致。
func (s *OpenAIGatewayService) openAICodexTicketBlocksAccount(account *Account, outboundModel string) bool {
	if s == nil || !isOpenAICodexTicketAccount(account) || !s.openAICodexTicketEnabled() {
		return false
	}
	cfg := s.openAICodexTicketConfig()
	if !cfg.FailClosed {
		return false
	}
	model := normalizeOpenAICodexTicketModel(outboundModel)
	if !s.openAICodexTicketGatedModel(model) {
		return false
	}
	ticket := s.lookupOpenAICodexTicket(account, model)
	routeCookie := s.lookupOpenAICodexRouteCookie(account)
	runtimeSettings := s.openAICodexTicketRuntimeSettingsContext(context.Background())
	ttl := runtimeSettings.TTL()
	now := time.Now()
	return !ticket.valid(now, cfg.TargetLength, ttl) || !routeCookie.valid(now, ttl)
}

func (s *OpenAIGatewayService) fireOpenAICodexTicketProbe(ctx context.Context, account *Account, token, model, proxyURL string, attemptTimeout time.Duration) (openAICodexTicketProbeResult, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, attemptTimeout)
	defer cancel()
	runtimeSettings := s.openAICodexTicketRuntimeSettingsContext(attemptCtx)
	existingRouteCookie := s.lookupOpenAICodexRouteCookie(account)
	ttl := runtimeSettings.TTL()
	now := time.Now()
	reuseRouteCookie := existingRouteCookie.valid(now, ttl) &&
		!existingRouteCookie.needsRefresh(now, ttl, runtimeSettings.RefreshBefore())

	body := []byte(`{"model":` + jsonString(model) + `,"store":false,"stream":true,"instructions":"Reply with exactly: pong","input":[{"role":"user","content":[{"type":"input_text","text":"ping"}]}]}`)
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, chatgptCodexURL, bytes.NewReader(body))
	if err != nil {
		return openAICodexTicketProbeResult{Verdict: openAICodexTicketProbeTransportError}, err
	}
	req = req.WithContext(WithHTTPUpstreamProfile(req.Context(), HTTPUpstreamProfileOpenAIHarvest))
	req.Close = true
	req.Host = "chatgpt.com"
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("OpenAI-Beta", "responses=experimental")
	req.Header.Set("session_id", uuid.NewString())
	if err := resolveAndSetOpenAIChatGPTAccountHeaders(attemptCtx, s.accountRepo, req.Header, account); err != nil {
		return openAICodexTicketProbeResult{Verdict: openAICodexTicketProbeTransportError}, err
	}
	applyOpenAICodexTicketHarvestIdentity(req.Header, model)
	if reuseRouteCookie {
		applyOpenAICodexRouteCookieHeader(req.Header, existingRouteCookie)
	}

	// Synthetic probes must use the dedicated no-reuse transport even when the
	// production account is bound to a plugin. This also avoids reading pluginManager
	// while handlers are still wiring it during gateway construction.
	resp, err := s.httpUpstream.Do(req, proxyURL, account.ID, account.Concurrency)
	if err != nil {
		return openAICodexTicketProbeResult{Verdict: openAICodexTicketProbeTransportError}, err
	}
	if resp == nil {
		return openAICodexTicketProbeResult{Verdict: openAICodexTicketProbeTransportError}, errors.New("nil upstream response")
	}
	defer func() {
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()
	result, inspectErr := inspectOpenAICodexTicketProbeResponse(resp, model, s.openAICodexTicketConfig().TargetLength)
	if result.Verdict == openAICodexTicketProbeMissingCookie && reuseRouteCookie && existingRouteCookie.valid(time.Now(), ttl) {
		result.RouteCookies = maps.Clone(existingRouteCookie.Values)
		result.CookieUpdated = false
		result.Verdict = openAICodexTicketProbeVerified
	}
	return result, inspectErr
}

func jsonString(v string) string {
	b, err := json.Marshal(v)
	if err != nil {
		return `""`
	}
	return string(b)
}

func applyOpenAICodexTicketHarvestIdentity(h http.Header, model string) {
	ensureCodexIdentityHeaders(h)
	enforceCodexIdentityHeaders(h)
	version := strings.TrimSpace(h.Get("version"))
	if needsOpenAICodexAstraVersion(model) && (version == "" || CompareVersions(version, openAICodexAstraMinVersion) < 0) {
		h.Set("version", openAICodexAstraMinVersion)
		h.Set("user-agent", buildCodexCLIUserAgent(openAICodexAstraMinVersion))
		h.Set("originator", openai.CodexDefaultOriginator)
	}
}

func needsOpenAICodexAstraVersion(model string) bool {
	m := strings.ToLower(normalizeOpenAICodexTicketModel(model))
	return strings.Contains(m, "gpt-6") || strings.Contains(m, "astra")
}

func (s *OpenAIGatewayService) StartOpenAICodexTicketHarvester() {
	if s == nil {
		return
	}
	s.openaiCodexTicketLifecycleMu.Lock()
	defer s.openaiCodexTicketLifecycleMu.Unlock()
	if s.openaiCodexTicketStopped || s.openaiCodexTicketDone != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.openaiCodexTicketCancel = cancel
	s.openaiCodexTicketDone = done
	go func() {
		defer close(done)
		s.openAICodexTicketHarvestLoop(ctx)
	}()
	cfg := s.openAICodexTicketConfig()
	logger.L().Info("openai_codex_ticket harvester started",
		zap.Int("ttl_seconds", cfg.TTLSeconds),
		zap.Int("refresh_before_seconds", cfg.RefreshBeforeSeconds),
		zap.Int("target_length", cfg.TargetLength),
		zap.Strings("models", cfg.Models),
	)
}

func (s *OpenAIGatewayService) StopOpenAICodexTicketHarvester() {
	if s == nil {
		return
	}
	s.openaiCodexTicketLifecycleMu.Lock()
	s.openaiCodexTicketStopped = true
	cancel, done := s.openaiCodexTicketCancel, s.openaiCodexTicketDone
	s.openaiCodexTicketLifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (s *OpenAIGatewayService) openAICodexTicketHarvestLoop(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.refreshOpenAICodexTickets(ctx)
			timer.Reset(time.Duration(s.openAICodexTicketConfig().HarvestProbeIntervalSeconds) * time.Second)
		}
	}
}

// refreshOpenAICodexTickets probes each account/model with a missing or soon-to-expire
// ticket once. The loop waits for all probes, then waits the configured interval
// before starting the next cycle.
func (s *OpenAIGatewayService) refreshOpenAICodexTickets(ctx context.Context) {
	if s == nil || s.accountRepo == nil || ctx.Err() != nil {
		return
	}
	if !s.openAICodexTicketEnabledContext(ctx) {
		s.openaiCodexTicketProbeBackoffs.Clear()
		s.openaiCodexTicketProxySessions.Clear()
		s.openaiCodexTicketManualRetries.Clear()
		s.openaiCodexTicketProxyPool.clear()
		return
	}
	runtimeSettings := s.openAICodexTicketRuntimeSettingsContext(ctx)
	if runtimeSettings.EnabledProxyCount() == 0 {
		s.openaiCodexTicketProbeBackoffs.Clear()
		s.openaiCodexTicketProxySessions.Clear()
		s.openaiCodexTicketManualRetries.Clear()
		s.openaiCodexTicketProxyPool.clear()
		return
	}
	accounts, err := s.accountRepo.ListByPlatform(ctx, PlatformOpenAI)
	if err != nil {
		logger.L().Warn("openai_codex_ticket list accounts failed", zap.Error(err))
		return
	}
	cfg := s.openAICodexTicketConfig()
	now := time.Now()
	ttl := runtimeSettings.TTL()
	refreshBefore := runtimeSettings.RefreshBefore()
	var wg sync.WaitGroup
	var rotateFixedProxy atomic.Bool
	probed := 0
	for i := range accounts {
		account := accounts[i]
		if !openAICodexTicketProbeEligible(&account, now) {
			for _, model := range cfg.Models {
				key := openAICodexTicketKey(account.ID, normalizeOpenAICodexTicketModel(model))
				s.openaiCodexTicketProbeBackoffs.Delete(key)
				s.openaiCodexTicketManualRetries.Delete(key)
				s.clearOpenAICodexTicketProxyKey(key)
			}
			continue
		}
		routeCookie := s.lookupOpenAICodexRouteCookie(&account)
		routeCookieFresh := !routeCookie.needsRefresh(now, ttl, refreshBefore)
		routeCookieRefreshScheduled := false
		for _, model := range cfg.Models {
			model := normalizeOpenAICodexTicketModel(model)
			if model == "" {
				continue
			}
			ticket := s.lookupOpenAICodexTicket(&account, model)
			ticketFresh := !ticket.needsRefreshFor(now, cfg.TargetLength, ttl, refreshBefore)
			if ticketFresh && (routeCookieFresh || routeCookieRefreshScheduled) {
				continue
			}
			key := openAICodexTicketKey(account.ID, model)
			if s.openAICodexTicketProbeBackedOffForSettings(key, runtimeSettings, now) {
				continue
			}
			if !routeCookieFresh {
				routeCookieRefreshScheduled = true
			}
			acc := account
			// Token/header helpers may update account metadata; each model owns its maps.
			acc.Extra = maps.Clone(account.Extra)
			acc.Credentials = maps.Clone(account.Credentials)
			probed++
			wg.Add(1)
			go func(acc Account, model string) {
				defer wg.Done()
				if s.probeOnceOpenAICodexTicketWithSettings(ctx, &acc, model, runtimeSettings) {
					rotateFixedProxy.Store(true)
				}
			}(acc, model)
		}
	}
	wg.Wait()
	if rotateFixedProxy.Load() {
		enabled := enabledOpenAICodexTicketProxies(runtimeSettings.ProxyPool)
		proxyURL := ""
		if len(enabled) == 1 {
			proxyURL = enabled[0].URL
		}
		status, rotateErr := s.rotateOpenAICodexTicketFixedProxy(ctx, proxyURL)
		if rotateErr != nil {
			logger.L().Warn("openai_codex_ticket proxy rotation failed",
				zap.Int("http", status), zap.Error(rotateErr))
		} else {
			logger.L().Info("openai_codex_ticket proxy rotated", zap.Int("http", status))
		}
	}
	if probed > 0 {
		logger.L().Info("openai_codex_ticket probe cycle", zap.Int("probed", probed))
	}
}

// probeOnceOpenAICodexTicket 走打票代理打一发。命中合格 292（HTTP 200、长度==target、
// gAAAAA 前缀）就落库；否则记 Info miss，交给下个周期重试。同一 key 并发去重，避免上一发还没
// 回来又叠一发。
func (s *OpenAIGatewayService) probeOnceOpenAICodexTicket(ctx context.Context, account *Account, model string) bool {
	return s.probeOnceOpenAICodexTicketWithSettings(ctx, account, model, s.openAICodexTicketRuntimeSettingsContext(ctx))
}

func (s *OpenAIGatewayService) probeOnceOpenAICodexTicketWithSettings(ctx context.Context, account *Account, model string, runtimeSettings OpenAICodexTicketRuntimeSettings) bool {
	if s == nil || !openAICodexTicketProbeEligible(account, time.Now()) || ctx.Err() != nil || !s.openAICodexTicketEnabledContext(ctx) {
		return false
	}
	cfg := s.openAICodexTicketConfig()
	if runtimeSettings.EnabledProxyCount() == 0 || s.httpUpstream == nil || ctx.Err() != nil {
		return false
	}
	key := openAICodexTicketKey(account.ID, model)
	s.openaiCodexTicketProbing.Store(key, true)
	defer s.openaiCodexTicketProbing.Delete(key)
	result, _, _ := s.openaiCodexTicketFlight.Do(key, func() (any, error) {
		token, _, err := s.GetAccessToken(ctx, account)
		if err != nil || strings.TrimSpace(token) == "" {
			backoff := s.recordOpenAICodexTicketProbeDiagnostic(
				account, model, key, runtimeSettings,
				openAICodexTicketProbeTokenError, 0, 0, "",
			)
			logger.L().Info("openai_codex_ticket probe miss",
				zap.Int64("account_id", account.ID), zap.String("model", model),
				zap.String("reason", string(openAICodexTicketProbeTokenError)), zap.Error(err),
				zap.Int("failures", backoff.Failures), zap.Int64("retry_in_seconds", int64(time.Until(backoff.RetryAt)/time.Second)))
			return false, nil
		}
		selection, ok := s.openaiCodexTicketProxyPool.selectProxy(key, runtimeSettings, time.Now())
		if !ok {
			return false, nil
		}
		defer selection.release()
		proxyTemplate := selection.proxy.URL
		proxySessionKey := openAICodexTicketProxySessionKey(key, selection.proxy.ID)
		proxyURL := s.openAICodexTicketProbeProxyURL(proxySessionKey, proxyTemplate)
		probeResult, perr := s.fireOpenAICodexTicketProbe(ctx, account, token, model, proxyURL, time.Duration(cfg.HarvestAttemptTimeoutSeconds)*time.Second)
		if perr != nil {
			s.openaiCodexTicketProxyPool.penalize(selection, time.Now(), openAICodexTicketProxyPenalty(runtimeSettings))
			rotations := s.rotateOpenAICodexTicketProxySession(proxySessionKey, proxyTemplate)
			rotateFixedProxy := selection.singleNode && rotations == 0 && s.openAICodexTicketCanRotateFixedProxy(proxyTemplate)
			backoff := s.recordOpenAICodexTicketProbeDiagnostic(
				account, model, key, runtimeSettings,
				openAICodexTicketProbeTransportError, probeResult.Status, len(probeResult.State), probeResult.ServedModel,
			)
			logger.L().Info("openai_codex_ticket probe miss",
				zap.Int64("account_id", account.ID), zap.String("model", model),
				zap.String("proxy_id", selection.proxy.ID),
				zap.String("reason", string(openAICodexTicketProbeTransportError)), zap.Error(perr),
				zap.Bool("proxy_session_rotated", rotations > 0), zap.Int("proxy_session_rotations", rotations),
				zap.Bool("proxy_rotation_requested", rotateFixedProxy),
				zap.Int("failures", backoff.Failures), zap.Int64("retry_in_seconds", int64(time.Until(backoff.RetryAt)/time.Second)))
			return rotateFixedProxy, nil
		}
		if !probeResult.verified() {
			rotations := 0
			rotateFixedProxy := false
			// A 200 response with the wrong turn-state is the observed signal for
			// an unsuitable egress IP. A 403 is also exit-specific for providers
			// with an explicit rotate endpoint. HTTP 400 remains an account/request
			// failure and must not consume a new proxy exit.
			if probeResult.Status == http.StatusOK || probeResult.Status == http.StatusForbidden {
				rotations = s.rotateOpenAICodexTicketProxySession(proxySessionKey, proxyTemplate)
				rotateFixedProxy = selection.singleNode && rotations == 0 && s.openAICodexTicketCanRotateFixedProxy(proxyTemplate)
			}
			if probeResult.Status == http.StatusForbidden {
				s.openaiCodexTicketProxyPool.penalize(selection, time.Now(), openAICodexTicketProxyPenalty(runtimeSettings))
			}
			backoff := s.recordOpenAICodexTicketProbeDiagnostic(
				account, model, key, runtimeSettings,
				probeResult.Verdict, probeResult.Status, len(probeResult.State), probeResult.ServedModel,
			)
			logger.L().Info("openai_codex_ticket probe miss",
				zap.Int64("account_id", account.ID), zap.String("model", model),
				zap.String("proxy_id", selection.proxy.ID),
				zap.String("reason", string(probeResult.Verdict)),
				zap.Int("http", probeResult.Status), zap.Int("len", len(probeResult.State)),
				zap.String("served_model", probeResult.ServedModel),
				zap.Bool("proxy_session_rotated", rotations > 0), zap.Int("proxy_session_rotations", rotations),
				zap.Bool("proxy_rotation_requested", rotateFixedProxy),
				zap.Int("failures", backoff.Failures), zap.Int64("retry_in_seconds", int64(time.Until(backoff.RetryAt)/time.Second)))
			return rotateFixedProxy, nil
		}
		s.openaiCodexTicketProbeBackoffs.Delete(key)
		s.openaiCodexTicketProxyPool.clearAttempts(key)
		s.markOpenAICodexTicketProxySessionSuccessful(proxySessionKey, proxyTemplate)
		now := time.Now()
		ttl := runtimeSettings.TTL()
		routeCookie := s.lookupOpenAICodexRouteCookie(account)
		var routeCookieUpdate *openAICodexRouteCookie
		if probeResult.CookieUpdated {
			routeCookie = newOpenAICodexRouteCookie(account.ID, probeResult.RouteCookies, now, ttl)
			routeCookieUpdate = routeCookie
		}
		if !routeCookie.valid(now, ttl) {
			backoff := s.recordOpenAICodexTicketProbeDiagnostic(
				account, model, key, runtimeSettings,
				openAICodexTicketProbeMissingCookie, probeResult.Status, len(probeResult.State), probeResult.ServedModel,
			)
			logger.L().Info("openai_codex_ticket probe miss",
				zap.Int64("account_id", account.ID), zap.String("model", model),
				zap.String("proxy_id", selection.proxy.ID),
				zap.String("reason", string(openAICodexTicketProbeMissingCookie)),
				zap.Int("http", probeResult.Status), zap.Int("len", len(probeResult.State)),
				zap.Int("failures", backoff.Failures), zap.Int64("retry_in_seconds", int64(time.Until(backoff.RetryAt)/time.Second)))
			return false, nil
		}
		ticket := &openAICodexTicket{
			AccountID:  account.ID,
			Model:      model,
			State:      probeResult.State,
			Length:     len(probeResult.State),
			CapturedAt: now,
			ExpiresAt:  now.Add(ttl),
			Attempts:   1,
		}
		s.storeOpenAICodexTicket(ctx, account, ticket, routeCookieUpdate)
		logger.L().Info("openai_codex_ticket harvested",
			zap.Int64("account_id", account.ID), zap.String("model", model),
			zap.String("proxy_id", selection.proxy.ID),
			zap.Int("length", ticket.Length), zap.Int("cookie_count", len(routeCookie.Values)),
			zap.Bool("cookie_updated", probeResult.CookieUpdated), zap.String("mode", "continuous"))
		return false, nil
	})
	rotateFixedProxy, _ := result.(bool)
	return rotateFixedProxy
}

func openAICodexTicketProxyPenalty(settings OpenAICodexTicketRuntimeSettings) time.Duration {
	duration := settings.RetryInterval()
	if duration < 30*time.Second {
		return 30 * time.Second
	}
	if duration > 5*time.Minute {
		return 5 * time.Minute
	}
	return duration
}

func (s *OpenAIGatewayService) clearOpenAICodexTicketProxyKey(key string) {
	s.openaiCodexTicketProxyPool.clearAttempts(key)
	prefix := key + "\x00proxy\x00"
	s.openaiCodexTicketProxySessions.Range(func(rawKey, _ any) bool {
		storedKey, ok := rawKey.(string)
		if ok && strings.HasPrefix(storedKey, prefix) {
			s.openaiCodexTicketProxySessions.Delete(storedKey)
		}
		return true
	})
}

// RequestOpenAICodexTicketRetry schedules one immediate probe for a single
// account/model. It bypasses the automatic backoff once, while a separate
// cooldown and the existing singleflight guard prevent click-driven loops.
func (s *OpenAIGatewayService) RequestOpenAICodexTicketRetry(ctx context.Context, account *Account, model string) (time.Time, error) {
	if s == nil || !s.openAICodexTicketEnabledContext(ctx) {
		return time.Time{}, ErrOpenAICodexTicketRetryDisabled
	}
	if !isOpenAICodexTicketAccount(account) {
		return time.Time{}, ErrOpenAICodexTicketRetryUnsupported
	}
	model = normalizeOpenAICodexTicketModel(model)
	if !s.openAICodexTicketGatedModel(model) {
		return time.Time{}, ErrOpenAICodexTicketRetryInvalidModel
	}
	now := time.Now()
	if !account.IsSchedulable() {
		return time.Time{}, ErrOpenAICodexTicketRetryIneligible
	}
	if openAICodexTicketQuotaExhausted(account, now) {
		return time.Time{}, ErrOpenAICodexTicketRetryQuotaExhausted
	}
	cfg := s.openAICodexTicketConfig()
	runtimeSettings := s.openAICodexTicketRuntimeSettingsContext(ctx)
	ttl := runtimeSettings.TTL()
	if runtimeSettings.EnabledProxyCount() == 0 || s.httpUpstream == nil {
		return time.Time{}, ErrOpenAICodexTicketRetryNoProxy
	}
	if ticket := s.lookupOpenAICodexTicket(account, model); ticket.valid(now, cfg.TargetLength, ttl) &&
		s.lookupOpenAICodexRouteCookie(account).valid(now, ttl) {
		return time.Time{}, ErrOpenAICodexTicketRetryAlreadyReady
	}

	key := openAICodexTicketKey(account.ID, model)
	if _, probing := s.openaiCodexTicketProbing.Load(key); probing {
		return time.Time{}, ErrOpenAICodexTicketRetryInProgress
	}
	retryAt, reserved := s.reserveOpenAICodexTicketManualRetry(key, now, runtimeSettings.ManualRetryCooldown())
	if !reserved {
		return retryAt, ErrOpenAICodexTicketRetryCooldown
	}

	// The manual action owns only this account/model. Clearing its automatic
	// backoff does not release or alter any other key.
	s.openaiCodexTicketProbeBackoffs.Delete(key)
	probeAccount := *account
	probeAccount.Extra = maps.Clone(account.Extra)
	probeAccount.Credentials = maps.Clone(account.Credentials)
	s.openaiCodexTicketProbing.Store(key, true)

	go func() {
		defer s.openaiCodexTicketProbing.Delete(key)
		probeTimeout := time.Duration(cfg.HarvestAttemptTimeoutSeconds)*time.Second + openAICodexTicketManualTimeout
		probeCtx, cancel := context.WithTimeout(context.Background(), probeTimeout)
		defer cancel()
		rotateFixedProxy := s.probeOnceOpenAICodexTicketWithSettings(probeCtx, &probeAccount, model, runtimeSettings)
		if rotateFixedProxy {
			enabled := enabledOpenAICodexTicketProxies(runtimeSettings.ProxyPool)
			proxyURL := ""
			if len(enabled) == 1 {
				proxyURL = enabled[0].URL
			}
			status, err := s.rotateOpenAICodexTicketFixedProxy(probeCtx, proxyURL)
			if err != nil {
				logger.L().Warn("openai_codex_ticket manual proxy rotation failed",
					zap.Int64("account_id", account.ID), zap.String("model", model),
					zap.Int("http", status), zap.Error(err))
			}
		}
		ready := false
		now := time.Now()
		if ticket := s.lookupOpenAICodexTicket(&probeAccount, model); ticket.valid(now, cfg.TargetLength, ttl) &&
			s.lookupOpenAICodexRouteCookie(&probeAccount).valid(now, ttl) {
			ready = true
		}
		logger.L().Info("openai_codex_ticket manual retry completed",
			zap.Int64("account_id", account.ID), zap.String("model", model), zap.Bool("ready", ready))
	}()

	logger.L().Info("openai_codex_ticket manual retry requested",
		zap.Int64("account_id", account.ID), zap.String("model", model),
		zap.Int64("cooldown_seconds", int64(runtimeSettings.ManualRetryCooldown()/time.Second)))
	return retryAt, nil
}

func (s *OpenAIGatewayService) reserveOpenAICodexTicketManualRetry(key string, now time.Time, cooldown time.Duration) (time.Time, bool) {
	s.openaiCodexTicketManualRetryMu.Lock()
	defer s.openaiCodexTicketManualRetryMu.Unlock()
	if raw, ok := s.openaiCodexTicketManualRetries.Load(key); ok {
		if retryAt, valid := raw.(time.Time); valid && retryAt.After(now) {
			return retryAt, false
		}
	}
	retryAt := now.Add(cooldown)
	s.openaiCodexTicketManualRetries.Store(key, retryAt)
	return retryAt, true
}

func openAICodexTicketProbeEligible(account *Account, now time.Time) bool {
	if !isOpenAICodexTicketAccount(account) || !account.IsSchedulable() {
		return false
	}
	return !openAICodexTicketQuotaExhausted(account, now)
}

// A full Codex window cannot produce a useful probe before it resets. Prefer an
// absolute reset time when present; without one, a fresh snapshot is trusted only
// until the normal auto-pause staleness bound so malformed legacy data cannot
// suppress harvesting forever.
func openAICodexTicketQuotaExhausted(account *Account, now time.Time) bool {
	if account == nil || len(account.Extra) == 0 {
		return false
	}
	for _, window := range []string{"5h", "7d"} {
		used, ok := resolveAccountExtraNumber(account.Extra, "codex_"+window+"_used_percent")
		if !ok || used < 100 {
			continue
		}
		if resetAt, ok := openAICodexWindowResetAt(account.Extra, window); ok {
			if now.Before(resetAt) {
				return true
			}
			continue
		}
		if !openAICodexSnapshotStaleForPause(account.Extra, now) {
			return true
		}
	}
	return false
}

// IsOpenAICodexTicketExtraKey identifies server-managed ticket material.
func IsOpenAICodexTicketExtraKey(key string) bool {
	return strings.HasPrefix(key, openAICodexTicketExtraKeyPrefix)
}

// MergeOpenAICodexTicketExtra preserves only persisted tickets, never summaries or
// blobs supplied by an account edit. The repository repeats this under the row
// lock so a concurrent harvest cannot be overwritten by a stale admin snapshot.
func MergeOpenAICodexTicketExtra(extra, current map[string]any) map[string]any {
	result := maps.Clone(extra)
	for key := range result {
		if IsOpenAICodexTicketExtraKey(key) {
			delete(result, key)
		}
	}
	for key, value := range current {
		if IsOpenAICodexTicketExtraKey(key) {
			if result == nil {
				result = make(map[string]any)
			}
			result[key] = value
		}
	}
	return result
}

// ValidateOpenAICodexTicketHarvestProxyURL validates only syntax, without making
// a network request or including credentials in validation errors.
func ValidateOpenAICodexTicketHarvestProxyURL(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if strings.Count(raw, openAICodexTicketProxySessionPlaceholder) > 1 {
		return errors.New("harvest proxy may contain at most one __SESSION__ placeholder")
	}
	validatedURL := strings.Replace(raw, openAICodexTicketProxySessionPlaceholder, strings.Repeat("a", openAICodexTicketProxySessionIDLength), 1)
	parsed, err := url.Parse(validatedURL)
	if err != nil || parsed.Hostname() == "" || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return errors.New("harvest proxy must be an HTTP(S) or SOCKS5(h) URL with a host and no path, query or fragment")
	}
	switch parsed.Scheme {
	case "http", "https", "socks5", "socks5h":
	default:
		return errors.New("harvest proxy scheme must be http, https, socks5 or socks5h")
	}
	if port := parsed.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("harvest proxy port must be between 1 and 65535")
		}
	}
	return nil
}

// MaskProxyURL never returns a stored proxy password, even for invalid legacy data.
func MaskProxyURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" || ValidateOpenAICodexTicketHarvestProxyURL(raw) != nil {
		return ""
	}
	parsed, _ := url.Parse(raw)
	if parsed.User != nil {
		if _, ok := parsed.User.Password(); ok {
			parsed.User = url.UserPassword(parsed.User.Username(), "***")
		}
	}
	return parsed.String()
}

// IsMaskedProxyURL recognizes the exact password placeholder emitted by the API.
func IsMaskedProxyURL(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return true
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.User == nil {
		return false
	}
	password, ok := parsed.User.Password()
	return ok && password == "***"
}

// Credential shadows do not own tickets. Keep their existing forwarding policy
// instead of imposing a gate for a key the harvester never populates.
func isOpenAICodexTicketAccount(account *Account) bool {
	return account != nil && account.IsOpenAIOAuthLike() && !account.IsShadow()
}

// IsOpenAICodexTicketPrivateExtraKey also covers the retired account-level proxy
// override, whose credentials may remain in older account records.
func IsOpenAICodexTicketPrivateExtraKey(key string) bool {
	return IsOpenAICodexTicketExtraKey(key) || key == "codex_harvest_proxy_url"
}

// RedactOpenAICodexTicketExtra strips ephemeral ticket material from exports
// without changing the source account or unrelated backup fields.
func RedactOpenAICodexTicketExtra(extra map[string]any) map[string]any {
	redacted := maps.Clone(extra)
	for key := range redacted {
		if IsOpenAICodexTicketPrivateExtraKey(key) {
			delete(redacted, key)
		}
	}
	return redacted
}
