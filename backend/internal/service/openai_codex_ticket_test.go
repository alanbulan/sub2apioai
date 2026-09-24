package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func fakeCodexTicketState(n int) string {
	if n < len(openAICodexTicketStatePrefix) {
		return strings.Repeat("A", n)
	}
	return openAICodexTicketStatePrefix + strings.Repeat("B", n-len(openAICodexTicketStatePrefix))
}

func ticketTestAccount(id int64) *Account {
	now := time.Now()
	return &Account{
		ID:          id,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Credentials: map[string]any{"access_token": "tok", "chatgpt_account_id": "acc-1"},
		Status:      StatusActive,
		Schedulable: true,
		Extra: map[string]any{
			openAICodexRouteCookieExtraKey: &openAICodexRouteCookie{
				AccountID:  id,
				Values:     map[string]string{"__cflb": "route-test", "__oailb": "origin-test"},
				CapturedAt: now,
				ExpiresAt:  now.Add(time.Hour),
			},
		},
	}
}

func addCodexTicketTestRouteCookies(h http.Header) {
	h.Add("Set-Cookie", "__cflb=route-test; Path=/; HttpOnly; Secure")
	h.Add("Set-Cookie", "__oailb=origin-test; Path=/; HttpOnly; Secure")
}

func ticketTestService(t *testing.T, cfg config.OpenAICodexTicketConfig, upstream HTTPUpstream) *OpenAIGatewayService {
	t.Helper()
	return &OpenAIGatewayService{
		cfg: &config.Config{
			Gateway: config.GatewayConfig{OpenAICodexTicket: cfg},
		},
		httpUpstream: upstream,
	}
}

type codexTicketProxySequenceUpstream struct {
	HTTPUpstream
	proxies  []string
	lengths  []int
	statuses []int
}

func codexTicketProbeModelFromRequest(req *http.Request) string {
	if req == nil || req.Body == nil {
		return ""
	}
	body, _ := io.ReadAll(req.Body)
	return extractOpenAICodexTicketModel(body)
}

func (u *codexTicketProxySequenceUpstream) Do(req *http.Request, proxyURL string, _ int64, _ int) (*http.Response, error) {
	u.proxies = append(u.proxies, proxyURL)
	index := len(u.proxies) - 1
	length := u.lengths[index]
	status := http.StatusOK
	if len(u.statuses) > index {
		status = u.statuses[index]
	}
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(length))
	if length == 292 && status == http.StatusOK {
		addCodexTicketTestRouteCookies(h)
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(codexTicketProbeSuccessSSE(codexTicketProbeModelFromRequest(req))))}, nil
}

type codexTicketRotatableProxyUpstream struct {
	HTTPUpstream
	mu                       sync.Mutex
	probeStatus              int
	probeLength              int
	probeCalls               int
	rotateCalls              int
	expectedProbesAtRotation int
	rotatedAfterAllProbes    bool
	proxyURLs                []string
}

func (u *codexTicketRotatableProxyUpstream) Do(req *http.Request, proxyURL string, _ int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.proxyURLs = append(u.proxyURLs, proxyURL)
	if req.URL.Host == "rotate.example" {
		u.rotateCalls++
		u.rotatedAfterAllProbes = u.probeCalls == u.expectedProbesAtRotation
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	u.probeCalls++
	status := u.probeStatus
	if status == 0 {
		status = http.StatusOK
	}
	h := http.Header{}
	if u.probeLength > 0 {
		h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(u.probeLength))
	}
	if u.probeLength == 292 && status == http.StatusOK {
		addCodexTicketTestRouteCookies(h)
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(codexTicketProbeSuccessSSE(codexTicketProbeModelFromRequest(req))))}, nil
}

func TestApplyOpenAICodexTicket_ReplacesHeader(t *testing.T) {
	state := fakeCodexTicketState(292)
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:      true,
		TargetLength: 292,
		TTLSeconds:   3600,
		FailClosed:   true,
	}, nil)
	account := ticketTestAccount(41)
	svc.storeOpenAICodexTicket(context.Background(), account, &openAICodexTicket{
		AccountID:  41,
		Model:      "gpt-6-astra",
		State:      state,
		Length:     292,
		CapturedAt: time.Now(),
		ExpiresAt:  time.Now().Add(time.Hour),
	}, nil)

	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(312))
	err := svc.applyOpenAICodexTicket(context.Background(), account, "gpt-6-astra", h)
	require.NoError(t, err)
	require.Equal(t, state, h.Get(openAICodexTurnStateHeader))
	require.Equal(t, 292, len(h.Get(openAICodexTurnStateHeader)))
	require.Contains(t, h.Get("Cookie"), "__cflb=route-test")
	require.Contains(t, h.Get("Cookie"), "__oailb=origin-test")
}

func TestApplyOpenAICodexTicket_DoesNotReuseOtherModelOrAccount(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:         true,
		TargetLength:    292,
		TTLSeconds:      3600,
		FailClosed:      true,
		HarvestProxyURL: "socks5h://harvest",
	}, &httpUpstreamRecorder{err: io.EOF})
	a := ticketTestAccount(41)
	b := ticketTestAccount(42)
	astra := fakeCodexTicketState(292)
	svc.storeOpenAICodexTicket(context.Background(), a, &openAICodexTicket{
		AccountID:  41,
		Model:      "gpt-6-astra",
		State:      astra,
		Length:     292,
		CapturedAt: time.Now(),
		ExpiresAt:  time.Now().Add(time.Hour),
	}, nil)

	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, "keep-ungated")
	err := svc.applyOpenAICodexTicket(context.Background(), a, "gpt-5.5", h)
	require.NoError(t, err)
	require.Equal(t, "keep-ungated", h.Get(openAICodexTurnStateHeader))
	require.False(t, svc.openAICodexTicketBlocksAccount(a, "gpt-5.5"))
	require.True(t, svc.openAICodexTicketBlocksAccount(b, "gpt-6-astra"))
	require.False(t, svc.openAICodexTicketBlocksAccount(a, "gpt-6-astra"))

	h = http.Header{}
	err = svc.applyOpenAICodexTicket(context.Background(), b, "gpt-6-astra", h)
	require.ErrorIs(t, err, ErrOpenAICodexTicketUnavailable)
	require.Empty(t, h.Get(openAICodexTurnStateHeader))
}

func TestLookupOpenAICodexTicket_PrefersNewerExtra(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, TargetLength: 292, TTLSeconds: 3600}, nil)
	account := ticketTestAccount(41)
	oldState := fakeCodexTicketState(292)
	newState := openAICodexTicketStatePrefix + strings.Repeat("C", 286)
	svc.storeOpenAICodexTicket(context.Background(), account, &openAICodexTicket{
		AccountID:  41,
		Model:      "gpt-6-astra",
		State:      oldState,
		Length:     292,
		CapturedAt: time.Now().Add(-30 * time.Minute),
		ExpiresAt:  time.Now().Add(-time.Minute),
	}, nil)
	account.Extra = map[string]any{openAICodexTicketExtraKey("gpt-6-astra"): &openAICodexTicket{
		Model:      "gpt-6-astra",
		State:      newState,
		Length:     292,
		CapturedAt: time.Now(),
		ExpiresAt:  time.Now().Add(time.Hour),
	},
	}
	got := svc.lookupOpenAICodexTicket(account, "gpt-6-astra")
	require.NotNil(t, got)
	require.Equal(t, newState, got.State)
	require.True(t, got.valid(time.Now(), 292, time.Hour))
}

func TestApplyOpenAICodexTicket_ExpiredNotInjected(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:         true,
		TargetLength:    292,
		TTLSeconds:      3600,
		FailClosed:      true,
		HarvestProxyURL: "socks5h://harvest",
	}, &httpUpstreamRecorder{err: io.EOF})
	account := ticketTestAccount(41)
	svc.storeOpenAICodexTicket(context.Background(), account, &openAICodexTicket{
		AccountID:  41,
		Model:      "gpt-6-astra",
		State:      fakeCodexTicketState(292),
		Length:     292,
		CapturedAt: time.Now().Add(-2 * time.Hour),
		ExpiresAt:  time.Now().Add(-time.Minute),
	}, nil)
	h := http.Header{}
	err := svc.applyOpenAICodexTicket(context.Background(), account, "gpt-6-astra", h)
	require.ErrorIs(t, err, ErrOpenAICodexTicketUnavailable)
	require.Empty(t, h.Get(openAICodexTurnStateHeader))
}

func TestApplyOpenAICodexTicket_WrongLengthNotInjected(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:      true,
		TargetLength: 292,
		TTLSeconds:   3600,
		FailClosed:   true,
	}, nil)
	account := ticketTestAccount(41)
	svc.storeOpenAICodexTicket(context.Background(), account, &openAICodexTicket{
		AccountID:  41,
		Model:      "gpt-6-astra",
		State:      fakeCodexTicketState(312),
		Length:     312,
		CapturedAt: time.Now(),
		ExpiresAt:  time.Now().Add(time.Hour),
	}, nil)
	h := http.Header{}
	err := svc.applyOpenAICodexTicket(context.Background(), account, "gpt-6-astra", h)
	require.ErrorIs(t, err, ErrOpenAICodexTicketUnavailable)
	require.Empty(t, h.Get(openAICodexTurnStateHeader))
}

func TestApplyOpenAICodexTicket_FailOpenSkipsInject(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:    true,
		FailClosed: false,
	}, nil)
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, "client-state")
	err := svc.applyOpenAICodexTicket(context.Background(), ticketTestAccount(41), "gpt-6-astra", h)
	require.NoError(t, err)
	require.Equal(t, "client-state", h.Get(openAICodexTurnStateHeader))
	require.False(t, svc.openAICodexTicketBlocksAccount(ticketTestAccount(41), "gpt-6-astra"))
}

func TestApplyOpenAICodexTicket_DisabledNoop(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: false, FailClosed: true}, nil)
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, "client-state")
	err := svc.applyOpenAICodexTicket(context.Background(), ticketTestAccount(41), "gpt-6-astra", h)
	require.NoError(t, err)
	require.Equal(t, "client-state", h.Get(openAICodexTurnStateHeader))
}

func TestHarvestOpenAICodexTicket_StopsAt292AndUsesHarvestProxy(t *testing.T) {
	state312 := fakeCodexTicketState(312)
	state292 := fakeCodexTicketState(292)
	header312 := http.Header{}
	header312.Set(openAICodexTurnStateHeader, state312)
	header292 := http.Header{}
	header292.Set(openAICodexTurnStateHeader, state292)
	addCodexTicketTestRouteCookies(header292)
	upstream := &httpUpstreamRecorder{
		responses: []*http.Response{
			{
				StatusCode: http.StatusOK,
				Header:     header312,
				Body:       io.NopCloser(strings.NewReader("data: {}\n\n")),
			},
			{
				StatusCode: http.StatusOK,
				Header:     header292,
				Body:       io.NopCloser(strings.NewReader(codexTicketProbeSuccessSSE("gpt-6-astra"))),
			},
		},
	}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:                      true,
		TargetLength:                 292,
		TTLSeconds:                   3600,
		HarvestProxyURL:              "socks5h://user:pass@harvest.example:31",
		HarvestAttemptTimeoutSeconds: 5,
		FailClosed:                   true,
	}, upstream)
	account := ticketTestAccount(41)

	svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")
	require.Nil(t, svc.lookupOpenAICodexTicket(account, "gpt-6-astra"))
	svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")
	ticket := svc.lookupOpenAICodexTicket(account, "gpt-6-astra")
	require.NotNil(t, ticket)
	require.Equal(t, state292, ticket.State)
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, "stale")
	require.NoError(t, svc.applyOpenAICodexTicket(context.Background(), account, "gpt-6-astra", h))
	require.Equal(t, state292, h.Get(openAICodexTurnStateHeader))
	require.Equal(t, "socks5h://user:pass@harvest.example:31", upstream.lastProxyURL)
	require.Len(t, upstream.requests, 2)
	require.Empty(t, upstream.requests[0].Header.Get(openAICodexTurnStateHeader))
	require.Equal(t, openAICodexAstraMinVersion, upstream.requests[0].Header.Get("version"))
	require.Equal(t, HTTPUpstreamProfileOpenAIHarvest, HTTPUpstreamProfileFromContext(upstream.requests[0].Context()))
	require.True(t, upstream.requests[0].Close)
}

func TestHarvestOpenAICodexTicket_HTTP503DoesNotAbortHunt(t *testing.T) {
	state292 := fakeCodexTicketState(292)
	header503 := http.Header{}
	header292 := http.Header{}
	header292.Set(openAICodexTurnStateHeader, state292)
	addCodexTicketTestRouteCookies(header292)
	responses := make([]*http.Response, 0, 3)
	responses = append(responses, &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     header503,
		Body:       io.NopCloser(strings.NewReader(`{"error":"overloaded"}`)),
	})
	responses = append(responses, &http.Response{
		StatusCode: http.StatusOK,
		Header:     header292,
		Body:       io.NopCloser(strings.NewReader(codexTicketProbeSuccessSSE("gpt-6-astra"))),
	})
	upstream := &httpUpstreamRecorder{responses: responses}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:                      true,
		TargetLength:                 292,
		TTLSeconds:                   3600,
		HarvestProxyURL:              "socks5h://harvest.example:31",
		HarvestAttemptTimeoutSeconds: 5,
		FailClosed:                   true,
	}, upstream)
	account := ticketTestAccount(41)
	svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")
	require.Nil(t, svc.lookupOpenAICodexTicket(account, "gpt-6-astra"))
	svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")
	ticket := svc.lookupOpenAICodexTicket(account, "gpt-6-astra")
	require.NotNil(t, ticket)
	require.Equal(t, state292, ticket.State)
	require.Len(t, upstream.requests, 2)
}

func TestLookupOpenAICodexTicket_HydratesFromExtra(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, TargetLength: 292, TTLSeconds: 3600}, nil)
	state := fakeCodexTicketState(292)
	account := ticketTestAccount(9)
	account.Extra = map[string]any{
		openAICodexTicketExtraKey("gpt-6-astra"): map[string]any{
			"state":       state,
			"length":      292,
			"model":       "gpt-6-astra",
			"captured_at": time.Now().Add(-time.Minute),
			"expires_at":  time.Now().Add(time.Hour),
		},
	}
	got := svc.lookupOpenAICodexTicket(account, "gpt-6-astra")
	require.NotNil(t, got)
	require.Equal(t, state, got.State)
	require.True(t, got.valid(time.Now(), 292, time.Hour))
}

func TestOpenAICodexTicketStatuses_ReportsRemainingTTL(t *testing.T) {
	now := time.Now()
	account := ticketTestAccount(41)
	account.Extra = map[string]any{
		openAICodexTicketExtraKey("gpt-6-astra"): map[string]any{
			"state":       fakeCodexTicketState(292),
			"length":      292,
			"model":       "gpt-6-astra",
			"captured_at": now.Add(-10 * time.Minute),
			"expires_at":  now.Add(50 * time.Minute),
		},
		openAICodexRouteCookieExtraKey: &openAICodexRouteCookie{
			AccountID:  41,
			Values:     map[string]string{"__cflb": "route-test"},
			CapturedAt: now.Add(-10 * time.Minute),
			ExpiresAt:  now.Add(50 * time.Minute),
		},
	}
	got := OpenAICodexTicketStatuses(account, config.OpenAICodexTicketConfig{Enabled: true, FailClosed: true, TTLSeconds: 3600}, now)
	require.Len(t, got, 2)
	require.Equal(t, "gpt-6-astra", got[0].Model)
	require.True(t, got[0].Ready)
	require.Greater(t, got[0].RemainingSeconds, int64(40*60))
	require.LessOrEqual(t, got[0].RemainingSeconds, int64(50*60))
	require.Equal(t, "gpt-5.6-sol", got[1].Model)
	require.False(t, got[1].Ready)
}

func TestOpenAICodexTicketStatuses_ReportLastProbeDiagnostic(t *testing.T) {
	cfg := config.OpenAICodexTicketConfig{
		Enabled:         true,
		FailClosed:      true,
		HarvestProxyURL: "http://proxy.example:8080",
		Models:          []string{"gpt-6-astra"},
	}
	svc := ticketTestService(t, cfg, nil)
	account := ticketTestAccount(41)
	settings := svc.openAICodexTicketRuntimeSettingsContext(context.Background())
	key := openAICodexTicketKey(account.ID, "gpt-6-astra")
	svc.recordOpenAICodexTicketProbeDiagnostic(
		key, settings,
		openAICodexTicketProbeModelMismatch, http.StatusOK, 292, "gpt-5.6-luna",
	)

	status := svc.OpenAICodexTicketStatuses(account, cfg, time.Now())
	require.Len(t, status, 1)
	require.Equal(t, string(openAICodexTicketProbeModelMismatch), status[0].LastProbeReason)
	require.Equal(t, http.StatusOK, status[0].LastProbeHTTPStatus)
	require.Equal(t, 292, status[0].LastProbeStateLength)
	require.Equal(t, "gpt-5.6-luna", status[0].LastProbeServedModel)
}

func TestExtractOpenAICodexTicketModel(t *testing.T) {
	require.Equal(t, "gpt-6-astra", extractOpenAICodexTicketModel([]byte(`{"model":"gpt-6-astra"}`)))
	require.Empty(t, extractOpenAICodexTicketModel([]byte(`{}`)))
}

// These stubs exercise the real continuous refresh path with both default models
// completing together. Run under -race to catch writes to the shared account maps.
type codexTicketRefreshRepo struct {
	AccountRepository
	accounts []Account
	mu       sync.Mutex
	updates  map[string]any
}

func (r *codexTicketRefreshRepo) ListByPlatform(context.Context, string) ([]Account, error) {
	return r.accounts, nil
}
func (r *codexTicketRefreshRepo) UpdateExtra(_ context.Context, _ int64, updates map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.updates == nil {
		r.updates = make(map[string]any)
	}
	for k, v := range updates {
		r.updates[k] = v
	}
	return nil
}

type codexTicketConcurrentUpstream struct {
	HTTPUpstream
	started atomic.Int64
	ready   chan struct{}
}

func (u *codexTicketConcurrentUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	if u.started.Add(1) == 2 {
		close(u.ready)
	}
	select {
	case <-u.ready:
	case <-req.Context().Done():
		return nil, req.Context().Err()
	}
	h := http.Header{}
	h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(292))
	addCodexTicketTestRouteCookies(h)
	return &http.Response{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader(codexTicketProbeSuccessSSE(codexTicketProbeModelFromRequest(req))))}, nil
}

type codexTicketFanoutUpstream struct {
	HTTPUpstream
	winner    string
	expected  int64
	started   atomic.Int64
	canceled  atomic.Int64
	ready     chan struct{}
	readyOnce sync.Once
	mu        sync.Mutex
	proxies   []string
}

func (u *codexTicketFanoutUpstream) Do(req *http.Request, proxyURL string, _ int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.proxies = append(u.proxies, proxyURL)
	u.mu.Unlock()
	if u.started.Add(1) == u.expected {
		u.readyOnce.Do(func() { close(u.ready) })
	}
	select {
	case <-u.ready:
	case <-req.Context().Done():
		u.canceled.Add(1)
		return nil, req.Context().Err()
	}
	if u.winner != "" && strings.Contains(proxyURL, u.winner) {
		h := http.Header{}
		h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(292))
		addCodexTicketTestRouteCookies(h)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     h,
			Body:       io.NopCloser(strings.NewReader(codexTicketProbeSuccessSSE(codexTicketProbeModelFromRequest(req)))),
		}, nil
	}
	if u.winner == "" {
		h := http.Header{}
		h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(312))
		return &http.Response{StatusCode: http.StatusOK, Header: h, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	<-req.Context().Done()
	u.canceled.Add(1)
	return nil, req.Context().Err()
}

func fourCodexTicketTestProxies() []OpenAICodexTicketProxy {
	return []OpenAICodexTicketProxy{
		testCodexProxy("first", 0, 1),
		testCodexProxy("second", 0, 1),
		testCodexProxy("third", 0, 1),
		testCodexProxy("winner", 0, 1),
	}
}

func TestOpenAICodexTicketProbeRacesFourProxiesAndCancelsLosers(t *testing.T) {
	const fanout = 4
	upstream := &codexTicketFanoutUpstream{
		winner:   "winner.example",
		expected: fanout,
		ready:    make(chan struct{}),
	}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:                      true,
		TargetLength:                 292,
		Models:                       []string{"gpt-6-astra"},
		HarvestAttemptTimeoutSeconds: 5,
	}, upstream)
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.ProxyPool = fourCodexTicketTestProxies()
	account := ticketTestAccount(41)

	require.False(t, svc.probeOnceOpenAICodexTicketWithSettings(context.Background(), account, "gpt-6-astra", settings))
	require.Equal(t, int64(fanout), upstream.started.Load())
	require.Equal(t, int64(fanout-1), upstream.canceled.Load())

	upstream.mu.Lock()
	proxies := append([]string(nil), upstream.proxies...)
	upstream.mu.Unlock()
	require.Len(t, proxies, fanout)
	require.Len(t, map[string]struct{}{proxies[0]: {}, proxies[1]: {}, proxies[2]: {}, proxies[3]: {}}, fanout)
	require.NotNil(t, svc.lookupOpenAICodexTicket(account, "gpt-6-astra"))
	_, hasRetryState := svc.openaiCodexTicketProbeStates.Load(openAICodexTicketKey(account.ID, "gpt-6-astra"))
	require.False(t, hasRetryState)

	svc.openaiCodexTicketProxyPool.mu.Lock()
	require.Empty(t, svc.openaiCodexTicketProxyPool.penalizedTo)
	svc.openaiCodexTicketProxyPool.mu.Unlock()
}

func TestOpenAICodexTicketFourProxyMissCountsOneRoundAndKeepsOldTicket(t *testing.T) {
	const fanout = 4
	upstream := &codexTicketFanoutUpstream{
		expected: fanout,
		ready:    make(chan struct{}),
	}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:                      true,
		TargetLength:                 292,
		Models:                       []string{"gpt-6-astra"},
		HarvestAttemptTimeoutSeconds: 5,
	}, upstream)
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.ProxyPool = fourCodexTicketTestProxies()
	account := ticketTestAccount(41)
	oldState := openAICodexTicketStatePrefix + strings.Repeat("C", 286)
	now := time.Now()
	svc.storeOpenAICodexTicket(context.Background(), account, &openAICodexTicket{
		AccountID: account.ID, Model: "gpt-6-astra", State: oldState, Length: 292,
		CapturedAt: now, ExpiresAt: now.Add(settings.TTL()),
	}, nil)

	require.False(t, svc.probeOnceOpenAICodexTicketWithSettings(context.Background(), account, "gpt-6-astra", settings))
	require.Equal(t, int64(fanout), upstream.started.Load())
	require.Equal(t, oldState, svc.lookupOpenAICodexTicket(account, "gpt-6-astra").State)
	require.Equal(t, "route-test", svc.lookupOpenAICodexRouteCookie(account).Values["__cflb"])
	raw, ok := svc.openaiCodexTicketProbeStates.Load(openAICodexTicketKey(account.ID, "gpt-6-astra"))
	require.True(t, ok)
	probeState, ok := raw.(openAICodexTicketProbeState)
	require.True(t, ok)
	require.Equal(t, 1, probeState.Failures)
}

func TestOpenAICodexTicketProbeUsesIndependentParallelProxySessions(t *testing.T) {
	const parallelism = 4
	upstream := &codexTicketFanoutUpstream{
		expected: parallelism,
		ready:    make(chan struct{}),
	}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:                      true,
		TargetLength:                 292,
		Models:                       []string{"gpt-6-astra"},
		HarvestAttemptTimeoutSeconds: 5,
	}, upstream)
	proxy := testCodexProxy("dynamic", 0, 1)
	proxy.URL = "http://user_" + openAICodexTicketProxySessionPlaceholder + ":secret@dynamic.example:8080"
	proxy.Parallelism = parallelism
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.ProxyPool = []OpenAICodexTicketProxy{proxy}
	account := ticketTestAccount(41)

	require.False(t, svc.probeOnceOpenAICodexTicketWithSettings(context.Background(), account, "gpt-6-astra", settings))
	require.Equal(t, int64(parallelism), upstream.started.Load())
	upstream.mu.Lock()
	proxies := append([]string(nil), upstream.proxies...)
	upstream.mu.Unlock()
	require.Len(t, proxies, parallelism)
	unique := make(map[string]struct{}, parallelism)
	for _, proxyURL := range proxies {
		unique[proxyURL] = struct{}{}
		require.NotContains(t, proxyURL, openAICodexTicketProxySessionPlaceholder)
	}
	require.Len(t, unique, parallelism)
}

func TestRefreshOpenAICodexTickets_ConcurrentModelsPreserveAccountSnapshot(t *testing.T) {
	account := ticketTestAccount(41)
	account.Status = StatusActive
	account.Extra = map[string]any{"existing": true}
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	upstream := &codexTicketConcurrentUpstream{ready: make(chan struct{})}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, HarvestProxyURL: "socks5h://proxy.example.com:1080"}, upstream)
	svc.accountRepo = repo
	svc.refreshOpenAICodexTickets(context.Background())
	require.Equal(t, int64(2), upstream.started.Load())
	require.Equal(t, map[string]any{"existing": true}, account.Extra)
	require.Len(t, repo.updates, 3)
	require.Contains(t, repo.updates, openAICodexRouteCookieExtraKey)
	for _, model := range []string{openAICodexTicketDefaultModel, openAICodexTicketDefaultSolModel} {
		ticket := svc.lookupOpenAICodexTicket(account, model)
		require.NotNil(t, ticket)
		require.True(t, ticket.valid(time.Now(), 292, time.Hour))
	}
	// Valid tickets do not produce another probe on the next cycle.
	svc.refreshOpenAICodexTickets(context.Background())
	require.Equal(t, int64(2), upstream.started.Load())
}

func TestRefreshOpenAICodexTickets_RefreshesBeforeExpiry(t *testing.T) {
	account := ticketTestAccount(41)
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	var calls atomic.Int64
	upstream := &codexTicketFuncUpstream{do: func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return codexTicketResponse(), nil
	}}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:              true,
		HarvestProxyURL:      "socks5h://proxy.example.com:1080",
		Models:               []string{"gpt-6-astra"},
		TTLSeconds:           3600,
		RefreshBeforeSeconds: 600,
	}, upstream)
	svc.accountRepo = repo

	oldState := openAICodexTicketStatePrefix + strings.Repeat("C", 286)
	oldExpiry := time.Now().Add(9 * time.Minute)
	svc.storeOpenAICodexTicket(context.Background(), account, &openAICodexTicket{
		AccountID:  account.ID,
		Model:      "gpt-6-astra",
		State:      oldState,
		Length:     292,
		CapturedAt: time.Now().Add(-51 * time.Minute),
		ExpiresAt:  oldExpiry,
	}, nil)
	require.True(t, svc.lookupOpenAICodexTicket(account, "gpt-6-astra").valid(time.Now(), 292, time.Hour))

	svc.refreshOpenAICodexTickets(context.Background())

	refreshed := svc.lookupOpenAICodexTicket(account, "gpt-6-astra")
	require.Equal(t, int64(1), calls.Load())
	require.NotNil(t, refreshed)
	require.NotEqual(t, oldState, refreshed.State)
	require.True(t, refreshed.ExpiresAt.After(oldExpiry))
}

func TestOpenAICodexTicketProbeRetryStaysAtConfiguredInterval(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{}, nil)
	key := openAICodexTicketKey(41, "gpt-6-astra")
	now := time.Date(2026, time.September, 20, 0, 0, 0, 0, time.UTC)
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.RetryIntervalSeconds = 7

	for i := 0; i < 20; i++ {
		state := svc.recordOpenAICodexTicketProbeFailure(key, settings, now)
		require.Equal(t, i+1, state.Failures)
		require.Equal(t, 7*time.Second, state.RetryAt.Sub(now))
		require.True(t, svc.openAICodexTicketProbeWaiting(key, settings, now))
	}

	changed := settings
	changed.ProxyPool = []OpenAICodexTicketProxy{testCodexProxy("new", 0, 1)}
	require.False(t, svc.openAICodexTicketProbeWaiting(key, changed, now))
	state := svc.recordOpenAICodexTicketProbeFailure(key, changed, now)
	require.Equal(t, 1, state.Failures)
	require.Equal(t, 7*time.Second, state.RetryAt.Sub(now))
}

func TestOpenAICodexTicketProxySessionsAreIndependentAndRotated(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{}, nil)
	template := "http://customer_90_" + openAICodexTicketProxySessionPlaceholder + ":secret@proxy.example:8080"
	astraKey := openAICodexTicketKey(41, "gpt-6-astra")
	solKey := openAICodexTicketKey(41, "gpt-5.6-sol")

	astraFirst := svc.openAICodexTicketProbeProxyURL(astraKey, template)
	require.NotContains(t, astraFirst, openAICodexTicketProxySessionPlaceholder)
	require.Equal(t, astraFirst, svc.openAICodexTicketProbeProxyURL(astraKey, template))
	require.NotEqual(t, astraFirst, svc.openAICodexTicketProbeProxyURL(solKey, template))

	require.Equal(t, 1, svc.rotateOpenAICodexTicketProxySession(astraKey, template))
	astraSecond := svc.openAICodexTicketProbeProxyURL(astraKey, template)
	require.NotEqual(t, astraFirst, astraSecond)

	svc.markOpenAICodexTicketProxySessionSuccessful(astraKey, template)
	require.Equal(t, 1, svc.rotateOpenAICodexTicketProxySession(astraKey, template))
	require.Equal(t, "http://fixed.example:8080", svc.openAICodexTicketProbeProxyURL(astraKey, "http://fixed.example:8080"))
	require.Zero(t, svc.rotateOpenAICodexTicketProxySession(astraKey, "http://fixed.example:8080"))
}

func TestOpenAICodexTicketProbeRotatesTemplatedProxyAndRetainsSuccess(t *testing.T) {
	template := "http://customer_90_" + openAICodexTicketProxySessionPlaceholder + ":secret@proxy.example:8080"
	upstream := &codexTicketProxySequenceUpstream{lengths: []int{312, 292, 292}}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:         true,
		HarvestProxyURL: template,
		Models:          []string{"gpt-6-astra"},
	}, upstream)
	account := ticketTestAccount(41)

	svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")
	svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")
	svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")

	require.Len(t, upstream.proxies, 3)
	for _, proxyURL := range upstream.proxies {
		require.NotContains(t, proxyURL, openAICodexTicketProxySessionPlaceholder)
	}
	require.NotEqual(t, upstream.proxies[0], upstream.proxies[1])
	require.Equal(t, upstream.proxies[1], upstream.proxies[2])
}

func TestOpenAICodexTicketProbeDoesNotRotateSessionOnHTTP400(t *testing.T) {
	template := "http://customer_90_" + openAICodexTicketProxySessionPlaceholder + ":secret@proxy.example:8080"
	upstream := &codexTicketProxySequenceUpstream{
		lengths:  []int{0, 0},
		statuses: []int{http.StatusBadRequest, http.StatusBadRequest},
	}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:         true,
		HarvestProxyURL: template,
		Models:          []string{"gpt-6-astra"},
	}, upstream)
	account := ticketTestAccount(41)
	key := openAICodexTicketKey(account.ID, "gpt-6-astra")

	svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")
	svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")

	require.Len(t, upstream.proxies, 2)
	require.Equal(t, []string{upstream.proxies[0], upstream.proxies[0]}, upstream.proxies)
	raw, ok := svc.openaiCodexTicketProbeStates.Load(key)
	require.True(t, ok)
	probeState, ok := raw.(openAICodexTicketProbeState)
	require.True(t, ok)
	require.Equal(t, 2, probeState.Failures)
	require.Greater(t, time.Until(probeState.RetryAt), 4*time.Second)
	require.Less(t, time.Until(probeState.RetryAt), 7*time.Second)
}

func TestRefreshOpenAICodexTicketsRotatesFixedProxyOnceAfterConcurrentMisses(t *testing.T) {
	account := ticketTestAccount(41)
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	upstream := &codexTicketRotatableProxyUpstream{
		probeLength:              312,
		expectedProbesAtRotation: 2,
	}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:               true,
		HarvestProxyURL:       "http://fixed-proxy.example:8080",
		HarvestProxyRotateURL: "http://rotate.example/change",
		Models:                []string{"gpt-6-astra", "gpt-5.6-sol"},
	}, upstream)
	svc.accountRepo = repo

	svc.refreshOpenAICodexTickets(context.Background())

	require.Equal(t, 2, upstream.probeCalls)
	require.Equal(t, 1, upstream.rotateCalls)
	require.True(t, upstream.rotatedAfterAllProbes)
	require.Equal(t, []string{
		"http://fixed-proxy.example:8080",
		"http://fixed-proxy.example:8080",
		"http://fixed-proxy.example:8080",
	}, upstream.proxyURLs)
	for _, model := range []string{"gpt-6-astra", "gpt-5.6-sol"} {
		raw, ok := svc.openaiCodexTicketProbeStates.Load(openAICodexTicketKey(account.ID, model))
		require.True(t, ok)
		probeState, ok := raw.(openAICodexTicketProbeState)
		require.True(t, ok)
		require.Less(t, time.Until(probeState.RetryAt), 7*time.Second)
	}
}

func TestOpenAICodexTicketFixedProxyRotationSelectsMarkedPoolNode(t *testing.T) {
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		HarvestProxyRotateURL: "http://rotate.example/change",
	}, &codexTicketRotatableProxyUpstream{})
	settings := DefaultOpenAICodexTicketRuntimeSettings()
	settings.ProxyPool = []OpenAICodexTicketProxy{
		testCodexProxy("primary", 0, 100),
		testCodexProxy("backup", 10, 100),
	}
	settings.ProxyPool[0].RotateOnFailure = true

	proxy, ok := svc.openAICodexTicketFixedProxyForRotation(settings)

	require.True(t, ok)
	require.Equal(t, "primary", proxy.ID)
	require.True(t, svc.openAICodexTicketCanRotateFixedProxy(settings.ProxyPool[0], settings))
	require.False(t, svc.openAICodexTicketCanRotateFixedProxy(settings.ProxyPool[1], settings))
}

func TestRefreshOpenAICodexTicketsRotatesFixedProxyOnHTTP403(t *testing.T) {
	account := ticketTestAccount(41)
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	upstream := &codexTicketRotatableProxyUpstream{
		probeStatus:              http.StatusForbidden,
		expectedProbesAtRotation: 1,
	}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:               true,
		HarvestProxyURL:       "http://fixed-proxy.example:8080",
		HarvestProxyRotateURL: "http://rotate.example/change",
		Models:                []string{"gpt-6-astra"},
	}, upstream)
	svc.accountRepo = repo

	svc.refreshOpenAICodexTickets(context.Background())

	require.Equal(t, 1, upstream.probeCalls)
	require.Equal(t, 1, upstream.rotateCalls)
}

func TestRefreshOpenAICodexTicketsDoesNotRotateFixedProxyOnHTTP400(t *testing.T) {
	account := ticketTestAccount(41)
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	upstream := &codexTicketRotatableProxyUpstream{probeStatus: http.StatusBadRequest}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:               true,
		HarvestProxyURL:       "http://fixed-proxy.example:8080",
		HarvestProxyRotateURL: "http://rotate.example/change",
		Models:                []string{"gpt-6-astra"},
	}, upstream)
	svc.accountRepo = repo

	svc.refreshOpenAICodexTickets(context.Background())

	require.Equal(t, 1, upstream.probeCalls)
	require.Zero(t, upstream.rotateCalls)
	raw, ok := svc.openaiCodexTicketProbeStates.Load(openAICodexTicketKey(account.ID, "gpt-6-astra"))
	require.True(t, ok)
	probeState, ok := raw.(openAICodexTicketProbeState)
	require.True(t, ok)
	require.Greater(t, time.Until(probeState.RetryAt), 4*time.Second)
	require.Less(t, time.Until(probeState.RetryAt), 7*time.Second)
}

func TestRefreshOpenAICodexTickets_WaitsUntilRetryOrPolicyChanges(t *testing.T) {
	account := ticketTestAccount(41)
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	var calls atomic.Int64
	upstream := &codexTicketFuncUpstream{do: func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		h := http.Header{}
		h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(312))
		return &http.Response{StatusCode: http.StatusOK, Header: h, Body: io.NopCloser(strings.NewReader(""))}, nil
	}}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:         true,
		HarvestProxyURL: "http://proxy-a.example",
		Models:          []string{"gpt-6-astra"},
	}, upstream)
	svc.accountRepo = repo

	svc.refreshOpenAICodexTickets(context.Background())
	svc.refreshOpenAICodexTickets(context.Background())
	require.Equal(t, int64(1), calls.Load())

	svc.cfg.Gateway.OpenAICodexTicket.HarvestProxyURL = "http://proxy-b.example"
	svc.refreshOpenAICodexTickets(context.Background())
	require.Equal(t, int64(2), calls.Load())
}

func TestOpenAICodexTicketProbeEligible(t *testing.T) {
	now := time.Now()
	future := now.Add(time.Hour)
	past := now.Add(-time.Minute)
	fresh := now.Add(-time.Minute)
	stale := now.Add(-openAICodexAutoPauseStaleAfter - time.Minute)

	accountWithExtra := func(extra map[string]any) *Account {
		account := ticketTestAccount(41)
		account.Extra = extra
		return account
	}

	tests := []struct {
		name    string
		account *Account
		want    bool
	}{
		{name: "active", account: ticketTestAccount(41), want: true},
		{name: "manually disabled", account: func() *Account {
			account := ticketTestAccount(41)
			account.Schedulable = false
			return account
		}(), want: false},
		{name: "rate limited", account: func() *Account {
			account := ticketTestAccount(41)
			account.RateLimitResetAt = &future
			return account
		}(), want: false},
		{name: "five hour quota exhausted", account: accountWithExtra(map[string]any{
			"codex_5h_used_percent": 100.0,
			"codex_5h_reset_at":     future.Format(time.RFC3339),
		}), want: false},
		{name: "seven day quota exhausted", account: accountWithExtra(map[string]any{
			"codex_7d_used_percent": 101.0,
			"codex_7d_reset_at":     future.Format(time.RFC3339),
		}), want: false},
		{name: "quota reset elapsed", account: accountWithExtra(map[string]any{
			"codex_7d_used_percent": 100.0,
			"codex_7d_reset_at":     past.Format(time.RFC3339),
		}), want: true},
		{name: "quota below full", account: accountWithExtra(map[string]any{
			"codex_7d_used_percent": 99.9,
			"codex_7d_reset_at":     future.Format(time.RFC3339),
		}), want: true},
		{name: "fresh full quota without reset", account: accountWithExtra(map[string]any{
			"codex_5h_used_percent":  100.0,
			"codex_usage_updated_at": fresh.Format(time.RFC3339),
		}), want: false},
		{name: "stale full quota without reset self heals", account: accountWithExtra(map[string]any{
			"codex_5h_used_percent":  100.0,
			"codex_usage_updated_at": stale.Format(time.RFC3339),
		}), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, openAICodexTicketProbeEligible(tt.account, now))
		})
	}
}

func TestRefreshOpenAICodexTickets_SkipsIneligibleAccounts(t *testing.T) {
	now := time.Now()
	active := ticketTestAccount(41)
	disabled := ticketTestAccount(42)
	disabled.Schedulable = false
	exhausted := ticketTestAccount(43)
	exhausted.Extra = map[string]any{
		"codex_7d_used_percent": 100.0,
		"codex_7d_reset_at":     now.Add(time.Hour).Format(time.RFC3339),
	}

	repo := &codexTicketRefreshRepo{accounts: []Account{*active, *disabled, *exhausted}}
	var calls atomic.Int64
	upstream := &codexTicketFuncUpstream{do: func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return codexTicketResponse(), nil
	}}
	svc := ticketTestService(t, config.OpenAICodexTicketConfig{
		Enabled:         true,
		HarvestProxyURL: "socks5h://proxy.example.com:1080",
		Models:          []string{"gpt-6-astra"},
	}, upstream)
	svc.accountRepo = repo

	svc.refreshOpenAICodexTickets(context.Background())

	require.Equal(t, int64(1), calls.Load())
	require.NotNil(t, svc.lookupOpenAICodexTicket(active, "gpt-6-astra"))
	require.Nil(t, svc.lookupOpenAICodexTicket(disabled, "gpt-6-astra"))
	require.Nil(t, svc.lookupOpenAICodexTicket(exhausted, "gpt-6-astra"))
}

func TestOpenAICodexTicketStatuses_RespectRuntimeConfiguration(t *testing.T) {
	account := ticketTestAccount(41)
	require.Empty(t, OpenAICodexTicketStatuses(account, config.OpenAICodexTicketConfig{}, time.Now()))
	cfg := config.OpenAICodexTicketConfig{Enabled: true, Models: []string{"custom-model"}}
	status := OpenAICodexTicketStatuses(account, cfg, time.Now())
	require.Len(t, status, 1)
	require.Equal(t, "custom-model", status[0].Model)
	require.False(t, status[0].Blocked)
	cfg.FailClosed = true
	require.True(t, OpenAICodexTicketStatuses(account, cfg, time.Now())[0].Blocked)
}

func TestRequestOpenAICodexTicketRetryIsScopedAndRateLimited(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	upstream := &codexTicketFuncUpstream{do: func(req *http.Request) (*http.Response, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
			h := http.Header{}
			h.Set(openAICodexTurnStateHeader, fakeCodexTicketState(312))
			return &http.Response{StatusCode: http.StatusOK, Header: h, Body: io.NopCloser(strings.NewReader(""))}, nil
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}}
	cfg := config.OpenAICodexTicketConfig{
		Enabled:         true,
		FailClosed:      true,
		HarvestProxyURL: "http://proxy.example:8080",
		Models:          []string{"gpt-6-astra", "gpt-5.6-sol"},
	}
	svc := ticketTestService(t, cfg, upstream)
	account := ticketTestAccount(41)

	retryAt, err := svc.RequestOpenAICodexTicketRetry(context.Background(), account, "gpt-6-astra")
	require.NoError(t, err)
	require.Greater(t, time.Until(retryAt), 50*time.Second)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("manual probe did not start")
	}
	status := svc.OpenAICodexTicketStatuses(account, cfg, time.Now())
	require.True(t, status[0].Probing)
	require.False(t, status[0].ManualRetryAllowed)
	_, err = svc.RequestOpenAICodexTicketRetry(context.Background(), account, "gpt-6-astra")
	require.ErrorIs(t, err, ErrOpenAICodexTicketRetryInProgress)

	close(release)
	require.Eventually(t, func() bool {
		status = svc.OpenAICodexTicketStatuses(account, cfg, time.Now())
		return !status[0].Probing
	}, time.Second, 10*time.Millisecond)
	_, err = svc.RequestOpenAICodexTicketRetry(context.Background(), account, "gpt-6-astra")
	require.ErrorIs(t, err, ErrOpenAICodexTicketRetryCooldown)
	status = svc.OpenAICodexTicketStatuses(account, cfg, time.Now())
	require.True(t, status[0].Blocked)
	require.Greater(t, status[0].RetryInSeconds, int64(0))
	require.Greater(t, status[0].ManualRetryInSeconds, int64(0))
	require.False(t, status[0].ManualRetryAllowed)
}

func TestRequestOpenAICodexTicketRetryHarvestsTicket(t *testing.T) {
	cfg := config.OpenAICodexTicketConfig{
		Enabled:         true,
		FailClosed:      true,
		HarvestProxyURL: "http://proxy.example:8080",
		Models:          []string{"gpt-6-astra"},
	}
	svc := ticketTestService(t, cfg, &codexTicketFuncUpstream{do: func(*http.Request) (*http.Response, error) {
		return codexTicketResponse(), nil
	}})
	account := ticketTestAccount(41)

	_, err := svc.RequestOpenAICodexTicketRetry(context.Background(), account, "gpt-6-astra")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		status := svc.OpenAICodexTicketStatuses(account, cfg, time.Now())
		return len(status) == 1 && status[0].Ready && !status[0].Probing
	}, time.Second, 10*time.Millisecond)
	_, err = svc.RequestOpenAICodexTicketRetry(context.Background(), account, "gpt-6-astra")
	require.ErrorIs(t, err, ErrOpenAICodexTicketRetryAlreadyReady)
}

func TestProbeOpenAICodexTicket_RejectsInvalidState(t *testing.T) {
	for _, state := range []string{fakeCodexTicketState(312), strings.Repeat("X", 292), ""} {
		h := http.Header{}
		h.Set(openAICodexTurnStateHeader, state)
		upstream := &httpUpstreamRecorder{responses: []*http.Response{{StatusCode: 200, Header: h, Body: io.NopCloser(strings.NewReader(""))}}}
		svc := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true, HarvestProxyURL: "http://proxy.example.com:8080"}, upstream)
		account := ticketTestAccount(41)
		svc.probeOnceOpenAICodexTicket(context.Background(), account, "gpt-6-astra")
		require.Nil(t, svc.lookupOpenAICodexTicket(account, "gpt-6-astra"))
	}
}
func TestOpenAICodexTicket_RequiresActualLengthAndExpiry(t *testing.T) {
	ticket := &openAICodexTicket{State: fakeCodexTicketState(312), Length: 292, ExpiresAt: time.Now().Add(time.Hour)}
	require.False(t, ticket.valid(time.Now(), 292, time.Hour))
	ticket.State = fakeCodexTicketState(292)
	ticket.ExpiresAt = time.Time{}
	require.False(t, ticket.valid(time.Now(), 292, time.Hour))
}

// /responses/compact 的出站模型被 Forward 改写为 gateway.openai_compact_model
// （默认非空），门票门控必须按该出站模型判定。否则对门控模型发 compact 请求时，
// 所有无票账号都会被 fail_closed 误判为不可调度，而这些请求实际不需要票。
func TestOpenAICodexTicketGate_CompactRequestUsesForwardOutboundModel(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		OpenAICompactModel: "gpt-5.5",
		OpenAICodexTicket: config.OpenAICodexTicketConfig{
			Enabled:      true,
			TargetLength: 292,
			TTLSeconds:   3600,
			FailClosed:   true,
			Models:       []string{"gpt-6-astra"},
		},
	}}}
	account := ticketTestAccount(41) // 无票

	// 出站模型预测必须与 Forward 的解析链一致。
	require.Equal(t, "gpt-6-astra", svc.openAICodexTicketOutboundModel(account, "gpt-6-astra", false))
	require.Equal(t, "gpt-5.5", svc.openAICodexTicketOutboundModel(account, "gpt-6-astra", true))

	// 普通请求：出站仍是门控模型且无票 → fail_closed 必须拦号。
	require.True(t, svc.isOpenAIAccountRequestRuntimeBlocked(account, "gpt-6-astra", false))

	// compact 请求：出站已被改写成非门控的 gpt-5.5 → 不得拦号。
	require.False(t, svc.isOpenAIAccountRequestRuntimeBlocked(account, "gpt-6-astra", true))

	// 回归锚点：按客户端原始模型判定（旧实现的口径）在 compact 下必然误拦。
	require.True(t, svc.openAICodexTicketBlocksAccount(account, canonicalOpenAIAccountSchedulingModel(account, "gpt-6-astra")))
}
