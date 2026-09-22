package service

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"
)

type openAICodexTicketProxyAttemptState struct {
	fingerprint [sha256.Size]byte
	attempted   map[string]struct{}
}

// openAICodexTicketProxyPoolRuntime owns process-local balancing state. All
// maps are lazy so OpenAIGatewayService remains safe in tests built as literals.
type openAICodexTicketProxyPoolRuntime struct {
	mu          sync.Mutex
	current     map[string]int
	active      map[string]int
	penalizedTo map[string]time.Time
	attempts    map[string]*openAICodexTicketProxyAttemptState
}

type openAICodexTicketSelectedProxy struct {
	proxy       OpenAICodexTicketProxy
	fingerprint [sha256.Size]byte
	release     func()
}

func openAICodexTicketProxyNodeKey(fingerprint [sha256.Size]byte, id string) string {
	return hex.EncodeToString(fingerprint[:]) + "\x00" + id
}

func openAICodexTicketProxySessionKey(ticketKey, proxyID string) string {
	return ticketKey + "\x00proxy\x00" + proxyID
}

func (r *openAICodexTicketProxyPoolRuntime) ensureMaps() {
	if r.current == nil {
		r.current = make(map[string]int)
	}
	if r.active == nil {
		r.active = make(map[string]int)
	}
	if r.penalizedTo == nil {
		r.penalizedTo = make(map[string]time.Time)
	}
	if r.attempts == nil {
		r.attempts = make(map[string]*openAICodexTicketProxyAttemptState)
	}
}

func enabledOpenAICodexTicketProxies(pool []OpenAICodexTicketProxy) []OpenAICodexTicketProxy {
	result := make([]OpenAICodexTicketProxy, 0, len(pool))
	for _, proxy := range pool {
		if proxy.Enabled && strings.TrimSpace(proxy.URL) != "" {
			result = append(result, proxy)
		}
	}
	return result
}

func (r *openAICodexTicketProxyPoolRuntime) selectProxy(key string, settings OpenAICodexTicketRuntimeSettings, now time.Time) (openAICodexTicketSelectedProxy, bool) {
	enabled := enabledOpenAICodexTicketProxies(settings.ProxyPool)
	if len(enabled) == 0 {
		return openAICodexTicketSelectedProxy{}, false
	}
	fingerprint := settings.Fingerprint()

	r.mu.Lock()
	r.ensureMaps()
	attemptState := r.attempts[key]
	if attemptState == nil || attemptState.fingerprint != fingerprint {
		attemptState = &openAICodexTicketProxyAttemptState{
			fingerprint: fingerprint,
			attempted:   make(map[string]struct{}, len(enabled)),
		}
		r.attempts[key] = attemptState
	}

	collect := func(skipAttempted, skipPenalized bool) []OpenAICodexTicketProxy {
		candidates := make([]OpenAICodexTicketProxy, 0, len(enabled))
		for _, proxy := range enabled {
			if skipAttempted {
				if _, attempted := attemptState.attempted[proxy.ID]; attempted {
					continue
				}
			}
			if skipPenalized {
				nodeKey := openAICodexTicketProxyNodeKey(fingerprint, proxy.ID)
				if until := r.penalizedTo[nodeKey]; until.After(now) {
					continue
				}
			}
			candidates = append(candidates, proxy)
		}
		return candidates
	}

	candidates := collect(true, true)
	if len(candidates) == 0 {
		// Prefer an untried but temporarily penalized node over immediately
		// repeating the node that just failed for this account/model.
		candidates = collect(true, false)
	}
	if len(candidates) == 0 {
		attemptState.attempted = make(map[string]struct{}, len(enabled))
		candidates = collect(false, true)
	}
	if len(candidates) == 0 {
		candidates = collect(false, false)
	}

	minPriority := candidates[0].Priority
	for _, proxy := range candidates[1:] {
		if proxy.Priority < minPriority {
			minPriority = proxy.Priority
		}
	}
	tier := make([]OpenAICodexTicketProxy, 0, len(candidates))
	for _, proxy := range candidates {
		if proxy.Priority == minPriority {
			tier = append(tier, proxy)
		}
	}

	totalWeight := 0
	selectedIndex := 0
	selectedScore := 0
	for index, proxy := range tier {
		nodeKey := openAICodexTicketProxyNodeKey(fingerprint, proxy.ID)
		r.current[nodeKey] += proxy.Weight
		totalWeight += proxy.Weight
		score := r.current[nodeKey] - r.active[nodeKey]*10000
		if index == 0 || score > selectedScore {
			selectedIndex = index
			selectedScore = score
		}
	}
	selected := tier[selectedIndex]
	nodeKey := openAICodexTicketProxyNodeKey(fingerprint, selected.ID)
	r.current[nodeKey] -= totalWeight
	r.active[nodeKey]++
	attemptState.attempted[selected.ID] = struct{}{}
	r.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.active[nodeKey] <= 1 {
				delete(r.active, nodeKey)
			} else {
				r.active[nodeKey]--
			}
		})
	}
	return openAICodexTicketSelectedProxy{
		proxy:       selected,
		fingerprint: fingerprint,
		release:     release,
	}, true
}

func (r *openAICodexTicketProxyPoolRuntime) selectProxies(key string, settings OpenAICodexTicketRuntimeSettings, now time.Time, limit int) []openAICodexTicketSelectedProxy {
	enabledCount := len(enabledOpenAICodexTicketProxies(settings.ProxyPool))
	if limit > enabledCount {
		limit = enabledCount
	}
	if limit <= 0 {
		return nil
	}

	selected := make([]openAICodexTicketSelectedProxy, 0, limit)
	for len(selected) < limit {
		proxy, ok := r.selectProxy(key, settings, now)
		if !ok {
			break
		}
		selected = append(selected, proxy)
	}
	return selected
}

func (r *openAICodexTicketProxyPoolRuntime) penalize(selection openAICodexTicketSelectedProxy, now time.Time, duration time.Duration) {
	if strings.TrimSpace(selection.proxy.ID) == "" || duration <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureMaps()
	nodeKey := openAICodexTicketProxyNodeKey(selection.fingerprint, selection.proxy.ID)
	until := now.Add(duration)
	if until.After(r.penalizedTo[nodeKey]) {
		r.penalizedTo[nodeKey] = until
	}
}

func (r *openAICodexTicketProxyPoolRuntime) clearAttempts(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.attempts != nil {
		delete(r.attempts, key)
	}
}

func (r *openAICodexTicketProxyPoolRuntime) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.current = nil
	r.active = nil
	r.penalizedTo = nil
	r.attempts = nil
}
