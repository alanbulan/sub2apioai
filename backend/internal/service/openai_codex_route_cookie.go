package service

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
)

const openAICodexRouteCookieExtraKey = openAICodexTicketExtraKeyPrefix + "route_cookie"

var openAICodexRouteCookieNames = map[string]struct{}{
	"__cf_bm": {},
	"__cflb":  {},
	"__oailb": {},
}

type openAICodexRouteCookie struct {
	AccountID  int64             `json:"account_id"`
	Values     map[string]string `json:"values"`
	CapturedAt time.Time         `json:"captured_at"`
	ExpiresAt  time.Time         `json:"expires_at"`
}

func extractOpenAICodexRouteCookieValues(resp *http.Response) map[string]string {
	if resp == nil {
		return nil
	}
	values := make(map[string]string)
	for _, cookie := range resp.Cookies() {
		if cookie == nil {
			continue
		}
		name := strings.TrimSpace(cookie.Name)
		if _, ok := openAICodexRouteCookieNames[name]; !ok || cookie.Value == "" {
			continue
		}
		serialized := (&http.Cookie{Name: name, Value: cookie.Value}).String()
		if serialized == "" || strings.ContainsAny(serialized, "\r\n") {
			continue
		}
		values[name] = cookie.Value
	}
	if len(values) == 0 {
		return nil
	}
	return values
}

func hasOpenAICodexRoutingCookie(values map[string]string) bool {
	return strings.TrimSpace(values["__cflb"]) != "" || strings.TrimSpace(values["__oailb"]) != ""
}

func (c *openAICodexRouteCookie) matches() bool {
	if c == nil || !hasOpenAICodexRoutingCookie(c.Values) {
		return false
	}
	for name, value := range c.Values {
		if _, ok := openAICodexRouteCookieNames[name]; !ok || value == "" {
			return false
		}
		if (&http.Cookie{Name: name, Value: value}).String() == "" {
			return false
		}
	}
	return true
}

func openAICodexEffectiveExpiry(capturedAt, expiresAt time.Time, ttl time.Duration) time.Time {
	if capturedAt.IsZero() || ttl <= 0 {
		return expiresAt
	}
	policyExpiry := capturedAt.Add(ttl)
	if expiresAt.IsZero() || policyExpiry.Before(expiresAt) {
		return policyExpiry
	}
	return expiresAt
}

func (c *openAICodexRouteCookie) effectiveExpiresAt(ttl time.Duration) time.Time {
	if c == nil {
		return time.Time{}
	}
	return openAICodexEffectiveExpiry(c.CapturedAt, c.ExpiresAt, ttl)
}

func (c *openAICodexRouteCookie) valid(now time.Time, ttl time.Duration) bool {
	if !c.matches() {
		return false
	}
	expiresAt := c.effectiveExpiresAt(ttl)
	return !expiresAt.IsZero() && now.Before(expiresAt)
}

func (c *openAICodexRouteCookie) needsRefresh(now time.Time, ttl, refreshBefore time.Duration) bool {
	if !c.valid(now, ttl) {
		return true
	}
	return !c.effectiveExpiresAt(ttl).After(now.Add(refreshBefore))
}

func parseOpenAICodexRouteCookieFromAny(accountID int64, raw any) *openAICodexRouteCookie {
	if raw == nil {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var cookie openAICodexRouteCookie
	if err := json.Unmarshal(b, &cookie); err != nil {
		return nil
	}
	cookie.AccountID = accountID
	if !cookie.matches() {
		return nil
	}
	return &cookie
}

func (s *OpenAIGatewayService) lookupOpenAICodexRouteCookie(account *Account) *openAICodexRouteCookie {
	if s == nil || account == nil || account.ID <= 0 {
		return nil
	}
	var memory *openAICodexRouteCookie
	if raw, ok := s.openaiCodexRouteCookies.Load(account.ID); ok {
		memory, _ = raw.(*openAICodexRouteCookie)
	}
	var persisted *openAICodexRouteCookie
	if account.Extra != nil {
		persisted = parseOpenAICodexRouteCookieFromAny(account.ID, account.Extra[openAICodexRouteCookieExtraKey])
	}
	if persisted != nil && (memory == nil || persisted.CapturedAt.After(memory.CapturedAt)) {
		s.openaiCodexRouteCookies.Store(account.ID, persisted)
		return persisted
	}
	if memory != nil {
		return memory
	}
	return nil
}

func applyOpenAICodexRouteCookieHeader(h http.Header, routeCookie *openAICodexRouteCookie) {
	if h == nil || routeCookie == nil || !routeCookie.matches() {
		return
	}
	values := make(map[string]string)
	request := &http.Request{Header: h}
	for _, cookie := range request.Cookies() {
		if cookie != nil && cookie.Name != "" {
			values[cookie.Name] = cookie.Value
		}
	}
	for name, value := range routeCookie.Values {
		values[name] = value
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		if serialized := (&http.Cookie{Name: name, Value: values[name]}).String(); serialized != "" {
			parts = append(parts, serialized)
		}
	}
	if len(parts) > 0 {
		h.Set("Cookie", strings.Join(parts, "; "))
	}
}

func newOpenAICodexRouteCookie(accountID int64, values map[string]string, now time.Time, ttl time.Duration) *openAICodexRouteCookie {
	if accountID <= 0 || !hasOpenAICodexRoutingCookie(values) || ttl <= 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for name, value := range values {
		cloned[name] = value
	}
	return &openAICodexRouteCookie{
		AccountID:  accountID,
		Values:     cloned,
		CapturedAt: now,
		ExpiresAt:  now.Add(ttl),
	}
}
