package service

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/config"
)

func (s *OpenAIGatewayService) openAICodexResponsesURL() string {
	if s == nil || s.settingService == nil {
		return chatgptCodexURL
	}
	fallbackBaseURL := config.DefaultOpenAICodexBaseURL
	if s.cfg != nil && strings.TrimSpace(s.cfg.Gateway.OpenAICodexBaseURL) != "" {
		fallbackBaseURL = s.cfg.Gateway.OpenAICodexBaseURL
	}
	settings := s.settingService.GetOpenAICodexTextRelaySettings(context.Background(), fallbackBaseURL)
	if !settings.Enabled {
		return chatgptCodexURL
	}
	return strings.TrimRight(settings.BaseURL, "/") + "/responses"
}

func (s *AccountTestService) openAICodexTextResponsesURL() string {
	if s != nil && s.openaiGatewayService != nil {
		return s.openaiGatewayService.openAICodexResponsesURL()
	}
	if s == nil || s.settingService == nil {
		return chatgptCodexAPIURL
	}
	fallbackBaseURL := config.DefaultOpenAICodexBaseURL
	if s.cfg != nil && strings.TrimSpace(s.cfg.Gateway.OpenAICodexBaseURL) != "" {
		fallbackBaseURL = s.cfg.Gateway.OpenAICodexBaseURL
	}
	settings := s.settingService.GetOpenAICodexTextRelaySettings(context.Background(), fallbackBaseURL)
	if !settings.Enabled {
		return chatgptCodexAPIURL
	}
	return strings.TrimRight(settings.BaseURL, "/") + "/responses"
}

func openAICodexWebSocketURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("invalid Codex upstream URL: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
		parsed.Scheme = "wss"
	case "http":
		parsed.Scheme = "ws"
	case "wss", "ws":
	default:
		return "", fmt.Errorf("unsupported Codex WebSocket scheme: %s", parsed.Scheme)
	}
	return parsed.String(), nil
}

// setOpenAICodexRequestHost preserves the first-party authority override while
// allowing a configured relay to receive its own virtual-host authority.
func setOpenAICodexRequestHost(req *http.Request) {
	if req == nil || req.URL == nil {
		return
	}
	if strings.EqualFold(req.URL.Hostname(), "chatgpt.com") {
		req.Host = "chatgpt.com"
		return
	}
	req.Host = ""
}
