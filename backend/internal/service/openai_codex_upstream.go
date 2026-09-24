package service

import (
	"context"
	"net/http"
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
