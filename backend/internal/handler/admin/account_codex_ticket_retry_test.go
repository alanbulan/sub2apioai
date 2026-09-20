package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type codexTicketGatewayStub struct {
	model   string
	err     error
	retryAt time.Time
}

func (s *codexTicketGatewayStub) OpenAICodexTicketStatuses(_ *service.Account, _ config.OpenAICodexTicketConfig, _ time.Time) []service.OpenAICodexTicketStatus {
	return []service.OpenAICodexTicketStatus{{
		Model:              "gpt-6-astra",
		Blocked:            true,
		Probing:            s.err == nil,
		ManualRetryAllowed: false,
	}}
}

func (s *codexTicketGatewayStub) RequestOpenAICodexTicketRetry(_ context.Context, _ *service.Account, model string) (time.Time, error) {
	s.model = model
	return s.retryAt, s.err
}

func TestRetryCodexTurnTicketStartsProbe(t *testing.T) {
	gin.SetMode(gin.TestMode)
	adminService := newStubAdminService()
	adminService.getAccountResult = &service.Account{
		ID:          41,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeOAuth,
		Status:      service.StatusActive,
		Schedulable: true,
	}
	gateway := &codexTicketGatewayStub{}
	h := &AccountHandler{
		adminService:         adminService,
		openaiGatewayService: gateway,
		cfg: &config.Config{Gateway: config.GatewayConfig{OpenAICodexTicket: config.OpenAICodexTicketConfig{
			Enabled: true,
		}}},
	}
	router := gin.New()
	router.POST("/accounts/:id/codex-turn-ticket/retry", h.RetryCodexTurnTicket)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/accounts/41/codex-turn-ticket/retry", strings.NewReader(`{"model":"gpt-6-astra"}`))
	request.Header.Set("Content-Type", "application/json")

	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	require.Equal(t, "gpt-6-astra", gateway.model)
	require.Contains(t, recorder.Body.String(), `"probing":true`)
}

func TestRetryCodexTurnTicketReturnsRetryAfterDuringCooldown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	adminService := newStubAdminService()
	adminService.getAccountResult = &service.Account{ID: 41}
	gateway := &codexTicketGatewayStub{
		err:     service.ErrOpenAICodexTicketRetryCooldown,
		retryAt: time.Now().Add(30 * time.Second),
	}
	h := &AccountHandler{adminService: adminService, openaiGatewayService: gateway}
	router := gin.New()
	router.POST("/accounts/:id/codex-turn-ticket/retry", h.RetryCodexTurnTicket)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/accounts/41/codex-turn-ticket/retry", strings.NewReader(`{"model":"gpt-6-astra"}`))
	request.Header.Set("Content-Type", "application/json")

	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusTooManyRequests, recorder.Code)
	require.NotEmpty(t, recorder.Header().Get("Retry-After"))
	require.ErrorIs(t, gateway.err, service.ErrOpenAICodexTicketRetryCooldown)
}
