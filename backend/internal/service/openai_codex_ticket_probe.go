package service

import (
	"bufio"
	"io"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

const (
	openAICodexTicketProbeMaxStreamBytes = 1 << 20
	openAICodexTicketProbeMaxLineBytes   = 512 << 10
)

type openAICodexTicketProbeVerdict string

const (
	openAICodexTicketProbeVerified        openAICodexTicketProbeVerdict = "verified"
	openAICodexTicketProbeLengthMismatch  openAICodexTicketProbeVerdict = "length_mismatch"
	openAICodexTicketProbeInvalidState    openAICodexTicketProbeVerdict = "invalid_state"
	openAICodexTicketProbeMissingState    openAICodexTicketProbeVerdict = "missing_state"
	openAICodexTicketProbeModelMismatch   openAICodexTicketProbeVerdict = "model_mismatch"
	openAICodexTicketProbeModelUnverified openAICodexTicketProbeVerdict = "model_unverified"
	openAICodexTicketProbeMissingCookie   openAICodexTicketProbeVerdict = "missing_cookie"
	openAICodexTicketProbeUpstreamFailed  openAICodexTicketProbeVerdict = "upstream_failed"
	openAICodexTicketProbeOverloaded      openAICodexTicketProbeVerdict = "upstream_overloaded"
	openAICodexTicketProbeIncomplete      openAICodexTicketProbeVerdict = "stream_incomplete"
	openAICodexTicketProbeHTTPError       openAICodexTicketProbeVerdict = "http_error"
	openAICodexTicketProbeTransportError  openAICodexTicketProbeVerdict = "transport_error"
	openAICodexTicketProbeTokenError      openAICodexTicketProbeVerdict = "token_error"
)

type openAICodexTicketProbeResult struct {
	State         string
	Status        int
	Verdict       openAICodexTicketProbeVerdict
	ServedModel   string
	RouteCookies  map[string]string
	CookieUpdated bool
}

func (r openAICodexTicketProbeResult) verified() bool {
	return r.Verdict == openAICodexTicketProbeVerified
}

// inspectOpenAICodexTicketProbeResponse performs cheap header checks first.
// Only a candidate target-length ticket is allowed to read the SSE body, so
// obvious misses do not consume additional residential-proxy traffic.
func inspectOpenAICodexTicketProbeResponse(resp *http.Response, requestedModel string, targetLength int) (openAICodexTicketProbeResult, error) {
	result := openAICodexTicketProbeResult{Verdict: openAICodexTicketProbeIncomplete}
	if resp == nil {
		return result, nil
	}
	result.Status = resp.StatusCode
	result.State = extractOpenAICodexTurnState(resp.Header)
	result.RouteCookies = extractOpenAICodexRouteCookieValues(resp)
	result.CookieUpdated = len(result.RouteCookies) > 0

	switch {
	case resp.StatusCode != http.StatusOK:
		result.Verdict = openAICodexTicketProbeHTTPError
		return result, nil
	case result.State == "":
		result.Verdict = openAICodexTicketProbeMissingState
		return result, nil
	case strings.ContainsAny(result.State, "\r\n\x00") || !strings.HasPrefix(result.State, openAICodexTicketStatePrefix):
		result.Verdict = openAICodexTicketProbeInvalidState
		return result, nil
	case len(result.State) != targetLength:
		result.Verdict = openAICodexTicketProbeLengthMismatch
		return result, nil
	case resp.Body == nil:
		return result, nil
	}

	verdict, servedModel, err := inspectOpenAICodexTicketProbeStream(resp.Body, requestedModel)
	if verdict == openAICodexTicketProbeVerified && !hasOpenAICodexRoutingCookie(result.RouteCookies) {
		verdict = openAICodexTicketProbeMissingCookie
	}
	result.Verdict = verdict
	result.ServedModel = safeOpenAICodexTicketObservedModel(servedModel)
	return result, err
}

func inspectOpenAICodexTicketProbeStream(body io.Reader, requestedModel string) (openAICodexTicketProbeVerdict, string, error) {
	observer := &upstreamResponseModelObserver{}
	var parser openAICompatSSEFrameParser
	scanner := bufio.NewScanner(io.LimitReader(body, openAICodexTicketProbeMaxStreamBytes))
	scanner.Buffer(make([]byte, 0, 8<<10), openAICodexTicketProbeMaxLineBytes)

	inspectFrame := func(frame openAICompatSSEFrame, ok bool) (openAICodexTicketProbeVerdict, bool) {
		if !ok {
			return "", false
		}
		payload := []byte(strings.TrimSpace(frame.Data))
		if len(payload) == 0 || string(payload) == "[DONE]" || !gjson.ValidBytes(payload) {
			return "", false
		}
		eventType := effectiveOpenAISSEEventType(payload, frame.EventType)
		observer.ObserveOpenAI(payload, eventType)

		switch eventType {
		case "response.output_text.delta":
			return openAICodexTicketProbeModelVerdict(observer, requestedModel), true
		case "response.completed", "response.done":
			if openAICodexTicketProbePayloadFailed(payload) {
				return openAICodexTicketProbeFailureVerdict(payload), true
			}
			return openAICodexTicketProbeModelVerdict(observer, requestedModel), true
		case "response.failed", "response.incomplete", "response.cancelled", "response.canceled", "error":
			return openAICodexTicketProbeFailureVerdict(payload), true
		default:
			return "", false
		}
	}

	for scanner.Scan() {
		frame, ok := parser.AddLine(strings.TrimRight(scanner.Text(), "\r"))
		if verdict, decisive := inspectFrame(frame, ok); decisive {
			return verdict, observer.Model(), nil
		}
	}
	if err := scanner.Err(); err != nil {
		return openAICodexTicketProbeTransportError, observer.Model(), err
	}
	if verdict, decisive := inspectFrame(parser.Finish()); decisive {
		return verdict, observer.Model(), nil
	}
	return openAICodexTicketProbeIncomplete, observer.Model(), nil
}

func openAICodexTicketProbeModelVerdict(observer *upstreamResponseModelObserver, requestedModel string) openAICodexTicketProbeVerdict {
	servedModel := strings.TrimSpace(observer.Model())
	if servedModel == "" {
		return openAICodexTicketProbeModelUnverified
	}
	if observer.Conflict() || !strings.EqualFold(strings.TrimSpace(requestedModel), servedModel) {
		return openAICodexTicketProbeModelMismatch
	}
	return openAICodexTicketProbeVerified
}

func openAICodexTicketProbePayloadFailed(payload []byte) bool {
	status := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, "response.status").String()))
	switch status {
	case "failed", "incomplete", "cancelled", "canceled":
		return true
	}
	errorValue := gjson.GetBytes(payload, "response.error")
	return errorValue.Exists() && errorValue.Type != gjson.Null
}

func openAICodexTicketProbeFailureVerdict(payload []byte) openAICodexTicketProbeVerdict {
	code := firstValidTrimmedGJSONString(payload, "error.code", "response.error.code")
	message := firstValidTrimmedGJSONString(payload, "error.message", "response.error.message")
	combined := strings.ToLower(code + " " + message)
	if strings.Contains(combined, "overload") || strings.EqualFold(code, "slow_down") {
		return openAICodexTicketProbeOverloaded
	}
	return openAICodexTicketProbeUpstreamFailed
}

func safeOpenAICodexTicketObservedModel(model string) string {
	model = strings.TrimSpace(model)
	if model == "" || len(model) > 120 {
		return ""
	}
	for i := 0; i < len(model); i++ {
		char := model[i]
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || strings.ContainsRune("-_.:/", rune(char)) {
			continue
		}
		return ""
	}
	return model
}
