package service

import (
	"regexp"
	"strings"
)

var openAITimezoneTagPattern = regexp.MustCompile(`(?is)<timezone>[^<]*</timezone>`)

// normalizeOpenAITextRequestTimezone aligns explicit client timezone metadata
// with the configured egress timezone. Image requests are left untouched.
func normalizeOpenAITextRequestTimezone(body []byte, timezoneName string, imageIntent bool) ([]byte, bool, error) {
	if imageIntent || len(body) == 0 {
		return body, false, nil
	}
	timezoneName = strings.TrimSpace(timezoneName)
	if timezoneName == "" || timezoneName == "Local" {
		return body, false, nil
	}

	var payload map[string]any
	if err := decodeOpenAIJSONUseNumber(body, &payload); err != nil {
		return body, false, err
	}

	changed := normalizeOpenAITimezonePayload(payload, timezoneName)
	if !changed {
		return body, false, nil
	}
	normalized, err := marshalOpenAIUpstreamJSON(payload)
	if err != nil {
		return body, false, err
	}
	return normalized, true, nil
}

func normalizeOpenAITimezonePayload(payload map[string]any, timezoneName string) bool {
	if len(payload) == 0 {
		return false
	}
	changed := normalizeOpenAITimezoneStringField(payload, "instructions", timezoneName)
	changed = normalizeOpenAITimezoneTextValue(payload, "input", timezoneName) || changed
	changed = normalizeOpenAITimezoneTextValue(payload, "messages", timezoneName) || changed
	changed = normalizeOpenAITimezoneMetadata(payload["client_metadata"], timezoneName) || changed

	if session, ok := payload["session"].(map[string]any); ok {
		changed = normalizeOpenAITimezoneStringField(session, "instructions", timezoneName) || changed
		changed = normalizeOpenAITimezoneTextValue(session, "input", timezoneName) || changed
		changed = normalizeOpenAITimezoneTextValue(session, "messages", timezoneName) || changed
		changed = normalizeOpenAITimezoneMetadata(session["client_metadata"], timezoneName) || changed
	}
	return changed
}

func normalizeOpenAITimezoneTextValue(parent map[string]any, key, timezoneName string) bool {
	value, exists := parent[key]
	if !exists {
		return false
	}
	normalized, changed := normalizeOpenAITimezoneTextContainer(value, timezoneName)
	if changed {
		parent[key] = normalized
	}
	return changed
}

func normalizeOpenAITimezoneTextContainer(value any, timezoneName string) (any, bool) {
	switch typed := value.(type) {
	case string:
		return replaceOpenAITimezoneTags(typed, timezoneName)
	case []any:
		changed := false
		for i, item := range typed {
			normalized, itemChanged := normalizeOpenAITimezoneTextContainer(item, timezoneName)
			if itemChanged {
				typed[i] = normalized
				changed = true
			}
		}
		return typed, changed
	case map[string]any:
		changed := normalizeOpenAITimezoneStringField(typed, "instructions", timezoneName)
		changed = normalizeOpenAITimezoneStringField(typed, "text", timezoneName) || changed
		changed = normalizeOpenAITimezoneTextValue(typed, "content", timezoneName) || changed
		return typed, changed
	default:
		return value, false
	}
}

func normalizeOpenAITimezoneStringField(parent map[string]any, key, timezoneName string) bool {
	value, ok := parent[key].(string)
	if !ok {
		return false
	}
	normalized, changed := replaceOpenAITimezoneTags(value, timezoneName)
	if changed {
		parent[key] = normalized
	}
	return changed
}

func replaceOpenAITimezoneTags(value, timezoneName string) (string, bool) {
	replacement := "<timezone>" + timezoneName + "</timezone>"
	normalized := openAITimezoneTagPattern.ReplaceAllString(value, replacement)
	return normalized, normalized != value
}

func normalizeOpenAITimezoneMetadata(value any, timezoneName string) bool {
	metadata, ok := value.(map[string]any)
	if !ok {
		return false
	}
	changed := false
	for _, key := range []string{"timezone", "time_zone"} {
		current, exists := metadata[key].(string)
		if exists && strings.TrimSpace(current) != timezoneName {
			metadata[key] = timezoneName
			changed = true
		}
	}
	return changed
}
