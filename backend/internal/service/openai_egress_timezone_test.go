package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeOpenAITextRequestTimezone(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		imageIntent bool
		wantChanged bool
		wantContain []string
	}{
		{
			name:        "top-level instructions",
			body:        `{"model":"gpt-5.6","instructions":"<environment_context><timezone>Etc/UTC</timezone></environment_context>"}`,
			wantChanged: true,
			wantContain: []string{`<timezone>Asia/Singapore</timezone>`},
		},
		{
			name:        "responses input content",
			body:        `{"input":[{"role":"user","content":[{"type":"input_text","text":"before <timezone>Asia/Shanghai</timezone> after"}]}]}`,
			wantChanged: true,
			wantContain: []string{`before <timezone>Asia/Singapore</timezone> after`},
		},
		{
			name:        "websocket session update and metadata",
			body:        `{"type":"session.update","session":{"instructions":"<timezone>UTC</timezone>","client_metadata":{"timezone":"UTC","time_zone":"Europe/London"}}}`,
			wantChanged: true,
			wantContain: []string{`<timezone>Asia/Singapore</timezone>`, `"timezone":"Asia/Singapore"`, `"time_zone":"Asia/Singapore"`},
		},
		{
			name:        "ordinary timezone wording is untouched",
			body:        `{"input":"Tell me what the timezone is."}`,
			wantChanged: false,
		},
		{
			name:        "image request is untouched",
			body:        `{"instructions":"<timezone>UTC</timezone>"}`,
			imageIntent: true,
			wantChanged: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := []byte(tt.body)
			got, changed, err := normalizeOpenAITextRequestTimezone(original, "Asia/Singapore", tt.imageIntent)
			require.NoError(t, err)
			require.Equal(t, tt.wantChanged, changed)
			if !tt.wantChanged {
				require.Equal(t, original, got)
			}
			for _, expected := range tt.wantContain {
				require.Contains(t, string(got), expected)
			}
		})
	}
}

func TestNormalizeOpenAITextRequestTimezoneRejectsInvalidJSON(t *testing.T) {
	body := []byte(`{"input":`)
	got, changed, err := normalizeOpenAITextRequestTimezone(body, "Asia/Singapore", false)
	require.Error(t, err)
	require.False(t, changed)
	require.Equal(t, body, got)
}
