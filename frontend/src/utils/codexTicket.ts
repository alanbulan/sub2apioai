import type { OpenAICodexTurnTicketStatus } from '@/types'

export interface CodexTicketPauseMessage {
  key: string
  params?: Record<string, string | number>
}

export function codexTicketPauseMessage(ticket: OpenAICodexTurnTicketStatus): CodexTicketPauseMessage {
  switch (ticket.last_probe_reason) {
    case 'length_mismatch':
      return {
        key: 'admin.accounts.openai.codexTurnTicketPausedLength',
        params: { length: ticket.last_probe_state_length ?? 0 }
      }
    case 'model_mismatch':
      return ticket.last_probe_served_model
        ? {
            key: 'admin.accounts.openai.codexTurnTicketPausedModel',
            params: { model: ticket.last_probe_served_model }
          }
        : { key: 'admin.accounts.openai.codexTurnTicketPausedModelUnknown' }
    case 'model_unverified':
      return { key: 'admin.accounts.openai.codexTurnTicketPausedModelUnknown' }
    case 'upstream_overloaded':
      return { key: 'admin.accounts.openai.codexTurnTicketPausedOverloaded' }
    case 'http_error':
      return {
        key: 'admin.accounts.openai.codexTurnTicketPausedHttp',
        params: { status: ticket.last_probe_http_status ?? 0 }
      }
    case 'transport_error':
      return { key: 'admin.accounts.openai.codexTurnTicketPausedTransport' }
    case 'token_error':
      return { key: 'admin.accounts.openai.codexTurnTicketPausedCredential' }
    case 'stream_incomplete':
    case 'upstream_failed':
      return { key: 'admin.accounts.openai.codexTurnTicketPausedIncomplete' }
    case 'missing_state':
    case 'invalid_state':
      return { key: 'admin.accounts.openai.codexTurnTicketPausedInvalid' }
    default:
      return { key: 'admin.accounts.openai.codexTurnTicketPaused' }
  }
}
