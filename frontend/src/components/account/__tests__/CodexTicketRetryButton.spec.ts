import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import CodexTicketRetryButton from '../CodexTicketRetryButton.vue'
import type { Account, OpenAICodexTurnTicketStatus } from '@/types'

const { getById, retryCodexTurnTicket, showError, showSuccess } = vi.hoisted(() => ({
  getById: vi.fn(),
  retryCodexTurnTicket: vi.fn(),
  showError: vi.fn(),
  showSuccess: vi.fn()
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    accounts: {
      getById,
      retryCodexTurnTicket
    }
  }
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({ showError, showSuccess })
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string, params?: Record<string, unknown>) => `${key}${params?.time ? `:${params.time}` : ''}`
    })
  }
})

const account = {
  id: 41,
  name: 'OpenAI OAuth',
  platform: 'openai',
  type: 'oauth',
  status: 'active'
} as Account

function ticket(overrides: Partial<OpenAICodexTurnTicketStatus> = {}): OpenAICodexTurnTicketStatus {
  return {
    model: 'gpt-6-astra',
    ready: false,
    remaining_seconds: 0,
    blocked: true,
    probing: false,
    manual_retry_allowed: true,
    ...overrides
  }
}

function mountButton(value = ticket()) {
  return mount(CodexTicketRetryButton, {
    props: { account, ticket: value },
    global: {
      stubs: {
        Icon: { template: '<span data-testid="icon" />' }
      }
    }
  })
}

describe('CodexTicketRetryButton', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    getById.mockReset()
    retryCodexTurnTicket.mockReset()
    showError.mockReset()
    showSuccess.mockReset()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('starts a retry and polls until the ticket becomes ready', async () => {
    const probingAccount = {
      ...account,
      codex_turn_tickets: [ticket({ probing: true, manual_retry_allowed: false })]
    }
    const readyAccount = {
      ...account,
      codex_turn_tickets: [ticket({ ready: true, blocked: false, probing: false, remaining_seconds: 3599 })]
    }
    retryCodexTurnTicket.mockResolvedValue(probingAccount)
    getById.mockResolvedValue(readyAccount)
    const wrapper = mountButton()

    await wrapper.get('[data-testid="codex-ticket-retry"]').trigger('click')
    await flushPromises()

    expect(retryCodexTurnTicket).toHaveBeenCalledWith(41, 'gpt-6-astra')
    expect(showSuccess).toHaveBeenCalledWith('admin.accounts.openai.codexTurnTicketRetryStarted')
    expect(wrapper.get('button').attributes('disabled')).toBeDefined()

    await vi.advanceTimersByTimeAsync(1000)
    await flushPromises()

    expect(getById).toHaveBeenCalledWith(41)
    expect(wrapper.emitted('account-updated')?.map(([value]) => value)).toEqual([
      probingAccount,
      readyAccount
    ])
    wrapper.unmount()
  })

  it('disables retries while probing or cooling down', async () => {
    const probing = mountButton(ticket({ probing: true }))
    expect(probing.get('button').attributes('disabled')).toBeDefined()
    probing.unmount()

    const coolingDown = mountButton(ticket({
      manual_retry_allowed: false,
      manual_retry_in_seconds: 2
    }))
    expect(coolingDown.get('button').attributes('disabled')).toBeDefined()
    expect(coolingDown.get('button').attributes('title')).toContain('2s')
    await coolingDown.get('button').trigger('click')
    expect(retryCodexTurnTicket).not.toHaveBeenCalled()

    await vi.advanceTimersByTimeAsync(2000)
    expect(coolingDown.get('button').attributes('disabled')).toBeUndefined()
    coolingDown.unmount()
  })

  it('surfaces retry failures without starting the poll loop', async () => {
    retryCodexTurnTicket.mockRejectedValue(new Error('cooldown'))
    const wrapper = mountButton()

    await wrapper.get('button').trigger('click')
    await flushPromises()

    expect(showError).toHaveBeenCalledWith('cooldown')
    expect(wrapper.text()).toContain('cooldown')
    expect(getById).not.toHaveBeenCalled()
    wrapper.unmount()
  })
})
