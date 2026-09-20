<template>
  <div class="inline-flex min-w-0 flex-col items-end">
    <button
      type="button"
      :class="buttonClass"
      :disabled="disabled"
      :title="buttonTitle"
      :aria-label="buttonTitle"
      :data-model="ticket.model"
      data-testid="codex-ticket-retry"
      @click.stop="retryTicket"
    >
      <Icon
        name="refresh"
        size="xs"
        :class="{ 'animate-spin': isProbing }"
        :stroke-width="2"
      />
      <span v-if="!compact">{{ buttonLabel }}</span>
    </button>
    <span
      v-if="!compact && lastError"
      class="mt-1 max-w-64 text-right text-xs text-red-600 dark:text-red-400"
    >
      {{ lastError }}
    </span>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { adminAPI } from '@/api/admin'
import { useAppStore } from '@/stores/app'
import type { Account, OpenAICodexTurnTicketStatus } from '@/types'
import Icon from '@/components/icons/Icon.vue'

const props = withDefaults(defineProps<{
  account: Account
  ticket: OpenAICodexTurnTicketStatus
  compact?: boolean
}>(), {
  compact: false
})

const emit = defineEmits<{
  'account-updated': [account: Account]
}>()

const { t } = useI18n()
const appStore = useAppStore()
const busy = ref(false)
const lastError = ref('')
const manualCooldown = ref(0)
const localCooldownFinished = ref(false)
let pollGeneration = 0
let cooldownTimer: number | undefined

const isProbing = computed(() => busy.value || props.ticket.probing === true)
const disabled = computed(() => (
  isProbing.value ||
  manualCooldown.value > 0 ||
  (props.ticket.manual_retry_allowed === false && !localCooldownFinished.value)
))
const buttonLabel = computed(() => isProbing.value
  ? t('admin.accounts.openai.codexTurnTicketRetrying')
  : t('admin.accounts.openai.codexTurnTicketRetryNow'))
const buttonTitle = computed(() => {
  if (lastError.value) return lastError.value
  if (isProbing.value) return t('admin.accounts.openai.codexTurnTicketRetrying')
  if (manualCooldown.value > 0) {
    return t('admin.accounts.openai.codexTurnTicketManualCooldown', {
      time: formatRetryDelay(manualCooldown.value)
    })
  }
  if ((props.ticket.retry_in_seconds ?? 0) > 0) {
    return `${t('admin.accounts.openai.codexTurnTicketRetryNow')} - ${t('admin.accounts.openai.codexTurnTicketAutoRetryIn', {
      time: formatRetryDelay(props.ticket.retry_in_seconds ?? 0)
    })}`
  }
  return t('admin.accounts.openai.codexTurnTicketRetryNow')
})
const buttonClass = computed(() => props.compact
  ? 'inline-flex h-5 w-5 flex-none items-center justify-center rounded text-amber-600 transition-colors hover:bg-amber-50 hover:text-amber-700 disabled:cursor-not-allowed disabled:opacity-40 dark:text-amber-400 dark:hover:bg-amber-900/30'
  : 'inline-flex h-8 flex-none items-center gap-1.5 rounded border border-amber-300 px-2.5 text-xs font-medium text-amber-700 transition-colors hover:bg-amber-50 disabled:cursor-not-allowed disabled:opacity-50 dark:border-amber-700 dark:text-amber-300 dark:hover:bg-amber-900/30')

function stopCooldownTimer() {
  if (cooldownTimer === undefined) return
  window.clearInterval(cooldownTimer)
  cooldownTimer = undefined
}

function startCooldownTimer() {
  stopCooldownTimer()
  if (manualCooldown.value <= 0) return
  cooldownTimer = window.setInterval(() => {
    manualCooldown.value = Math.max(0, manualCooldown.value - 1)
    if (manualCooldown.value === 0) {
      localCooldownFinished.value = true
      stopCooldownTimer()
    }
  }, 1000)
}

watch(
  () => [props.ticket.manual_retry_in_seconds ?? 0, props.ticket.manual_retry_allowed] as const,
  ([seconds, allowed]) => {
    manualCooldown.value = Math.max(0, Math.ceil(seconds || 0))
    localCooldownFinished.value = manualCooldown.value === 0 && allowed === true
    startCooldownTimer()
  },
  { immediate: true }
)

function formatRetryDelay(seconds: number) {
  const total = Math.max(0, Math.ceil(seconds || 0))
  if (total < 60) return `${total}s`
  const minutes = Math.ceil(total / 60)
  return `${minutes}m`
}

function errorMessage(error: unknown) {
  if (error && typeof error === 'object' && 'message' in error) {
    const message = (error as { message?: unknown }).message
    if (typeof message === 'string' && message.trim()) return message
  }
  return t('admin.accounts.openai.codexTurnTicketRetryFailed')
}

const wait = (milliseconds: number) => new Promise(resolve => window.setTimeout(resolve, milliseconds))

async function pollTicket(generation: number) {
  for (let attempt = 0; attempt < 35 && generation === pollGeneration; attempt += 1) {
    await wait(1000)
    if (generation !== pollGeneration) return
    const account = await adminAPI.accounts.getById(props.account.id)
    if (generation !== pollGeneration) return
    emit('account-updated', account)
    const ticket = account.codex_turn_tickets?.find(item => item.model === props.ticket.model)
    if (!ticket || ticket.ready || ticket.probing !== true) return
  }
}

async function retryTicket() {
  if (disabled.value) return
  const generation = ++pollGeneration
  busy.value = true
  lastError.value = ''
  try {
    const account = await adminAPI.accounts.retryCodexTurnTicket(props.account.id, props.ticket.model)
    emit('account-updated', account)
    appStore.showSuccess(t('admin.accounts.openai.codexTurnTicketRetryStarted'))
    await pollTicket(generation)
  } catch (error) {
    if (generation !== pollGeneration) return
    lastError.value = errorMessage(error)
    appStore.showError(lastError.value)
  } finally {
    if (generation === pollGeneration) busy.value = false
  }
}

onBeforeUnmount(() => {
  pollGeneration += 1
  stopCooldownTimer()
})
</script>
