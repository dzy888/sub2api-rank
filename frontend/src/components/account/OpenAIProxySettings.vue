<template>
  <div class="space-y-3" data-testid="openai-proxy-settings">
    <div class="flex flex-wrap gap-4">
      <label class="flex items-center gap-1.5 text-sm">
        <input :checked="mode === 'single'" :name="modeName" type="radio" value="single"
          data-testid="openai-proxy-mode-single" class="h-4 w-4 border-gray-300 text-primary-600 focus:ring-primary-500 dark:border-dark-500"
          @change="emit('update:mode', 'single')" />
        {{ t('admin.accounts.openai.proxyModeSingle') }}
      </label>
      <NewFeatureBadge :notice-id="NEW_FEATURE_NOTICES.openaiAccountMultiProxy" :user-id="userId" />
      <label class="flex items-center gap-1.5 text-sm">
        <input :checked="mode === 'request'" :name="modeName" type="radio" value="request"
          data-testid="openai-proxy-mode-request" class="h-4 w-4 border-gray-300 text-primary-600 focus:ring-primary-500 dark:border-dark-500"
          @change="emit('update:mode', 'request')" />
        {{ t('admin.accounts.openai.proxyModeRequest') }}
      </label>
    </div>
    <ProxySelector v-if="mode === 'single'" :model-value="singleProxyId" :proxies="proxies" :disabled="loading"
      @update:model-value="emit('update:singleProxyId', $event)" />
    <p v-if="mode === 'request'" class="text-sm text-amber-700 dark:text-amber-400" role="note" data-testid="openai-proxy-risk">
      {{ t('admin.accounts.openai.proxyRequestRisk') }}
    </p>
    <label v-if="mode === 'request'" class="flex items-start gap-2 text-sm" data-testid="openai-proxy-session-affinity-setting">
      <input :checked="sessionAffinity" type="checkbox" data-testid="openai-proxy-session-affinity"
        class="mt-0.5 h-4 w-4 rounded border-gray-300 text-primary-600 focus:ring-primary-500 dark:border-dark-500"
        @change="emit('update:sessionAffinity', ($event.target as HTMLInputElement).checked)" />
      <span>
        <span class="block font-medium text-gray-700 dark:text-gray-300">{{ t('admin.accounts.openai.proxySessionAffinity') }}</span>
        <span class="mt-0.5 block text-xs text-gray-500 dark:text-gray-400">{{ t('admin.accounts.openai.proxySessionAffinityHint') }}</span>
      </span>
    </label>
    <div class="flex items-center justify-between gap-4">
      <div class="min-w-0">
        <label class="input-label mb-0">{{ t('admin.accounts.openai.proxySessionMaxConcurrency') }}</label>
        <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">{{ t('admin.accounts.openai.proxySessionMaxConcurrencyHint') }}</p>
      </div>
      <div class="w-32 flex-shrink-0">
        <input :value="sessionMaxConcurrency" data-testid="openai-proxy-session-max-concurrency"
          type="number" min="0" step="1" class="input" :placeholder="t('admin.accounts.openai.proxySessionMaxConcurrencyOff')"
          @input="emit('update:sessionMaxConcurrency', normalizeLimit(($event.target as HTMLInputElement).value))" />
      </div>
    </div>
    <div class="flex items-center justify-between gap-4">
      <div class="min-w-0">
        <label class="input-label mb-0">{{ t('admin.accounts.openai.proxySessionSlotCount') }}</label>
        <p class="mt-1 text-xs text-gray-500 dark:text-gray-400">{{ t('admin.accounts.openai.proxySessionSlotCountHint') }}</p>
      </div>
      <div class="w-32 flex-shrink-0">
        <input :value="sessionSlotCount" data-testid="openai-proxy-session-slot-count"
          type="number" min="0" step="1" class="input" :placeholder="t('admin.accounts.openai.proxySessionMaxConcurrencyOff')"
          @input="emit('update:sessionSlotCount', normalizeLimit(($event.target as HTMLInputElement).value))" />
      </div>
    </div>
    <p v-if="sessionConcurrencyBudget > 0" class="text-xs text-gray-600 dark:text-gray-400" data-testid="openai-proxy-session-budget">
      {{ t('admin.accounts.openai.proxySessionBudgetPreview', { budget: sessionConcurrencyBudget }) }}
    </p>
    <BPSProxyRoutingSettings v-if="mode === 'request'" mode="request" embedded :proxy-ids="proxyIds" :proxies="proxies"
      :loading="loading" :load-error="loadError" :current-account-id="currentAccountId" :user-id="userId"
      @update:proxy-ids="emit('update:proxyIds', $event)" @retry="emit('retry')" />
  </div>
</template>

<script setup lang="ts">
import { computed, useId } from 'vue'
import { useI18n } from 'vue-i18n'
import type { Proxy } from '@/types'
import ProxySelector from '@/components/common/ProxySelector.vue'
import BPSProxyRoutingSettings from './BPSProxyRoutingSettings.vue'
import NewFeatureBadge from '@/components/common/NewFeatureBadge.vue'
import { NEW_FEATURE_NOTICES } from '@/constants/newFeatureNotices'

const props = defineProps<{
  mode: 'single' | 'request'
  sessionAffinity?: boolean
  sessionMaxConcurrency?: number | null
  sessionSlotCount?: number | null
  singleProxyId: number | null
  proxyIds: number[]
  proxies: Proxy[]
  currentAccountId?: number | null
  userId?: number | null
  loading?: boolean
  loadError?: boolean
}>()
const emit = defineEmits<{
  'update:mode': [value: 'single' | 'request']
  'update:sessionAffinity': [value: boolean]
  'update:sessionMaxConcurrency': [value: number | null]
  'update:sessionSlotCount': [value: number | null]
  'update:singleProxyId': [value: number | null]
  'update:proxyIds': [value: number[]]
  retry: []
}>()
const { t } = useI18n()
// 账号总并发由「会话数 × 单会话上限」推导；两者都填才展示。
const sessionConcurrencyBudget = computed(() => {
  const limit = props.sessionMaxConcurrency ?? 0
  const slots = props.sessionSlotCount ?? 0
  return limit > 0 && slots > 0 ? limit * slots : 0
})
// 0 / 空表示不限；后端把非正数当不限，这里统一收敛成 null 便于回显。
const normalizeLimit = (raw: string): number | null => {
  const value = Number.parseInt(raw, 10)
  return Number.isFinite(value) && value > 0 ? value : null
}
const modeName = useId()
</script>
