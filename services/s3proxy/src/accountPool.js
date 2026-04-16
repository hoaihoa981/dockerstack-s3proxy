/**
 * src/accountPool.js
 * Account pool management: selection, quota tracking, and in-memory state.
 */

import {
  getAllAccounts,
  getAccountById,
  upsertAccount,
  deactivateMissingAccounts,
} from './db.js'
import { rtdbGet, rtdbPatch } from './firebase.js'
import { metrics } from './routes/metrics.js'
import config from './config.js'
import {
  buildRtdbAccountPath,
  resolveAccountIdFromRtdbEntry,
} from './accountId.js'
import { normalizeSupabaseAccessTokenExp } from './supabaseS3.js'

export class StorageFullError extends Error {
  constructor(message = 'All storage accounts are at capacity') {
    super(message)
    this.name = 'StorageFullError'
    this.statusCode = 507
  }
}

const accountMap = new Map()
let activeAccounts = []

function refreshMetrics() {
  for (const account of accountMap.values()) {
    metrics.accountUsedBytes.set({ account_id: account.account_id }, Math.max(0, account.used_bytes ?? 0))
    metrics.accountQuotaBytes.set({ account_id: account.account_id }, Math.max(0, account.quota_bytes ?? 0))
  }
}

function rebuildActiveAccounts() {
  activeAccounts = [...accountMap.values()]
    .filter((account) => account.active === 1 || account.active === true)
    .sort((left, right) => left.used_bytes - right.used_bytes || left.account_id.localeCompare(right.account_id))

  refreshMetrics()
}

function loadFromSQLite() {
  const rows = getAllAccounts()
  accountMap.clear()

  for (const row of rows) {
    accountMap.set(row.account_id, { ...row })
  }

  rebuildActiveAccounts()
}

loadFromSQLite()

export function syncAccountsFromRows(rows = []) {
  for (const row of rows) {
    if (!row?.account_id) continue
    accountMap.set(row.account_id, { ...row })
  }

  rebuildActiveAccounts()
}

export function syncAccountFromDb(accountId) {
  const row = getAccountById(accountId)
  if (!row) {
    accountMap.delete(accountId)
    rebuildActiveAccounts()
    return null
  }

  accountMap.set(accountId, { ...row })
  rebuildActiveAccounts()
  return row
}

function isPublicBucketAccount(account) {
  return account?.public_bucket === 1 || account?.public_bucket === true
}

export function selectAccountForUpload(sizeBytes, options = {}) {
  const excludeIds = options.excludeIds instanceof Set
    ? options.excludeIds
    : new Set(options.excludeIds ?? [])
  const publicBucket = options.publicBucket === true

  for (const account of activeAccounts) {
    if (excludeIds.has(account.account_id)) continue
    if (publicBucket) {
      if (!isPublicBucketAccount(account)) continue
    } else if (isPublicBucketAccount(account)) {
      continue
    }

    const projected = (account.used_bytes + sizeBytes) / account.quota_bytes
    if (projected < config.QUOTA_THRESHOLD) {
      return account
    }
  }

  throw new StorageFullError(
    `${publicBucket ? 'No public backend' : 'No private backend'} can accept ${sizeBytes} bytes (threshold: ${config.QUOTA_THRESHOLD * 100}%)`
  )
}

export function setAccountUsedBytes(accountId, usedBytes) {
  const account = accountMap.get(accountId)
  if (!account) return

  account.used_bytes = Math.max(0, usedBytes)
  rebuildActiveAccounts()
}

export async function patchAccountUsageToRtdb(accountId) {
  const account = accountMap.get(accountId)
  if (!account) return

  await rtdbPatch(buildRtdbAccountPath(accountId), {
    accountId,
    usedBytes: account.used_bytes,
  })
}

export async function reloadAccountsFromRTDB() {
  try {
    const rtdbAccounts = await rtdbGet('/accounts')
    const ids = []

    if (rtdbAccounts && typeof rtdbAccounts === 'object') {
      for (const [accountKey, data] of Object.entries(rtdbAccounts)) {
        const accountId = resolveAccountIdFromRtdbEntry(accountKey, data?.accountId ?? data?.account_id)
        if (!accountId) continue
        ids.push(accountId)
        const supabase = data?.supabase && typeof data.supabase === 'object' ? data.supabase : {}
        upsertAccount({
          account_id: accountId,
          access_key_id: data.accessKeyId,
          secret_key: data.secretAccessKey,
          endpoint: data.endpoint,
          region: data.region,
          bucket: data.bucket,
          addressing_style: data.addressingStyle ?? data.addressing_style ?? 'path',
          payload_signing_mode: data.payloadSigningMode ?? data.payload_signing_mode ?? 'unsigned',
          email_owner: data.emailOwner ?? data.email_owner ?? supabase.emailOwner ?? '',
          supabase_access_token: data.supabaseAccessToken
            ?? data.supabase_access_token
            ?? supabase.accessToken
            ?? '',
          supabase_access_token_exp: normalizeSupabaseAccessTokenExp(
            data.supabaseAccessTokenExp
            ?? data.supabaseAccessTokenExperimental
            ?? data.supabase_access_token_exp
            ?? data.supabase_access_token_experimental
            ?? data['supabase.accessToken.exp']
            ?? data['supabase.accessToken.experimental']
            ?? supabase.accessTokenExp
            ?? supabase.accessTokenExperimental
            ?? supabase.accessToken?.experimental
            ?? supabase.accessToken?.exp,
          ),
          public_bucket: data.publicBucket
            ?? data.public_bucket
            ?? false,
          quota_bytes: data.quotaBytes ?? 5_368_709_120,
          used_bytes: data.usedBytes ?? 0,
          active: data.active ? 1 : 0,
          added_at: data.addedAt ?? Date.now(),
        })
      }
    }

    deactivateMissingAccounts(ids)
  } catch (err) {
    process.stderr.write(`[accountPool] reloadAccountsFromRTDB error: ${err.message}\n`)
  }

  loadFromSQLite()
}

export function reloadAccountsFromSQLite() {
  loadFromSQLite()
}

export function getAccount(accountId) {
  return accountMap.get(accountId)
}

export function getAccountsStats() {
  const all = [...accountMap.values()]
  let totalBytes = 0
  let usedBytes = 0
  let full = 0

  for (const account of all) {
    totalBytes += account.quota_bytes
    usedBytes += account.used_bytes
    if ((account.used_bytes / account.quota_bytes) >= config.QUOTA_THRESHOLD) full++
  }

  return { total: all.length, active: activeAccounts.length, full, totalBytes, usedBytes }
}
