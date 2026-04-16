/**
 * src/routes/accounts.js
 * Admin account management APIs for single-account upsert and bulk import.
 */

import { db, getAccountById, getAllAccounts, upsertAccount } from '../db.js'
import { rtdbBatchPatch } from '../firebase.js'
import { reloadAccountsFromRTDB, reloadAccountsFromSQLite } from '../accountPool.js'
import {
  buildRtdbAccountPath,
  suggestAccountId,
  validateAccountIdForRealtime,
} from '../accountId.js'
import {
  isEmailOwner,
  isSupabaseAccessToken,
  normalizeSupabaseAccessTokenExp,
} from '../supabaseS3.js'
import { resolveS3SigningRegion } from '../s3Signing.js'

function hasOwn(object, key) {
  return Object.prototype.hasOwnProperty.call(object, key)
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value)
}

function normalizeString(value) {
  if (value === undefined || value === null) return ''
  return String(value).trim()
}

function normalizePositiveInteger(value, fallback, fieldName, errors, sourceLabel) {
  if (value === undefined || value === null || value === '') return fallback
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric <= 0) {
    errors.push(`${sourceLabel}.${fieldName} must be a positive number`)
    return fallback
  }
  return Math.trunc(numeric)
}

function normalizeNonNegativeInteger(value, fallback, fieldName, errors, sourceLabel) {
  if (value === undefined || value === null || value === '') return fallback
  const numeric = Number(value)
  if (!Number.isFinite(numeric) || numeric < 0) {
    errors.push(`${sourceLabel}.${fieldName} must be a non-negative number`)
    return fallback
  }
  return Math.trunc(numeric)
}

function normalizeBoolean(value, fallback = true) {
  if (value === undefined || value === null || value === '') return fallback
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0

  const normalized = String(value).trim().toLowerCase()
  if (['true', '1', 'yes', 'y', 'on'].includes(normalized)) return true
  if (['false', '0', 'no', 'n', 'off'].includes(normalized)) return false
  return fallback
}

function normalizeAddressingStyle(value) {
  const raw = normalizeString(value).toLowerCase()
  if (!raw) return 'path'
  if (['path', 'path-style', 'path_style'].includes(raw)) return 'path'
  if (['virtual', 'virtual-hosted', 'virtual_hosted', 'virtual-hosted-style'].includes(raw)) return 'virtual'
  return ''
}

function normalizePayloadSigningMode(value) {
  const raw = normalizeString(value).toLowerCase()
  if (!raw) return 'unsigned'
  if (['unsigned', 'unsigned-payload', 'unsigned_payload'].includes(raw)) return 'unsigned'
  if (['signed', 'strict', 'required'].includes(raw)) return 'signed'
  return ''
}

function looksLikeSingleAccountDocument(value) {
  if (!isPlainObject(value)) return false
  return [
    'accountId',
    'account_id',
    'accessKeyId',
    'access_key_id',
    'secretAccessKey',
    'secret_key',
    'endpoint',
    'bucket',
    'emailOwner',
    'supabaseAccessToken',
  ]
    .some((field) => hasOwn(value, field))
}

function getImportEntries(payload) {
  const source = isPlainObject(payload) && hasOwn(payload, 'accounts')
    ? payload.accounts
    : payload

  if (Array.isArray(source)) {
    return source.map((entry, index) => ({
      entry,
      fallbackId: null,
      sourceLabel: `accounts[${index}]`,
    }))
  }

  if (looksLikeSingleAccountDocument(source)) {
    return [{
      entry: source,
      fallbackId: normalizeString(source.accountId ?? source.account_id),
      sourceLabel: 'account',
    }]
  }

  if (isPlainObject(source)) {
    return Object.entries(source).map(([accountId, entry]) => ({
      entry,
      fallbackId: accountId,
      sourceLabel: `accounts.${accountId}`,
    }))
  }

  return []
}

function toRtdbAccountDocument(account) {
  return {
    accountId: account.account_id,
    accessKeyId: account.access_key_id,
    secretAccessKey: account.secret_key,
    endpoint: account.endpoint,
    region: account.region,
    bucket: account.bucket,
    addressingStyle: account.addressing_style ?? 'path',
    payloadSigningMode: account.payload_signing_mode ?? 'unsigned',
    emailOwner: account.email_owner ?? '',
    supabaseAccessToken: account.supabase_access_token ?? '',
    supabaseAccessTokenExp: account.supabase_access_token_exp ?? null,
    supabaseAccessTokenExperimental: account.supabase_access_token_exp ?? null,
    supabase: {
      accessToken: account.supabase_access_token ?? '',
      accessTokenExp: account.supabase_access_token_exp ?? null,
      accessTokenExperimental: account.supabase_access_token_exp ?? null,
    },
    publicBucket: account.public_bucket === 1,
    quotaBytes: account.quota_bytes,
    usedBytes: account.used_bytes,
    active: account.active === 1,
    addedAt: account.added_at,
  }
}

function toPublicAccount(account, action = null) {
  const payload = {
    accountId: account.account_id,
    accessKeyId: account.access_key_id,
    endpoint: account.endpoint,
    region: account.region,
    bucket: account.bucket,
    addressingStyle: account.addressing_style ?? 'path',
    payloadSigningMode: account.payload_signing_mode ?? 'unsigned',
    emailOwner: account.email_owner ?? '',
    supabaseAccessTokenExp: account.supabase_access_token_exp ?? null,
    supabaseAccessTokenExperimental: account.supabase_access_token_exp ?? null,
    hasSupabaseAccessToken: Boolean(account.supabase_access_token),
    publicBucket: account.public_bucket === 1 || account.public_bucket === true,
    quotaBytes: account.quota_bytes,
    usedBytes: account.used_bytes,
    active: account.active === 1 || account.active === true,
    addedAt: account.added_at,
    hasSecret: Boolean(account.secret_key),
  }

  if (action) payload.action = action
  return payload
}

function normalizeAccountEntries(payload) {
  const entries = getImportEntries(payload)
  const errors = []

  if (entries.length === 0) {
    return {
      errors: ['Request body must be a single account, an array of accounts, or an object/map under `accounts`'],
      rows: [],
      resultAccounts: [],
    }
  }

  const seenIds = new Set()
  const rows = []
  const resultAccounts = []

  for (const { entry, fallbackId, sourceLabel } of entries) {
    const errorCountBefore = errors.length

    if (!isPlainObject(entry)) {
      errors.push(`${sourceLabel} must be an object`)
      continue
    }

    const accountId = normalizeString(entry.accountId ?? entry.account_id ?? fallbackId)
    const accountIdValidation = validateAccountIdForRealtime(accountId)
    const accessKeyId = normalizeString(entry.accessKeyId ?? entry.access_key_id)
    const secretKey = normalizeString(entry.secretAccessKey ?? entry.secret_key)
    const endpoint = normalizeString(entry.endpoint)
    const regionInput = normalizeString(entry.region)
    const region = resolveS3SigningRegion({
      endpoint,
      region: regionInput,
    })
    const bucket = normalizeString(entry.bucket)
    const addressingStyle = normalizeAddressingStyle(entry.addressingStyle ?? entry.addressing_style)
    const payloadSigningMode = normalizePayloadSigningMode(entry.payloadSigningMode ?? entry.payload_signing_mode)
    const emailOwner = normalizeString(
      entry.emailOwner
      ?? entry.email_owner
      ?? entry['supabase.emailOwner']
      ?? entry?.supabase?.emailOwner,
    ).toLowerCase()
    const supabaseAccessToken = normalizeString(
      entry.supabaseAccessToken
      ?? entry.supabase_access_token
      ?? entry['supabase.accessToken']
      ?? entry?.supabase?.accessToken,
    )
    const supabaseAccessTokenExp = normalizeSupabaseAccessTokenExp(
      entry.supabaseAccessTokenExp
      ?? entry.supabaseAccessTokenExperimental
      ?? entry.supabase_access_token_exp
      ?? entry.supabase_access_token_experimental
      ?? entry['supabase.accessToken.exp']
      ?? entry['supabase.accessToken.experimental']
      ?? entry?.supabase?.accessTokenExp
      ?? entry?.supabase?.accessTokenExperimental
      ?? entry?.supabase?.accessToken?.experimental
      ?? entry?.supabase?.accessToken?.exp,
    )

    if (!accountIdValidation.valid) {
      const suggestion = suggestAccountId(accountId)
      let message = `${sourceLabel}.accountId ${accountIdValidation.reason}`
      if (suggestion && suggestion !== accountIdValidation.accountId) {
        message += ` (suggested: ${suggestion})`
      }
      errors.push(message)
    }
    if (!accessKeyId) errors.push(`${sourceLabel}.accessKeyId is required`)
    if (!secretKey) errors.push(`${sourceLabel}.secretAccessKey is required`)
    if (!endpoint) errors.push(`${sourceLabel}.endpoint is required`)
    if (!regionInput) errors.push(`${sourceLabel}.region is required`)
    if (!bucket) errors.push(`${sourceLabel}.bucket is required`)
    if (!addressingStyle) errors.push(`${sourceLabel}.addressingStyle must be one of: path, virtual`)
    if (!payloadSigningMode) errors.push(`${sourceLabel}.payloadSigningMode must be one of: unsigned, signed`)
    if (emailOwner && !isEmailOwner(emailOwner)) {
      errors.push(`${sourceLabel}.emailOwner must be a valid email`)
    }
    if (supabaseAccessToken && !isSupabaseAccessToken(supabaseAccessToken)) {
      errors.push(`${sourceLabel}.supabaseAccessToken must match token format sbp_...`)
    }
    if (supabaseAccessTokenExp && !isSupabaseAccessToken(supabaseAccessTokenExp)) {
      errors.push(`${sourceLabel}.supabaseAccessTokenExp must match token format sbp_...`)
    }

    if (accountIdValidation.valid) {
      const normalizedId = accountIdValidation.accountId
      if (seenIds.has(normalizedId)) {
        errors.push(`${sourceLabel}.accountId duplicates another imported account`)
      }
      seenIds.add(normalizedId)
    }

    if (endpoint) {
      try {
        const parsed = new URL(endpoint)
        if (!['http:', 'https:'].includes(parsed.protocol)) {
          errors.push(`${sourceLabel}.endpoint must use http or https`)
        }
      } catch {
        errors.push(`${sourceLabel}.endpoint must be a valid URL`)
      }
    }

    const quotaBytes = normalizePositiveInteger(entry.quotaBytes ?? entry.quota_bytes, 5_368_709_120, 'quotaBytes', errors, sourceLabel)
    const usedBytes = normalizeNonNegativeInteger(entry.usedBytes ?? entry.used_bytes, 0, 'usedBytes', errors, sourceLabel)
    const addedAt = normalizeNonNegativeInteger(entry.addedAt ?? entry.added_at, Date.now(), 'addedAt', errors, sourceLabel)
    const publicBucket = normalizeBoolean(entry.publicBucket ?? entry.public_bucket, false) ? 1 : 0
    const active = normalizeBoolean(entry.active, true) ? 1 : 0

    if (errors.length > errorCountBefore) {
      continue
    }

    const previous = getAccountById(accountIdValidation.accountId)
    const row = {
      account_id: accountIdValidation.accountId,
      access_key_id: accessKeyId,
      secret_key: secretKey,
      endpoint,
      region,
      bucket,
      addressing_style: addressingStyle,
      payload_signing_mode: payloadSigningMode,
      email_owner: emailOwner,
      supabase_access_token: supabaseAccessToken,
      supabase_access_token_exp: supabaseAccessTokenExp,
      public_bucket: publicBucket,
      quota_bytes: quotaBytes,
      used_bytes: usedBytes,
      active,
      added_at: addedAt,
    }

    rows.push(row)
    resultAccounts.push(toPublicAccount(row, previous ? 'updated' : 'created'))
  }

  return { errors, rows, resultAccounts }
}

const applyAccountRows = db.transaction((rows) => {
  for (const row of rows) {
    upsertAccount(row)
  }
})

async function importAccountsHandler(request, reply) {
  const { errors, rows, resultAccounts } = normalizeAccountEntries(request.body)

  if (errors.length > 0) {
    return reply.code(400).send({
      error: 'Invalid account import payload',
      errors,
    })
  }

  applyAccountRows(rows)
  reloadAccountsFromSQLite()

  const updates = Object.fromEntries(rows.map((row) => [buildRtdbAccountPath(row.account_id), toRtdbAccountDocument(row)]))
  let rtdbSynced = true
  let warning = ''

  try {
    await rtdbBatchPatch(updates)
    await reloadAccountsFromRTDB()
  } catch (err) {
    rtdbSynced = false
    warning = `Accounts stored locally, but RTDB sync failed: ${err.message}`
    request.log.warn({ err, accountCount: rows.length }, 'account import RTDB sync failed')
    reloadAccountsFromSQLite()
  }

  return reply.code(200).send({
    imported: rows.length,
    rtdbSynced,
    warning: warning || undefined,
    accounts: resultAccounts,
  })
}

export default async function accountRoutes(fastify, _opts) {
  const authHook = { preHandler: [fastify.authenticate] }

  fastify.get('/admin/accounts', authHook, async (_request, reply) => {
    const accounts = getAllAccounts().map((account) => toPublicAccount(account))
    return reply.code(200).send({
      total: accounts.length,
      accounts,
    })
  })

  fastify.post('/admin/accounts', authHook, importAccountsHandler)
  fastify.post('/admin/accounts/import', authHook, importAccountsHandler)
}
