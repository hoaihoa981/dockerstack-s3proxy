/**
 * src/accountId.js
 * Shared account-id normalization/validation and RTDB key mapping helpers.
 */

const ACCOUNT_ID_MIN_LENGTH = 3
const ACCOUNT_ID_MAX_LENGTH = 80
const ACCOUNT_ID_PATTERN = /^[A-Za-z0-9_-]+$/
const ACCOUNT_ID_FORBIDDEN_CHARS = /[.#$[\]/\s]/
const RTDB_KEY_FORBIDDEN_CHARS = /[.#$[\]/]/
const CONTROL_CHARS = /[\u0000-\u001F\u007F]/
const ENCODED_KEY_PREFIX = '~aid~'

export function normalizeAccountId(value) {
  if (value === undefined || value === null) return ''
  return String(value).trim()
}

export function suggestAccountId(value) {
  const raw = normalizeAccountId(value)
  if (!raw) return ''

  const suggestion = raw
    .replace(/[.#$[\]/\s]+/g, '-')
    .replace(/[^A-Za-z0-9_-]+/g, '-')
    .replace(/[-_]{2,}/g, '-')
    .replace(/^[-_]+|[-_]+$/g, '')
    .slice(0, ACCOUNT_ID_MAX_LENGTH)

  if (!suggestion) return ''
  if (!ACCOUNT_ID_PATTERN.test(suggestion)) return ''
  if (suggestion.length < ACCOUNT_ID_MIN_LENGTH) return ''
  return suggestion
}

export function validateAccountIdForRealtime(value) {
  const accountId = normalizeAccountId(value)
  if (!accountId) {
    return { valid: false, accountId, reason: 'is required' }
  }

  if (accountId.length < ACCOUNT_ID_MIN_LENGTH || accountId.length > ACCOUNT_ID_MAX_LENGTH) {
    return {
      valid: false,
      accountId,
      reason: `must be ${ACCOUNT_ID_MIN_LENGTH}-${ACCOUNT_ID_MAX_LENGTH} characters`,
    }
  }

  if (CONTROL_CHARS.test(accountId) || ACCOUNT_ID_FORBIDDEN_CHARS.test(accountId)) {
    return {
      valid: false,
      accountId,
      reason: 'must use only letters, numbers, "-" or "_" (no spaces or . # $ [ ] /)',
    }
  }

  if (!ACCOUNT_ID_PATTERN.test(accountId)) {
    return {
      valid: false,
      accountId,
      reason: 'contains unsupported characters (allowed: letters, numbers, "-" and "_")',
    }
  }

  return { valid: true, accountId, reason: '' }
}

function encodeAccountIdKey(accountId) {
  return `${ENCODED_KEY_PREFIX}${Buffer.from(accountId, 'utf8').toString('base64url')}`
}

function decodeAccountIdKey(encodedKey) {
  if (!encodedKey.startsWith(ENCODED_KEY_PREFIX)) return encodedKey
  const payload = encodedKey.slice(ENCODED_KEY_PREFIX.length)
  if (!payload) return encodedKey

  try {
    const decoded = Buffer.from(payload, 'base64url').toString('utf8')
    return normalizeAccountId(decoded) || encodedKey
  } catch {
    return encodedKey
  }
}

export function toRtdbAccountKey(accountIdValue) {
  const accountId = normalizeAccountId(accountIdValue)
  if (!accountId) {
    throw new Error('accountId is required')
  }

  if (CONTROL_CHARS.test(accountId) || RTDB_KEY_FORBIDDEN_CHARS.test(accountId) || accountId.startsWith(ENCODED_KEY_PREFIX)) {
    return encodeAccountIdKey(accountId)
  }

  return accountId
}

export function buildRtdbAccountPath(accountIdValue) {
  return `/accounts/${toRtdbAccountKey(accountIdValue)}`
}

export function resolveAccountIdFromRtdbEntry(accountKey, docAccountId) {
  const explicitId = normalizeAccountId(docAccountId)
  if (explicitId) return explicitId
  return decodeAccountIdKey(normalizeAccountId(accountKey))
}
