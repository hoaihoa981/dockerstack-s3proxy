/**
 * src/publicObjectUrl.js
 * Build direct backend URLs for public objects.
 */

function encodeSegment(value = '') {
  return encodeURIComponent(String(value)).replace(/[!'()*]/g, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`)
}

function encodePath(value = '') {
  return String(value)
    .split('/')
    .filter((segment) => segment !== '')
    .map((segment) => encodeSegment(segment))
    .join('/')
}

function joinPath(...segments) {
  const normalized = segments
    .map((segment) => String(segment ?? '').trim())
    .filter(Boolean)
    .map((segment) => segment.replace(/^\/+|\/+$/g, ''))
    .filter(Boolean)

  return normalized.length > 0 ? `/${normalized.join('/')}` : '/'
}

function isSupabaseS3Endpoint(endpointUrl) {
  const path = endpointUrl.pathname.replace(/\/+$/, '')
  return /\/storage\/v1\/s3$/i.test(path)
    && /\.supabase\.co$/i.test(endpointUrl.hostname)
}

export function buildDirectPublicObjectUrl(account, backendKey) {
  if (!account?.endpoint || !account?.bucket || !backendKey) return null

  let endpointUrl
  try {
    endpointUrl = new URL(account.endpoint)
  } catch {
    return null
  }

  const encodedBucket = encodePath(account.bucket)
  const encodedBackendKey = encodePath(backendKey)

  if (isSupabaseS3Endpoint(endpointUrl)) {
    return `${endpointUrl.protocol}//${endpointUrl.host}${joinPath('storage/v1/object/public', encodedBucket, encodedBackendKey)}`
  }

  const addressingStyle = String(account.addressing_style ?? 'path').toLowerCase()
  if (addressingStyle === 'virtual') {
    const host = `${account.bucket}.${endpointUrl.hostname}${endpointUrl.port ? `:${endpointUrl.port}` : ''}`
    return `${endpointUrl.protocol}//${host}${joinPath(endpointUrl.pathname, encodedBackendKey)}`
  }

  return `${endpointUrl.protocol}//${endpointUrl.host}${joinPath(endpointUrl.pathname, encodedBucket, encodedBackendKey)}`
}
