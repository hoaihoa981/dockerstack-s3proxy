/**
 * src/metadata.js
 * Shared helpers for logical metadata, backend key mapping, and RTDB payloads.
 */

import {
  ROUTE_RECONCILE_STATUS,
  ROUTE_SCOPE,
  ROUTE_STATE,
  ROUTE_SYNC_STATE,
} from './db.js'

export const LIST_TOKEN_VERSION = 1
export const ORPHAN_BUCKET = '__orphan__'
export const PUBLIC_PROXY_BUCKET = 'public-bucket-proxy'

export function encodeKey(bucket, objectKey) {
  return Buffer.from(`${bucket}/${objectKey}`).toString('base64url')
}

export function buildBackendKey(bucket, objectKey) {
  return objectKey ? `${bucket}/${objectKey}` : `${bucket}/`
}

export function parseBackendKey(backendKey) {
  if (typeof backendKey !== 'string') return null
  const separator = backendKey.indexOf('/')
  if (separator <= 0) return null

  const bucket = backendKey.slice(0, separator)
  const objectKey = backendKey.slice(separator + 1)
  if (!bucket || !objectKey) return null

  return { bucket, objectKey }
}

export function encodeListContinuationToken(payload) {
  return Buffer.from(JSON.stringify({ v: LIST_TOKEN_VERSION, ...payload })).toString('base64url')
}

export function decodeListContinuationToken(token) {
  if (!token) return null

  try {
    const decoded = JSON.parse(Buffer.from(token, 'base64url').toString('utf8'))
    if (decoded?.v !== LIST_TOKEN_VERSION || typeof decoded.after !== 'string') {
      return null
    }

    return {
      after: decoded.after,
      kind: decoded.kind === 'prefix' ? 'prefix' : 'object',
    }
  } catch {
    return null
  }
}

export function toRouteCacheValue(route) {
  if (!route) return null

  return {
    accountId: route.account_id,
    bucket: route.bucket,
    objectKey: route.object_key,
    backendKey: route.backend_key,
    sizeBytes: route.size_bytes ?? 0,
    etag: route.etag ?? null,
    lastModified: route.last_modified ?? null,
    contentType: route.content_type ?? null,
    state: route.state,
    deletedAt: route.deleted_at ?? null,
    metadataVersion: route.metadata_version ?? 1,
    routeScope: route.route_scope ?? ROUTE_SCOPE.MAIN,
    publicUrl: route.public_url ?? null,
    syncState: route.sync_state ?? ROUTE_SYNC_STATE.SYNCED,
    reconcileStatus: route.reconcile_status ?? ROUTE_RECONCILE_STATUS.HEALTHY,
  }
}

export function isVisibleRoute(route) {
  return Boolean(route)
    && route.state === ROUTE_STATE.ACTIVE
    && (route.deleted_at === null || route.deleted_at === undefined)
}

export function buildRtdbRouteDocument(route) {
  return {
    accountId: route.account_id,
    bucket: route.bucket,
    objectKey: route.object_key,
    backendKey: route.backend_key,
    sizeBytes: route.size_bytes ?? 0,
    etag: route.etag ?? null,
    lastModified: route.last_modified ?? null,
    contentType: route.content_type ?? null,
    uploadedAt: route.uploaded_at ?? Date.now(),
    updatedAt: route.updated_at ?? Date.now(),
    deletedAt: route.deleted_at ?? null,
    metadataVersion: route.metadata_version ?? 1,
    routeScope: route.route_scope ?? ROUTE_SCOPE.MAIN,
    publicUrl: route.public_url ?? null,
    state: route.state ?? ROUTE_STATE.ACTIVE,
    syncState: route.sync_state ?? ROUTE_SYNC_STATE.SYNCED,
    reconcileStatus: route.reconcile_status ?? ROUTE_RECONCILE_STATUS.HEALTHY,
    backendLastSeenAt: route.backend_last_seen_at ?? null,
    backendMissingSince: route.backend_missing_since ?? null,
    lastReconciledAt: route.last_reconciled_at ?? null,
    instanceId: route.instance_id ?? '',
  }
}

export function routeFromRtdb(encodedKey, doc = {}) {
  return {
    encoded_key: encodedKey,
    account_id: doc.accountId,
    bucket: doc.bucket ?? '',
    object_key: doc.objectKey ?? '',
    backend_key: doc.backendKey ?? (doc.bucket && doc.objectKey ? buildBackendKey(doc.bucket, doc.objectKey) : doc.objectKey ?? ''),
    size_bytes: doc.sizeBytes ?? 0,
    etag: doc.etag ?? null,
    last_modified: doc.lastModified ?? doc.uploadedAt ?? Date.now(),
    content_type: doc.contentType ?? null,
    uploaded_at: doc.uploadedAt ?? Date.now(),
    updated_at: doc.updatedAt ?? doc.uploadedAt ?? Date.now(),
    deleted_at: doc.deletedAt ?? null,
    metadata_version: doc.metadataVersion ?? 1,
    route_scope: doc.routeScope ?? ROUTE_SCOPE.MAIN,
    public_url: doc.publicUrl ?? null,
    state: doc.state ?? ROUTE_STATE.ACTIVE,
    sync_state: doc.syncState ?? ROUTE_SYNC_STATE.SYNCED,
    reconcile_status: doc.reconcileStatus ?? ROUTE_RECONCILE_STATUS.HEALTHY,
    backend_last_seen_at: doc.backendLastSeenAt ?? null,
    backend_missing_since: doc.backendMissingSince ?? null,
    last_reconciled_at: doc.lastReconciledAt ?? null,
    instance_id: doc.instanceId ?? '',
  }
}

export function buildOpaqueOrphanRoute(accountId, backendKey, inventory = {}, now = Date.now()) {
  const encodedKey = encodeKey(ORPHAN_BUCKET, `${accountId}/${backendKey}`)

  return {
    encoded_key: encodedKey,
    account_id: accountId,
    bucket: ORPHAN_BUCKET,
    object_key: `${accountId}/${backendKey}`,
    backend_key: backendKey,
    size_bytes: inventory.sizeBytes ?? 0,
    etag: inventory.etag ?? null,
    last_modified: inventory.lastModified ?? now,
    content_type: inventory.contentType ?? null,
    uploaded_at: inventory.lastModified ?? now,
    updated_at: now,
    deleted_at: null,
    metadata_version: inventory.metadataVersion ?? 1,
    route_scope: ROUTE_SCOPE.MAIN,
    public_url: null,
    state: ROUTE_STATE.ORPHANED,
    sync_state: ROUTE_SYNC_STATE.PENDING_SYNC,
    reconcile_status: ROUTE_RECONCILE_STATUS.NEEDS_REVIEW,
    backend_last_seen_at: now,
    backend_missing_since: null,
    last_reconciled_at: now,
    instance_id: inventory.instanceId ?? '',
  }
}
