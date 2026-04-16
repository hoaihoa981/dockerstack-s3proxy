/**
 * src/routes/publicBucketProxy.js
 * CRUD API for public files stored directly on public backends.
 */

import { createHash } from 'crypto'
import { Readable } from 'stream'

import { cacheDelete, cacheSet } from '../cache.js'
import {
  commitUploadedObjectMetadata,
  finalizeRouteDelete,
  getAccountById,
  getRoute,
  listPublicRoutes,
  markRouteDeleting,
  revertDeletingRoute,
  ROUTE_RECONCILE_STATUS,
  ROUTE_SCOPE,
  ROUTE_STATE,
} from '../db.js'
import {
  buildBackendKey,
  encodeKey,
  PUBLIC_PROXY_BUCKET,
  toRouteCacheValue,
} from '../metadata.js'
import { patchAccountUsageToRtdb, selectAccountForUpload, StorageFullError, syncAccountsFromRows } from '../accountPool.js'
import { syncRouteToRtdb } from '../controlPlane.js'
import { buildDirectPublicObjectUrl } from '../publicObjectUrl.js'
import { proxyRequest } from '../utils/sigv4.js'
import { metrics, refreshMetadataMetrics } from './metrics.js'
import config from '../config.js'

function normalizeBoolean(value) {
  if (value === undefined || value === null || value === '') return false
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0

  return ['true', '1', 'yes', 'y', 'on'].includes(String(value).trim().toLowerCase())
}

function normalizeFilePath(value) {
  const normalized = String(value ?? '')
    .trim()
    .replace(/^\/+/, '')
    .replace(/\/{2,}/g, '/')

  return normalized
}

function getPayloadStream(request) {
  if (request.body && typeof request.body.pipe === 'function') {
    return request.body
  }
  if (Buffer.isBuffer(request.body)) {
    return Readable.from(request.body)
  }
  if (request.body instanceof Uint8Array) {
    return Readable.from(Buffer.from(request.body))
  }
  if (typeof request.body === 'string') {
    return Readable.from(Buffer.from(request.body))
  }
  return request.raw
}

async function readRequestBodyBuffer(request, maxBytes = 100 * 1024 * 1024) {
  if (Buffer.isBuffer(request.body)) return request.body
  if (request.body instanceof Uint8Array) return Buffer.from(request.body)
  if (typeof request.body === 'string') return Buffer.from(request.body)

  const source = request.body && typeof request.body[Symbol.asyncIterator] === 'function'
    ? request.body
    : request.raw

  const chunks = []
  let total = 0

  for await (const chunk of source) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
    total += buffer.length

    if (total > maxBytes) {
      const err = new Error(`Request body exceeds ${maxBytes} bytes`)
      err.statusCode = 413
      throw err
    }

    chunks.push(buffer)
  }

  return Buffer.concat(chunks)
}

function verifyContentMd5Header(request, bodyBuffer) {
  const provided = request.headers['content-md5']
  if (!provided) return { ok: true }

  const expected = String(provided).trim()
  const digest = createHash('md5').update(bodyBuffer).digest('base64')
  return { ok: digest === expected }
}

function objectMetadataFromHeaders(headers, fallback = {}) {
  const contentLength = Number.parseInt(headers['content-length'] ?? fallback.size_bytes ?? '0', 10)
  const lastModifiedHeader = headers['last-modified']
  const parsedLastModified = lastModifiedHeader ? Date.parse(lastModifiedHeader) : NaN

  return {
    sizeBytes: Number.isFinite(contentLength) ? contentLength : 0,
    etag: headers.etag?.replace(/"/g, '') ?? fallback.etag ?? null,
    contentType: headers['content-type'] ?? fallback.content_type ?? 'application/octet-stream',
    lastModified: Number.isFinite(parsedLastModified) ? parsedLastModified : (fallback.last_modified ?? Date.now()),
  }
}

async function consumeTextBody(response) {
  if (!response?.body) return ''
  return response.body.text().catch(() => '')
}

function getPublicEncodedKey(filePath) {
  return encodeKey(PUBLIC_PROXY_BUCKET, filePath)
}

function getTrackedPublicRoute(filePath) {
  const route = getRoute(getPublicEncodedKey(filePath))
  if (!route || route.route_scope !== ROUTE_SCOPE.PUBLIC) return null
  return route
}

function toPublicFile(route) {
  const account = getAccountById(route.account_id)
  const directUrl = route.public_url ?? buildDirectPublicObjectUrl(account, route.backend_key)

  return {
    path: route.object_key,
    encodedKey: route.encoded_key,
    backendKey: route.backend_key,
    directUrl,
    accountId: route.account_id,
    contentType: route.content_type ?? 'application/octet-stream',
    sizeBytes: route.size_bytes ?? 0,
    etag: route.etag ?? null,
    uploadedAt: route.uploaded_at ?? null,
    updatedAt: route.updated_at ?? null,
    lastModified: route.last_modified ?? null,
    metadataVersion: route.metadata_version ?? 1,
    state: route.state,
    routeScope: route.route_scope,
  }
}

async function syncCommittedRoute(route, accounts, request) {
  try {
    await syncRouteToRtdb(route)
  } catch (err) {
    metrics.metadataCommitFailuresTotal.inc({ stage: 'rtdb_sync' })
    request.log.warn({ err, encodedKey: route.encoded_key }, 'public route RTDB sync failed; leaving PENDING_SYNC')
  }

  for (const account of accounts) {
    try {
      await patchAccountUsageToRtdb(account.account_id)
    } catch (err) {
      request.log.warn({ err, accountId: account.account_id }, 'public route account usage RTDB patch failed')
    }
  }
}

async function choosePublicUploadTarget(filePath, encodedKey, sizeBytes) {
  const existingMetadata = getTrackedPublicRoute(filePath)
  if (existingMetadata && ![ROUTE_STATE.DELETED, ROUTE_STATE.DELETING].includes(existingMetadata.state)) {
    const account = getAccountById(existingMetadata.account_id)
    if (account) {
      return {
        account,
        backendKey: existingMetadata.backend_key || buildBackendKey(PUBLIC_PROXY_BUCKET, filePath),
        existingMetadata,
      }
    }
  }

  const account = selectAccountForUpload(sizeBytes, { publicBucket: true })
  return {
    account,
    backendKey: buildBackendKey(PUBLIC_PROXY_BUCKET, filePath),
    existingMetadata: existingMetadata ?? null,
  }
}

export default async function publicBucketProxyRoutes(fastify, _opts) {
  try {
    fastify.addContentTypeParser('*', (request, payload, done) => done(null, payload))
  } catch {
    // parser may already exist in current encapsulation context.
  }

  fastify.get('/public-bucket-proxy/files', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const prefix = normalizeFilePath(request.query?.prefix ?? '')
    const files = listPublicRoutes(prefix).map(toPublicFile)

    reply.send({
      total: files.length,
      files,
    })
  })

  fastify.get('/public-bucket-proxy/files/*', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const filePath = normalizeFilePath(request.params['*'])
    if (!filePath) {
      return reply.code(400).send({ ok: false, error: 'file path is required' })
    }

    const route = getTrackedPublicRoute(filePath)
    if (!route || route.state !== ROUTE_STATE.ACTIVE || route.deleted_at !== null) {
      return reply.code(404).send({ ok: false, error: 'public file not found' })
    }

    const file = toPublicFile(route)
    if (normalizeBoolean(request.query?.redirect)) {
      if (!file.directUrl) {
        return reply.code(409).send({ ok: false, error: 'directUrl is unavailable for this backend' })
      }
      return reply.redirect(file.directUrl)
    }

    return reply.send({ ok: true, file })
  })

  fastify.put('/public-bucket-proxy/files/*', {
    config: { skipAuth: true, rawBody: true },
  }, async (request, reply) => {
    const filePath = normalizeFilePath(request.params['*'])
    if (!filePath) {
      return reply.code(400).send({ ok: false, error: 'file path is required' })
    }

    let bodyStream = getPayloadStream(request)
    let sizeBytes = Number.parseInt(request.headers['content-length'] ?? '0', 10) || 0
    if (request.headers['content-md5']) {
      const bodyBuffer = await readRequestBodyBuffer(request)
      const md5Check = verifyContentMd5Header(request, bodyBuffer)
      if (!md5Check.ok) {
        return reply.code(400).send({ ok: false, error: 'Content-MD5 does not match request body' })
      }
      bodyStream = Readable.from(bodyBuffer)
      sizeBytes = bodyBuffer.length
    }

    let target
    try {
      target = await choosePublicUploadTarget(filePath, getPublicEncodedKey(filePath), sizeBytes)
    } catch (err) {
      if (err instanceof StorageFullError) {
        return reply.code(507).send({ ok: false, error: err.message })
      }
      throw err
    }

    const upstream = await proxyRequest({
      account: target.account,
      method: 'PUT',
      path: `/${target.account.bucket}/${target.backendKey}`,
      headers: {
        'content-type': request.headers['x-public-file-content-type']
          || request.headers['x-file-content-type']
          || request.headers['content-type']
          || 'application/octet-stream',
        'x-forwarded-request-id': request.id,
      },
      bodyStream,
    })

    if (upstream.statusCode >= 300) {
      return reply.code(upstream.statusCode).send({
        ok: false,
        error: await consumeTextBody(upstream),
      })
    }

    const now = Date.now()
    const directUrl = buildDirectPublicObjectUrl(target.account, target.backendKey)
    const upstreamMetadata = objectMetadataFromHeaders(upstream.headers, {
      size_bytes: sizeBytes,
      content_type: request.headers['x-public-file-content-type']
        || request.headers['x-file-content-type']
        || request.headers['content-type']
        || 'application/octet-stream',
      last_modified: now,
    })

    let committed
    try {
      committed = commitUploadedObjectMetadata({
        encoded_key: getPublicEncodedKey(filePath),
        account_id: target.account.account_id,
        bucket: PUBLIC_PROXY_BUCKET,
        object_key: filePath,
        backend_key: target.backendKey,
        size_bytes: upstreamMetadata.sizeBytes || sizeBytes,
        etag: upstreamMetadata.etag,
        last_modified: upstreamMetadata.lastModified,
        content_type: upstreamMetadata.contentType,
        uploaded_at: now,
        updated_at: now,
        public_url: directUrl,
        route_scope: ROUTE_SCOPE.PUBLIC,
        instance_id: config.INSTANCE_ID,
      })
    } catch (err) {
      metrics.metadataCommitFailuresTotal.inc({ stage: 'sqlite_write' })
      request.log.error({ err, filePath }, 'public file metadata commit failed after upstream upload')

      try {
        await proxyRequest({
          account: target.account,
          method: 'DELETE',
          path: `/${target.account.bucket}/${target.backendKey}`,
          headers: { 'x-forwarded-request-id': request.id },
        })
      } catch (rollbackErr) {
        metrics.metadataCommitFailuresTotal.inc({ stage: 'rollback_delete' })
        request.log.error({ err: rollbackErr, filePath }, 'public file rollback delete failed after metadata commit failure')
      }

      return reply.code(500).send({ ok: false, error: 'Metadata commit failed after upload' })
    }

    syncAccountsFromRows(committed.affectedAccounts)
    cacheSet(committed.route.encoded_key, toRouteCacheValue(committed.route))
    refreshMetadataMetrics()
    await syncCommittedRoute(committed.route, committed.affectedAccounts, request)

    metrics.uploadBytesTotal.inc({ account_id: target.account.account_id }, committed.route.size_bytes)

    return reply.send({
      ok: true,
      action: target.existingMetadata ? 'updated' : 'created',
      file: toPublicFile(committed.route),
    })
  })

  fastify.delete('/public-bucket-proxy/files/*', {
    config: { skipAuth: true },
  }, async (request, reply) => {
    const filePath = normalizeFilePath(request.params['*'])
    if (!filePath) {
      return reply.code(400).send({ ok: false, error: 'file path is required' })
    }

    const route = getTrackedPublicRoute(filePath)
    if (!route || route.state === ROUTE_STATE.DELETED) {
      return reply.send({ ok: true, deleted: false })
    }

    const account = getAccountById(route.account_id)
    if (!account) {
      return reply.code(404).send({ ok: false, error: 'backend account not found' })
    }

    markRouteDeleting(route.encoded_key, Date.now())

    const upstream = await proxyRequest({
      account,
      method: 'DELETE',
      path: `/${account.bucket}/${route.backend_key}`,
      headers: { 'x-forwarded-request-id': request.id },
    })

    if (upstream.statusCode >= 300 && upstream.statusCode !== 404) {
      const restored = revertDeletingRoute(route.encoded_key, Date.now())
      if (restored && restored.state === ROUTE_STATE.ACTIVE) {
        cacheSet(route.encoded_key, toRouteCacheValue(restored))
        await syncCommittedRoute(restored, [], request)
      }

      return reply.code(upstream.statusCode).send({
        ok: false,
        error: await consumeTextBody(upstream),
      })
    }

    const finalized = finalizeRouteDelete(route.encoded_key, Date.now(), {
      backendMissing: upstream.statusCode === 404,
      reconcileStatus: upstream.statusCode === 404
        ? ROUTE_RECONCILE_STATUS.NEEDS_REVIEW
        : ROUTE_RECONCILE_STATUS.HEALTHY,
    })

    syncAccountsFromRows(finalized.affectedAccounts)
    cacheDelete(route.encoded_key)
    refreshMetadataMetrics()
    if (finalized.route) {
      await syncCommittedRoute(finalized.route, finalized.affectedAccounts, request)
    }

    return reply.send({
      ok: true,
      deleted: true,
      file: finalized.route ? toPublicFile(finalized.route) : null,
    })
  })
}
