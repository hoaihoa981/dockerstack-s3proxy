/**
 * src/index.js
 * Fastify server bootstrap.
 */

import Fastify from 'fastify'
import pino from 'pino'
import { randomBytes } from 'crypto'

import config from './config.js'
import {
  db,
  deleteRoute,
  ensureBucketsFromActiveRoutes,
  getAllRoutes,
  upsertRoute,
} from './db.js'
import { cacheSet, cacheDelete } from './cache.js'
import {
  reloadAccountsFromRTDB,
  getAccountsStats,
} from './accountPool.js'
import { rtdbGet, rtdbPatch, rtdbListen } from './firebase.js'
import { startQuotaPoller, stopQuotaPoller } from './quotaPoller.js'
import { startReconciler, stopReconciler, runReconcilerCycle } from './reconciler.js'
import { flushPendingRouteSync } from './controlPlane.js'
import { routeFromRtdb, isVisibleRoute, toRouteCacheValue } from './metadata.js'
import { setRtdbState } from './routes/health.js'
import { metrics, refreshMetadataMetrics } from './routes/metrics.js'
import { sendAlert } from './utils/webhook.js'
import { startCronScheduler, stopCronScheduler } from './cronScheduler.js'

import authPlugin from './plugins/auth.js'
import errorHandler from './plugins/errorHandler.js'
import healthRoutes from './routes/health.js'
import metricsRoutes from './routes/metrics.js'
import accountRoutes from './routes/accounts.js'
import adminRoutes from './routes/admin.js'
import publicBucketProxyRoutes from './routes/publicBucketProxy.js'
import s3Routes from './routes/s3.js'

const log = pino({
  level: config.LOG_LEVEL,
  ...(process.env.NODE_ENV !== 'production' ? {
    transport: { target: 'pino-pretty', options: { colorize: true } },
  } : {}),
})

const fastify = Fastify({
  logger: log,
  genReqId: () => randomBytes(6).toString('base64url').slice(0, 10),
  bodyLimit: 100 * 1024 * 1024,
  ignoreTrailingSlash: true,
})

let routesListener = null
let accountsListener = null
let rtdbLastEventAt = Date.now()
let accountsDebounceTimer = null
let shuttingDown = false

function applyRouteDocument(encodedKey, routeDoc) {
  const row = routeFromRtdb(encodedKey, routeDoc)
  upsertRoute(row)

  if (isVisibleRoute(row)) {
    cacheSet(encodedKey, toRouteCacheValue(row))
  } else {
    cacheDelete(encodedKey)
  }
}

function startRoutesListener() {
  let backoff = 1000

  function connect() {
    if (routesListener) {
      try { routesListener.close() } catch {
        // ignore
      }
    }

    routesListener = rtdbListen(
      '/routes',
      (eventType, data) => {
        rtdbLastEventAt = Date.now()
        metrics.rtdbSyncLagMs.set(0)

        try {
          if (eventType === 'put') {
            if (!data) return

            if (data.path === '/' && data.data) {
              for (const [encodedKey, routeDoc] of Object.entries(data.data)) {
                if (!routeDoc) continue
                applyRouteDocument(encodedKey, routeDoc)
              }
            } else {
              const encodedKey = data.path?.replace(/^\//, '')
              if (!encodedKey) return

              if (data.data === null) {
                deleteRoute(encodedKey)
                cacheDelete(encodedKey)
              } else {
                applyRouteDocument(encodedKey, data.data)
              }
            }
          } else if (eventType === 'patch') {
            for (const [encodedKey, routeDoc] of Object.entries(data.data ?? {})) {
              if (!routeDoc) continue
              applyRouteDocument(encodedKey, routeDoc)
            }
          }

          refreshMetadataMetrics()
        } catch (err) {
          log.error({ err }, 'routes listener processing error')
        }

        backoff = 1000
      },
      (err) => {
        log.warn({ err: err.message, backoff }, 'routes listener error; reconnecting')
        setRtdbState({ listenerActive: false })
        setTimeout(() => {
          backoff = Math.min(backoff * 2, 60_000)
          connect()
        }, backoff)
      },
    )

    setRtdbState({ listenerActive: true })
  }

  connect()
}

function startAccountsListener() {
  let backoff = 1000

  function connect() {
    if (accountsListener) {
      try { accountsListener.close() } catch {
        // ignore
      }
    }

    accountsListener = rtdbListen(
      '/accounts',
      () => {
        rtdbLastEventAt = Date.now()

        if (accountsDebounceTimer) clearTimeout(accountsDebounceTimer)
        accountsDebounceTimer = setTimeout(() => {
          reloadAccountsFromRTDB().catch((err) => {
            log.error({ err }, 'accounts reload error')
          })
        }, 2000)

        backoff = 1000
      },
      (err) => {
        log.warn({ err: err.message, backoff }, 'accounts listener error; reconnecting')
        setTimeout(() => {
          backoff = Math.min(backoff * 2, 60_000)
          connect()
        }, backoff)
      },
    )
  }

  connect()
}

async function shutdown(signal) {
  if (shuttingDown) return
  shuttingDown = true

  log.info({ signal }, 'shutting down')

  try {
    await fastify.close()
  } catch (err) {
    log.error({ err }, 'error closing Fastify')
  }

  stopQuotaPoller()
  stopReconciler()
  stopCronScheduler()

  if (routesListener) {
    try { routesListener.close() } catch {
      // ignore
    }
  }
  if (accountsListener) {
    try { accountsListener.close() } catch {
      // ignore
    }
  }

  try {
    await rtdbPatch(`/instances/${config.INSTANCE_ID}`, { healthy: false })
  } catch {
    // ignore
  }

  try {
    db.close()
  } catch {
    // ignore
  }

  log.info('shutdown complete')
  process.exit(0)
}

process.on('SIGTERM', () => shutdown('SIGTERM'))
process.on('SIGINT', () => shutdown('SIGINT'))

process.on('unhandledRejection', (reason, promise) => {
  log.error({ reason, promise }, 'unhandled promise rejection')
  sendAlert({ event: 'unhandled_rejection', detail: String(reason) })
})

process.on('uncaughtException', (err) => {
  log.fatal({ err }, 'uncaught exception')
  sendAlert({ event: 'uncaught_exception', detail: err.message })
  process.exit(1)
})

setInterval(() => {
  metrics.rtdbSyncLagMs.set(Date.now() - rtdbLastEventAt)
}, 5000).unref()

async function bootstrap() {
  log.info({ instanceId: config.INSTANCE_ID, port: config.PORT }, 'config validated')
  log.info({ path: config.SQLITE_PATH }, 'sqlite initialized')

  let rtdbConnected = false
  try {
    await rtdbGet('/accounts')
    rtdbConnected = true
    log.info('rtdb connected')
  } catch (err) {
    log.warn({ err: err.message }, 'rtdb unreachable; continuing with local SQLite state')
  }
  setRtdbState({ connected: rtdbConnected })

  if (rtdbConnected) {
    try {
      await reloadAccountsFromRTDB()
      log.info('accounts loaded from rtdb')
    } catch (err) {
      log.warn({ err: err.message }, 'account reload failed; using local SQLite state')
    }
  }

  if (rtdbConnected) {
    try {
      const rtdbRoutes = await rtdbGet('/routes')
      if (rtdbRoutes && typeof rtdbRoutes === 'object') {
        for (const [encodedKey, routeDoc] of Object.entries(rtdbRoutes)) {
          if (!routeDoc?.accountId) continue
          applyRouteDocument(encodedKey, routeDoc)
        }
        log.info({ count: Object.keys(rtdbRoutes).length }, 'routes backfilled from rtdb')
      }
    } catch (err) {
      log.warn({ err: err.message }, 'failed to backfill routes from rtdb')
    }
  }

  try {
    const recentRoutes = getAllRoutes()
      .filter((route) => isVisibleRoute(route))
      .slice(0, 10_000)

    for (const route of recentRoutes) {
      cacheSet(route.encoded_key, toRouteCacheValue(route))
    }
    log.info({ count: recentRoutes.length }, 'route cache warmed')
  } catch (err) {
    log.warn({ err: err.message }, 'route cache warm failed')
  }

  try {
    const bucketRepair = ensureBucketsFromActiveRoutes(Date.now())
    log.info({ bucketRepair }, 'bucket state repaired from active routes')
  } catch (err) {
    log.warn({ err: err.message }, 'bucket state repair failed')
  }

  await fastify.register(authPlugin)
  await fastify.register(errorHandler)

  fastify.decorate('config', config)

  await fastify.register(healthRoutes)
  await fastify.register(metricsRoutes)
  await fastify.register(accountRoutes)
  await fastify.register(adminRoutes)
  await fastify.register(publicBucketProxyRoutes)
  await fastify.register(s3Routes, { prefix: '/' })

  fastify.addHook('onSend', async (_request, reply) => {
    if (!reply.hasHeader('Access-Control-Allow-Origin')) {
      reply.header('Access-Control-Allow-Origin', '*')
    }
  })

  await fastify.listen({ port: config.PORT, host: '0.0.0.0' })
  log.info({ port: config.PORT }, 'fastify listening')

  if (rtdbConnected) {
    startRoutesListener()
    startAccountsListener()
    log.info('rtdb listeners started')
  } else {
    log.warn('rtdb listeners skipped because remote is offline')
  }

  const startedAt = Date.now()
  async function heartbeat() {
    try {
      await rtdbPatch(`/instances/${config.INSTANCE_ID}`, {
        lastHeartbeat: Date.now(),
        startedAt,
        healthy: true,
      })
    } catch {
      // ignore
    }
  }

  await heartbeat().catch(() => {})
  const heartbeatTimer = setInterval(() => heartbeat().catch(() => {}), 30000)
  heartbeatTimer.unref()

  startQuotaPoller(log)
  startReconciler(log)
  await startCronScheduler(log)
  refreshMetadataMetrics()

  try {
    await flushPendingRouteSync(log)
  } catch (err) {
    log.warn({ err }, 'initial pending sync flush failed')
  }

  runReconcilerCycle(log).catch((err) => {
    log.warn({ err }, 'initial reconciler cycle failed')
  })

  const stats = getAccountsStats()
  log.info({ accounts: stats }, 'startup complete')
}

bootstrap().catch((err) => {
  log.fatal({ err }, 'bootstrap failed')
  process.exit(1)
})
