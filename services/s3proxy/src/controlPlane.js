/**
 * src/controlPlane.js
 * Shared helpers for RTDB metadata replication and pending-sync flushing.
 */

import { getPendingSyncRoutes, markRouteSynced } from './db.js'
import { rtdbSet, rtdbPatch } from './firebase.js'
import { buildRtdbRouteDocument } from './metadata.js'
import { metrics, refreshMetadataMetrics } from './routes/metrics.js'
import config from './config.js'
import { buildRtdbAccountPath } from './accountId.js'

export async function syncRouteToRtdb(route) {
  await rtdbSet(`/routes/${route.encoded_key}`, buildRtdbRouteDocument(route))
  markRouteSynced(route.encoded_key)
}

export async function syncAccountUsageToRtdb(account) {
  if (!account?.account_id) return
  await rtdbPatch(buildRtdbAccountPath(account.account_id), {
    accountId: account.account_id,
    usedBytes: account.used_bytes ?? 0,
  })
}

export async function syncAccountsUsageBatch(accounts = [], log = console) {
  for (const account of accounts) {
    try {
      await syncAccountUsageToRtdb(account)
    } catch (err) {
      log.warn?.({ err, accountId: account.account_id }, 'account usage RTDB sync failed')
    }
  }
}

export async function flushPendingRouteSync(log = console, limit = config.PENDING_SYNC_BATCH_SIZE) {
  const pending = getPendingSyncRoutes(limit)
  if (pending.length === 0) return 0

  let synced = 0

  for (const route of pending) {
    try {
      await syncRouteToRtdb(route)
      synced += 1
    } catch (err) {
      metrics.metadataCommitFailuresTotal.inc({ stage: 'rtdb_sync' })
      log.warn?.({ err, encodedKey: route.encoded_key }, 'pending route sync failed')
    }
  }

  if (synced > 0) {
    refreshMetadataMetrics()
  }

  return synced
}
