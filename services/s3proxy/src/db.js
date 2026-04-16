/**
 * src/db.js
 * SQLite init, idempotent migrations, and metadata/account query helpers.
 * Uses better-sqlite3 (synchronous).
 */

import Database from 'better-sqlite3'
import { mkdirSync } from 'fs'
import { dirname } from 'path'
import config from './config.js'

try {
  mkdirSync(dirname(config.SQLITE_PATH), { recursive: true })
} catch {
  // ignore
}

export const ROUTE_STATE = Object.freeze({
  ACTIVE: 'ACTIVE',
  DELETING: 'DELETING',
  DELETED: 'DELETED',
  MISSING_BACKEND: 'MISSING_BACKEND',
  ORPHANED: 'ORPHANED',
})

export const ROUTE_SYNC_STATE = Object.freeze({
  SYNCED: 'SYNCED',
  PENDING_SYNC: 'PENDING_SYNC',
})

export const ROUTE_SCOPE = Object.freeze({
  MAIN: 'main',
  PUBLIC: 'public',
})

export const ROUTE_RECONCILE_STATUS = Object.freeze({
  HEALTHY: 'HEALTHY',
  WRONG_ACCOUNT: 'WRONG_ACCOUNT',
  METADATA_MISMATCH: 'METADATA_MISMATCH',
  NEEDS_REVIEW: 'NEEDS_REVIEW',
})

export const BUCKET_VERSIONING_STATUS = Object.freeze({
  NONE: '',
  ENABLED: 'Enabled',
  SUSPENDED: 'Suspended',
})

export const db = new Database(config.SQLITE_PATH)

db.pragma('journal_mode = WAL')
db.pragma('synchronous = NORMAL')
db.pragma('cache_size = -64000')
db.pragma('foreign_keys = ON')
db.pragma('busy_timeout = 5000')

function toTimestamp(value, fallback = Date.now()) {
  if (value === undefined || value === null || value === '') return fallback
  if (value instanceof Date) return value.getTime()
  const numeric = Number(value)
  return Number.isFinite(numeric) ? numeric : fallback
}

function toNullableTimestamp(value) {
  if (value === undefined || value === null || value === '') return null
  return toTimestamp(value, null)
}

function tableColumns(tableName) {
  return db.prepare(`PRAGMA table_info(${tableName})`).all()
}

function hasColumn(tableName, columnName) {
  return tableColumns(tableName).some((column) => column.name === columnName)
}

function ensureColumn(tableName, columnName, sqlFragment) {
  if (!hasColumn(tableName, columnName)) {
    db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${sqlFragment}`)
  }
}

db.exec(`
  CREATE TABLE IF NOT EXISTS accounts (
    account_id     TEXT    PRIMARY KEY,
    access_key_id  TEXT    NOT NULL,
    secret_key     TEXT    NOT NULL,
    endpoint       TEXT    NOT NULL,
    region         TEXT    NOT NULL,
    bucket         TEXT    NOT NULL,
    addressing_style TEXT  NOT NULL DEFAULT 'path',
    payload_signing_mode TEXT NOT NULL DEFAULT 'unsigned',
    email_owner    TEXT    NOT NULL DEFAULT '',
    supabase_access_token TEXT NOT NULL DEFAULT '',
    supabase_access_token_exp TEXT,
    public_bucket  INTEGER NOT NULL DEFAULT 0,
    quota_bytes    INTEGER NOT NULL DEFAULT 5368709120,
    used_bytes     INTEGER NOT NULL DEFAULT 0,
    active         INTEGER NOT NULL DEFAULT 1,
    added_at       INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS routes (
    encoded_key           TEXT    PRIMARY KEY,
    account_id            TEXT    NOT NULL REFERENCES accounts(account_id),
    bucket                TEXT    NOT NULL,
    object_key            TEXT    NOT NULL,
    backend_key           TEXT    NOT NULL DEFAULT '',
    size_bytes            INTEGER NOT NULL DEFAULT 0,
    etag                  TEXT,
    last_modified         INTEGER,
    content_type          TEXT,
    uploaded_at           INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL DEFAULT 0,
    deleted_at            INTEGER,
    metadata_version      INTEGER NOT NULL DEFAULT 1,
    route_scope           TEXT    NOT NULL DEFAULT 'main',
    public_url            TEXT,
    state                 TEXT    NOT NULL DEFAULT 'ACTIVE',
    sync_state            TEXT    NOT NULL DEFAULT 'SYNCED',
    reconcile_status      TEXT    NOT NULL DEFAULT 'HEALTHY',
    backend_last_seen_at  INTEGER,
    backend_missing_since INTEGER,
    last_reconciled_at    INTEGER,
    instance_id           TEXT    NOT NULL
  );

  CREATE TABLE IF NOT EXISTS multipart_uploads (
    upload_id   TEXT    PRIMARY KEY,
    account_id  TEXT    NOT NULL,
    bucket      TEXT    NOT NULL,
    object_key  TEXT    NOT NULL,
    backend_key TEXT    NOT NULL DEFAULT '',
    started_at  INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS buckets (
    bucket            TEXT    PRIMARY KEY,
    created_at        INTEGER NOT NULL,
    updated_at        INTEGER NOT NULL,
    deleted_at        INTEGER,
    versioning_status TEXT    NOT NULL DEFAULT ''
  );

  CREATE TABLE IF NOT EXISTS cron_jobs (
    job_id       TEXT    PRIMARY KEY,
    name         TEXT    NOT NULL,
    kind         TEXT    NOT NULL,
    expression   TEXT    NOT NULL,
    timezone     TEXT    NOT NULL DEFAULT 'UTC',
    enabled      INTEGER NOT NULL DEFAULT 1,
    payload_json TEXT    NOT NULL DEFAULT '{}',
    source       TEXT    NOT NULL DEFAULT 'user',
    created_at   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL
  );
`)

ensureColumn('routes', 'backend_key', "backend_key TEXT NOT NULL DEFAULT ''")
ensureColumn('routes', 'etag', 'etag TEXT')
ensureColumn('routes', 'last_modified', 'last_modified INTEGER')
ensureColumn('routes', 'content_type', 'content_type TEXT')
ensureColumn('routes', 'updated_at', 'updated_at INTEGER NOT NULL DEFAULT 0')
ensureColumn('routes', 'deleted_at', 'deleted_at INTEGER')
ensureColumn('routes', 'metadata_version', 'metadata_version INTEGER NOT NULL DEFAULT 1')
ensureColumn('routes', 'route_scope', "route_scope TEXT NOT NULL DEFAULT 'main'")
ensureColumn('routes', 'public_url', 'public_url TEXT')
ensureColumn('routes', 'state', "state TEXT NOT NULL DEFAULT 'ACTIVE'")
ensureColumn('routes', 'sync_state', "sync_state TEXT NOT NULL DEFAULT 'SYNCED'")
ensureColumn('routes', 'reconcile_status', "reconcile_status TEXT NOT NULL DEFAULT 'HEALTHY'")
ensureColumn('routes', 'backend_last_seen_at', 'backend_last_seen_at INTEGER')
ensureColumn('routes', 'backend_missing_since', 'backend_missing_since INTEGER')
ensureColumn('routes', 'last_reconciled_at', 'last_reconciled_at INTEGER')
ensureColumn('multipart_uploads', 'backend_key', "backend_key TEXT NOT NULL DEFAULT ''")
ensureColumn('accounts', 'addressing_style', "addressing_style TEXT NOT NULL DEFAULT 'path'")
ensureColumn('accounts', 'payload_signing_mode', "payload_signing_mode TEXT NOT NULL DEFAULT 'unsigned'")
ensureColumn('accounts', 'email_owner', "email_owner TEXT NOT NULL DEFAULT ''")
ensureColumn('accounts', 'supabase_access_token', "supabase_access_token TEXT NOT NULL DEFAULT ''")
ensureColumn('accounts', 'supabase_access_token_exp', 'supabase_access_token_exp TEXT')
ensureColumn('accounts', 'public_bucket', 'public_bucket INTEGER NOT NULL DEFAULT 0')
ensureColumn('buckets', 'updated_at', 'updated_at INTEGER NOT NULL DEFAULT 0')
ensureColumn('buckets', 'deleted_at', 'deleted_at INTEGER')
ensureColumn('buckets', 'versioning_status', "versioning_status TEXT NOT NULL DEFAULT ''")

db.exec(`
  UPDATE routes
  SET backend_key = object_key
  WHERE COALESCE(backend_key, '') = '';

  UPDATE routes
  SET last_modified = COALESCE(last_modified, uploaded_at)
  WHERE last_modified IS NULL;

  UPDATE routes
  SET updated_at = COALESCE(NULLIF(updated_at, 0), uploaded_at)
  WHERE updated_at IS NULL OR updated_at = 0;

  UPDATE routes
  SET metadata_version = COALESCE(NULLIF(metadata_version, 0), 1)
  WHERE metadata_version IS NULL OR metadata_version = 0;

  UPDATE routes
  SET route_scope = 'main'
  WHERE COALESCE(route_scope, '') = '';

  UPDATE routes
  SET state = 'ACTIVE'
  WHERE COALESCE(state, '') = '';

  UPDATE routes
  SET sync_state = 'SYNCED'
  WHERE COALESCE(sync_state, '') = '';

  UPDATE routes
  SET reconcile_status = 'HEALTHY'
  WHERE COALESCE(reconcile_status, '') = '';

  UPDATE multipart_uploads
  SET backend_key = object_key
  WHERE COALESCE(backend_key, '') = '';

  UPDATE accounts
  SET public_bucket = COALESCE(public_bucket, 0)
  WHERE public_bucket IS NULL;

  INSERT OR IGNORE INTO buckets (bucket, created_at, updated_at, deleted_at, versioning_status)
  SELECT bucket, MIN(uploaded_at), MAX(updated_at), NULL, ''
  FROM routes
  WHERE route_scope = 'main'
  GROUP BY bucket;

  UPDATE buckets
  SET updated_at = COALESCE(NULLIF(updated_at, 0), created_at)
  WHERE updated_at IS NULL OR updated_at = 0;

  CREATE INDEX IF NOT EXISTS idx_routes_account ON routes(account_id);
  CREATE INDEX IF NOT EXISTS idx_routes_account_backend ON routes(account_id, backend_key);
  CREATE INDEX IF NOT EXISTS idx_routes_bucket_object ON routes(bucket, object_key, state, deleted_at);
  CREATE INDEX IF NOT EXISTS idx_routes_scope_bucket_object ON routes(route_scope, bucket, object_key, state, deleted_at);
  CREATE INDEX IF NOT EXISTS idx_routes_uploaded ON routes(uploaded_at);
  CREATE INDEX IF NOT EXISTS idx_routes_sync_state ON routes(sync_state, updated_at);
  CREATE INDEX IF NOT EXISTS idx_routes_state ON routes(state, account_id);
  CREATE INDEX IF NOT EXISTS idx_routes_scope_state ON routes(route_scope, state, updated_at);
  CREATE INDEX IF NOT EXISTS idx_accounts_active ON accounts(active, used_bytes);
  CREATE INDEX IF NOT EXISTS idx_accounts_public_active ON accounts(public_bucket, active, used_bytes);
  CREATE INDEX IF NOT EXISTS idx_buckets_deleted ON buckets(deleted_at, bucket);
  CREATE INDEX IF NOT EXISTS idx_cron_jobs_enabled ON cron_jobs(enabled, updated_at);
`)

function isUsageCounted(route) {
  if (!route) return false
  return route.state === ROUTE_STATE.ACTIVE || route.state === ROUTE_STATE.DELETING
}

function applyAccountAdjustments(deltas) {
  for (const [accountId, delta] of deltas.entries()) {
    if (!delta) continue
    db.prepare(`
      UPDATE accounts
      SET used_bytes = MAX(0, used_bytes + @delta)
      WHERE account_id = @account_id
    `).run({ account_id: accountId, delta })
  }
}

function fetchAffectedAccounts(accountIds) {
  return [...new Set(accountIds)]
    .map((accountId) => getAccountById(accountId))
    .filter(Boolean)
}

function normalizeRouteForWrite(route, existing) {
  const now = Date.now()
  const uploadedAt = toTimestamp(route.uploaded_at, existing?.uploaded_at ?? now)
  const updatedAt = toTimestamp(route.updated_at, now)

  return {
    encoded_key: route.encoded_key,
    account_id: route.account_id ?? existing?.account_id ?? '',
    bucket: route.bucket ?? existing?.bucket ?? '',
    object_key: route.object_key ?? existing?.object_key ?? '',
    backend_key: route.backend_key ?? existing?.backend_key ?? route.object_key ?? existing?.object_key ?? '',
    size_bytes: Number(route.size_bytes ?? existing?.size_bytes ?? 0) || 0,
    etag: route.etag ?? existing?.etag ?? null,
    last_modified: toNullableTimestamp(route.last_modified ?? existing?.last_modified ?? uploadedAt),
    content_type: route.content_type ?? existing?.content_type ?? null,
    uploaded_at: uploadedAt,
    updated_at: updatedAt,
    deleted_at: toNullableTimestamp(route.deleted_at ?? existing?.deleted_at ?? null),
    metadata_version: Number(route.metadata_version ?? existing?.metadata_version ?? 1) || 1,
    route_scope: route.route_scope ?? existing?.route_scope ?? ROUTE_SCOPE.MAIN,
    public_url: route.public_url ?? existing?.public_url ?? null,
    state: route.state ?? existing?.state ?? ROUTE_STATE.ACTIVE,
    sync_state: route.sync_state ?? existing?.sync_state ?? ROUTE_SYNC_STATE.SYNCED,
    reconcile_status: route.reconcile_status ?? existing?.reconcile_status ?? ROUTE_RECONCILE_STATUS.HEALTHY,
    backend_last_seen_at: toNullableTimestamp(route.backend_last_seen_at ?? existing?.backend_last_seen_at ?? null),
    backend_missing_since: toNullableTimestamp(route.backend_missing_since ?? existing?.backend_missing_since ?? null),
    last_reconciled_at: toNullableTimestamp(route.last_reconciled_at ?? existing?.last_reconciled_at ?? null),
    instance_id: route.instance_id ?? existing?.instance_id ?? '',
  }
}

const stmts = {
  upsertBucket: db.prepare(`
    INSERT INTO buckets (bucket, created_at, updated_at, deleted_at, versioning_status)
    VALUES (@bucket, @created_at, @updated_at, @deleted_at, @versioning_status)
    ON CONFLICT(bucket) DO UPDATE SET
      updated_at = excluded.updated_at,
      deleted_at = excluded.deleted_at,
      versioning_status = excluded.versioning_status
  `),
  getAllBuckets: db.prepare(`
    SELECT *
    FROM buckets
    ORDER BY bucket ASC
  `),
  getBucket: db.prepare(`
    SELECT *
    FROM buckets
    WHERE bucket = ?
    LIMIT 1
  `),
  listActiveBuckets: db.prepare(`
    SELECT *
    FROM buckets
    WHERE deleted_at IS NULL
    ORDER BY bucket ASC
  `),
  upsertAccount: db.prepare(`
    INSERT OR REPLACE INTO accounts
      (account_id, access_key_id, secret_key, endpoint, region, bucket, addressing_style, payload_signing_mode,
       email_owner, supabase_access_token, supabase_access_token_exp, public_bucket, quota_bytes, used_bytes, active, added_at)
    VALUES
      (@account_id, @access_key_id, @secret_key, @endpoint, @region, @bucket, @addressing_style, @payload_signing_mode,
       @email_owner, @supabase_access_token, @supabase_access_token_exp, @public_bucket, @quota_bytes, @used_bytes, @active, @added_at)
  `),
  getAllAccounts: db.prepare(`SELECT * FROM accounts ORDER BY used_bytes ASC, account_id ASC`),
  getAllActiveAccounts: db.prepare(`SELECT * FROM accounts WHERE active = 1 ORDER BY used_bytes ASC, account_id ASC`),
  getAccountById: db.prepare(`SELECT * FROM accounts WHERE account_id = ?`),
  deleteAccountById: db.prepare(`DELETE FROM accounts WHERE account_id = ?`),
  setUsedBytesAbsolute: db.prepare(`
    UPDATE accounts
    SET used_bytes = MAX(0, @bytes)
    WHERE account_id = @account_id
  `),
  upsertCronJob: db.prepare(`
    INSERT INTO cron_jobs (
      job_id, name, kind, expression, timezone, enabled,
      payload_json, source, created_at, updated_at
    ) VALUES (
      @job_id, @name, @kind, @expression, @timezone, @enabled,
      @payload_json, @source, @created_at, @updated_at
    )
    ON CONFLICT(job_id) DO UPDATE SET
      name = excluded.name,
      kind = excluded.kind,
      expression = excluded.expression,
      timezone = excluded.timezone,
      enabled = excluded.enabled,
      payload_json = excluded.payload_json,
      source = excluded.source,
      updated_at = excluded.updated_at
  `),
  getCronJobById: db.prepare(`SELECT * FROM cron_jobs WHERE job_id = ? LIMIT 1`),
  getAllCronJobs: db.prepare(`
    SELECT *
    FROM cron_jobs
    ORDER BY source DESC, updated_at DESC, job_id ASC
  `),
  deleteCronJob: db.prepare(`DELETE FROM cron_jobs WHERE job_id = ?`),
  upsertRoute: db.prepare(`
    INSERT INTO routes (
      encoded_key, account_id, bucket, object_key, backend_key,
      size_bytes, etag, last_modified, content_type,
      uploaded_at, updated_at, deleted_at, metadata_version, route_scope, public_url,
      state, sync_state, reconcile_status,
      backend_last_seen_at, backend_missing_since, last_reconciled_at,
      instance_id
    ) VALUES (
      @encoded_key, @account_id, @bucket, @object_key, @backend_key,
      @size_bytes, @etag, @last_modified, @content_type,
      @uploaded_at, @updated_at, @deleted_at, @metadata_version, @route_scope, @public_url,
      @state, @sync_state, @reconcile_status,
      @backend_last_seen_at, @backend_missing_since, @last_reconciled_at,
      @instance_id
    )
    ON CONFLICT(encoded_key) DO UPDATE SET
      account_id = excluded.account_id,
      bucket = excluded.bucket,
      object_key = excluded.object_key,
      backend_key = excluded.backend_key,
      size_bytes = excluded.size_bytes,
      etag = excluded.etag,
      last_modified = excluded.last_modified,
      content_type = excluded.content_type,
      uploaded_at = excluded.uploaded_at,
      updated_at = excluded.updated_at,
      deleted_at = excluded.deleted_at,
      metadata_version = excluded.metadata_version,
      route_scope = excluded.route_scope,
      public_url = excluded.public_url,
      state = excluded.state,
      sync_state = excluded.sync_state,
      reconcile_status = excluded.reconcile_status,
      backend_last_seen_at = excluded.backend_last_seen_at,
      backend_missing_since = excluded.backend_missing_since,
      last_reconciled_at = excluded.last_reconciled_at,
      instance_id = excluded.instance_id
  `),
  getRoute: db.prepare(`SELECT * FROM routes WHERE encoded_key = ?`),
  getRouteByBackendKey: db.prepare(`
    SELECT *
    FROM routes
    WHERE account_id = @account_id
      AND backend_key = @backend_key
    ORDER BY updated_at DESC
    LIMIT 1
  `),
  deleteRoute: db.prepare(`DELETE FROM routes WHERE encoded_key = ?`),
  getAllRoutes: db.prepare(`SELECT * FROM routes ORDER BY updated_at DESC, uploaded_at DESC`),
  countRoutesAll: db.prepare(`SELECT COUNT(*) AS count FROM routes`),
  countRoutesVisible: db.prepare(`
    SELECT COUNT(*) AS count
    FROM routes
    WHERE route_scope = 'main'
      AND state = 'ACTIVE'
      AND deleted_at IS NULL
  `),
  listBucketsFromVisibleRoutes: db.prepare(`
    SELECT
      bucket,
      MIN(uploaded_at) AS first_uploaded_at,
      MAX(COALESCE(updated_at, uploaded_at)) AS last_updated_at
    FROM routes
    WHERE route_scope = 'main'
      AND state = 'ACTIVE'
      AND deleted_at IS NULL
      AND COALESCE(bucket, '') != ''
    GROUP BY bucket
  `),
  listVisibleObjectsPage: db.prepare(`
    SELECT *
    FROM routes
    WHERE route_scope = 'main'
      AND bucket = @bucket
      AND state = 'ACTIVE'
      AND deleted_at IS NULL
      AND object_key >= @lower_bound
    ORDER BY object_key ASC, encoded_key ASC
    LIMIT @limit
  `),
  listRoutesByBucket: db.prepare(`
    SELECT *
    FROM routes
    WHERE route_scope = 'main'
      AND bucket = @bucket
      AND object_key LIKE @prefix || '%'
    ORDER BY object_key ASC, encoded_key ASC
  `),
  listPublicRoutes: db.prepare(`
    SELECT *
    FROM routes
    WHERE route_scope = 'public'
      AND state = 'ACTIVE'
      AND deleted_at IS NULL
      AND object_key LIKE @prefix || '%'
    ORDER BY object_key ASC, encoded_key ASC
  `),
  getTrackedRoutesByAccount: db.prepare(`
    SELECT *
    FROM routes
    WHERE account_id = @account_id
      AND state != 'DELETED'
    ORDER BY object_key ASC, encoded_key ASC
  `),
  getPendingSyncRoutes: db.prepare(`
    SELECT *
    FROM routes
    WHERE sync_state = 'PENDING_SYNC'
    ORDER BY updated_at ASC, encoded_key ASC
    LIMIT @limit
  `),
  markRouteSynced: db.prepare(`
    UPDATE routes
    SET sync_state = 'SYNCED',
        updated_at = @updated_at
    WHERE encoded_key = @encoded_key
  `),
  updateRouteState: db.prepare(`
    UPDATE routes
    SET state = @state,
        updated_at = @updated_at,
        deleted_at = @deleted_at,
        metadata_version = @metadata_version,
        sync_state = @sync_state,
        reconcile_status = @reconcile_status,
        backend_missing_since = @backend_missing_since,
        last_reconciled_at = @last_reconciled_at
    WHERE encoded_key = @encoded_key
  `),
  upsertMultipartUpload: db.prepare(`
    INSERT OR REPLACE INTO multipart_uploads
      (upload_id, account_id, bucket, object_key, backend_key, started_at)
    VALUES
      (@upload_id, @account_id, @bucket, @object_key, @backend_key, @started_at)
  `),
  getMultipartUpload: db.prepare(`SELECT * FROM multipart_uploads WHERE upload_id = ?`),
  deleteMultipartUpload: db.prepare(`DELETE FROM multipart_uploads WHERE upload_id = ?`),
  activeObjectStatsByBucket: db.prepare(`
    SELECT bucket, COUNT(*) AS object_count, COALESCE(SUM(size_bytes), 0) AS total_bytes
    FROM routes
    WHERE route_scope = 'main'
      AND state = 'ACTIVE'
      AND deleted_at IS NULL
    GROUP BY bucket
  `),
  logicalBytesByBucketAccount: db.prepare(`
    SELECT bucket, account_id, COALESCE(SUM(size_bytes), 0) AS total_bytes
    FROM routes
    WHERE route_scope = 'main'
      AND state = 'ACTIVE'
      AND deleted_at IS NULL
    GROUP BY bucket, account_id
  `),
  stateCountsByAccount: db.prepare(`
    SELECT account_id, state, COUNT(*) AS object_count
    FROM routes
    GROUP BY account_id, state
  `),
}

export function upsertAccount(account) {
  stmts.upsertAccount.run({
    account_id: account.account_id,
    access_key_id: account.access_key_id,
    secret_key: account.secret_key,
    endpoint: account.endpoint,
    region: account.region,
    bucket: account.bucket,
    addressing_style: account.addressing_style ?? 'path',
    payload_signing_mode: account.payload_signing_mode ?? 'unsigned',
    email_owner: account.email_owner ?? '',
    supabase_access_token: account.supabase_access_token ?? '',
    supabase_access_token_exp: account.supabase_access_token_exp ?? null,
    public_bucket: account.public_bucket ? 1 : 0,
    quota_bytes: account.quota_bytes ?? 5_368_709_120,
    used_bytes: account.used_bytes ?? 0,
    active: account.active ?? 1,
    added_at: account.added_at ?? Date.now(),
  })
}

export function getAllAccounts() {
  return stmts.getAllAccounts.all()
}

export function getAllActiveAccounts() {
  return stmts.getAllActiveAccounts.all()
}

export function getAccountById(accountId) {
  return stmts.getAccountById.get(accountId)
}

export function deleteAccount(accountId) {
  stmts.deleteAccountById.run(accountId)
}

export function setUsedBytesAbsolute(accountId, bytes) {
  stmts.setUsedBytesAbsolute.run({ account_id: accountId, bytes })
}

export function upsertCronJob(job) {
  const now = Date.now()
  const existing = stmts.getCronJobById.get(job.job_id)
  stmts.upsertCronJob.run({
    job_id: job.job_id,
    name: job.name,
    kind: job.kind,
    expression: job.expression,
    timezone: job.timezone ?? 'UTC',
    enabled: job.enabled ? 1 : 0,
    payload_json: typeof job.payload_json === 'string'
      ? job.payload_json
      : JSON.stringify(job.payload_json ?? {}),
    source: job.source ?? existing?.source ?? 'user',
    created_at: existing?.created_at ?? now,
    updated_at: now,
  })
  return stmts.getCronJobById.get(job.job_id)
}

export function getCronJobById(jobId) {
  return stmts.getCronJobById.get(jobId)
}

export function getAllCronJobs() {
  return stmts.getAllCronJobs.all()
}

export function deleteCronJob(jobId) {
  stmts.deleteCronJob.run(jobId)
}

export function getBucket(bucket) {
  return stmts.getBucket.get(bucket)
}

export function getAllBuckets() {
  return stmts.getAllBuckets.all()
}

export function listActiveBuckets() {
  return stmts.listActiveBuckets.all()
}

export const ensureBucketActive = db.transaction((bucket, now = Date.now()) => {
  const existing = stmts.getBucket.get(bucket)

  if (!existing) {
    stmts.upsertBucket.run({
      bucket,
      created_at: now,
      updated_at: now,
      deleted_at: null,
      versioning_status: BUCKET_VERSIONING_STATUS.NONE,
    })
    return { bucket: stmts.getBucket.get(bucket), existed: false }
  }

  if (existing.deleted_at !== null) {
    stmts.upsertBucket.run({
      bucket,
      created_at: existing.created_at ?? now,
      updated_at: now,
      deleted_at: null,
      versioning_status: existing.versioning_status ?? BUCKET_VERSIONING_STATUS.NONE,
    })
    return { bucket: stmts.getBucket.get(bucket), existed: false }
  }

  stmts.upsertBucket.run({
    bucket,
    created_at: existing.created_at ?? now,
    updated_at: now,
    deleted_at: null,
    versioning_status: existing.versioning_status ?? BUCKET_VERSIONING_STATUS.NONE,
  })
  return { bucket: stmts.getBucket.get(bucket), existed: true }
})

export const setBucketVersioningStatus = db.transaction((bucket, status, now = Date.now()) => {
  const normalizedStatus = [BUCKET_VERSIONING_STATUS.ENABLED, BUCKET_VERSIONING_STATUS.SUSPENDED]
    .includes(status)
    ? status
    : BUCKET_VERSIONING_STATUS.NONE

  const existing = stmts.getBucket.get(bucket)
  if (!existing || existing.deleted_at !== null) {
    return null
  }

  stmts.upsertBucket.run({
    bucket,
    created_at: existing.created_at ?? now,
    updated_at: now,
    deleted_at: null,
    versioning_status: normalizedStatus,
  })

  return stmts.getBucket.get(bucket)
})

export const markBucketDeleted = db.transaction((bucket, now = Date.now()) => {
  const existing = stmts.getBucket.get(bucket)
  if (!existing || existing.deleted_at !== null) return null

  stmts.upsertBucket.run({
    bucket,
    created_at: existing.created_at ?? now,
    updated_at: now,
    deleted_at: now,
    versioning_status: existing.versioning_status ?? BUCKET_VERSIONING_STATUS.NONE,
  })

  return stmts.getBucket.get(bucket)
})

export function upsertRoute(route) {
  const existing = stmts.getRoute.get(route.encoded_key)
  const normalized = normalizeRouteForWrite(route, existing)
  stmts.upsertRoute.run(normalized)

  if (normalized.route_scope === ROUTE_SCOPE.MAIN
      && normalized.bucket
      && normalized.state === ROUTE_STATE.ACTIVE
      && (normalized.deleted_at === null || normalized.deleted_at === undefined)) {
    ensureBucketActive(normalized.bucket, normalized.updated_at ?? Date.now())
  }
}

export function getRoute(encodedKey) {
  return stmts.getRoute.get(encodedKey)
}

export function getRouteByBackendKey(accountId, backendKey) {
  return stmts.getRouteByBackendKey.get({ account_id: accountId, backend_key: backendKey })
}

export function deleteRoute(encodedKey) {
  stmts.deleteRoute.run(encodedKey)
}

export function getAllRoutes() {
  return stmts.getAllRoutes.all()
}

export function listRoutesByBucket(bucket, prefix = '') {
  return stmts.listRoutesByBucket.all({ bucket, prefix })
}

export function listPublicRoutes(prefix = '') {
  return stmts.listPublicRoutes.all({ prefix })
}

export function listVisibleObjectsPage(bucket, { lowerBound = '', limit = 1000 } = {}) {
  return stmts.listVisibleObjectsPage.all({
    bucket,
    lower_bound: lowerBound,
    limit: Math.max(1, Math.min(Number(limit) || 1000, 5000)),
  })
}

export function getTrackedRoutesByAccount(accountId) {
  return stmts.getTrackedRoutesByAccount.all({ account_id: accountId })
}

export function countRoutes(options = {}) {
  if (options.visibleOnly) {
    return stmts.countRoutesVisible.get().count
  }
  return stmts.countRoutesAll.get().count
}

export function getPendingSyncRoutes(limit = 500) {
  return stmts.getPendingSyncRoutes.all({ limit: Math.max(1, Math.min(Number(limit) || 500, 5000)) })
}

export const ensureBucketsFromActiveRoutes = db.transaction((now = Date.now()) => {
  const rows = stmts.listBucketsFromVisibleRoutes.all()
  let created = 0
  let revived = 0
  let alreadyActive = 0

  for (const row of rows) {
    const bucket = row?.bucket
    if (!bucket) continue

    const existing = stmts.getBucket.get(bucket)
    const bucketNow = Number(row?.last_updated_at) || now
    const result = ensureBucketActive(bucket, bucketNow)

    if (!existing) {
      created++
      continue
    }

    if (existing.deleted_at !== null && existing.deleted_at !== undefined) {
      revived++
      continue
    }

    if (result?.existed) alreadyActive++
  }

  return {
    scanned: rows.length,
    created,
    revived,
    alreadyActive,
  }
})

export function markRouteSynced(encodedKey, updatedAt = Date.now()) {
  stmts.markRouteSynced.run({ encoded_key: encodedKey, updated_at: updatedAt })
}

function withVersion(existing, nextUpdatedAt = Date.now()) {
  return {
    metadataVersion: (existing?.metadata_version ?? 0) + 1,
    updatedAt: nextUpdatedAt,
  }
}

export const commitUploadedObjectMetadata = db.transaction((route) => {
  const existing = stmts.getRoute.get(route.encoded_key)
  const now = Date.now()
  const routeScope = route.route_scope ?? existing?.route_scope ?? ROUTE_SCOPE.MAIN

  if (routeScope === ROUTE_SCOPE.MAIN) {
    ensureBucketActive(route.bucket, now)
  }

  const { metadataVersion, updatedAt } = withVersion(existing, now)
  const normalized = normalizeRouteForWrite({
    ...route,
    route_scope: routeScope,
    deleted_at: null,
    state: ROUTE_STATE.ACTIVE,
    sync_state: ROUTE_SYNC_STATE.PENDING_SYNC,
    reconcile_status: ROUTE_RECONCILE_STATUS.HEALTHY,
    metadata_version: metadataVersion,
    updated_at: updatedAt,
    uploaded_at: route.uploaded_at ?? now,
    backend_last_seen_at: route.backend_last_seen_at ?? now,
    backend_missing_since: null,
    last_reconciled_at: route.last_reconciled_at ?? now,
  }, existing)

  const deltas = new Map()
  if (isUsageCounted(existing)) {
    deltas.set(existing.account_id, (deltas.get(existing.account_id) ?? 0) - existing.size_bytes)
  }
  if (isUsageCounted(normalized)) {
    deltas.set(normalized.account_id, (deltas.get(normalized.account_id) ?? 0) + normalized.size_bytes)
  }

  stmts.upsertRoute.run(normalized)
  applyAccountAdjustments(deltas)

  return {
    route: stmts.getRoute.get(normalized.encoded_key),
    previous: existing ?? null,
    affectedAccounts: fetchAffectedAccounts([...deltas.keys()]),
  }
})

export const markRouteDeleting = db.transaction((encodedKey, now = Date.now()) => {
  const existing = stmts.getRoute.get(encodedKey)
  if (!existing || existing.state === ROUTE_STATE.DELETED) {
    return existing ?? null
  }

  const { metadataVersion, updatedAt } = withVersion(existing, now)
  stmts.updateRouteState.run({
    encoded_key: encodedKey,
    state: ROUTE_STATE.DELETING,
    updated_at: updatedAt,
    deleted_at: null,
    metadata_version: metadataVersion,
    sync_state: ROUTE_SYNC_STATE.PENDING_SYNC,
    reconcile_status: ROUTE_RECONCILE_STATUS.HEALTHY,
    backend_missing_since: null,
    last_reconciled_at: now,
  })

  return stmts.getRoute.get(encodedKey)
})

export const revertDeletingRoute = db.transaction((encodedKey, now = Date.now()) => {
  const existing = stmts.getRoute.get(encodedKey)
  if (!existing || existing.state !== ROUTE_STATE.DELETING) {
    return existing ?? null
  }

  const { metadataVersion, updatedAt } = withVersion(existing, now)
  stmts.updateRouteState.run({
    encoded_key: encodedKey,
    state: ROUTE_STATE.ACTIVE,
    updated_at: updatedAt,
    deleted_at: null,
    metadata_version: metadataVersion,
    sync_state: ROUTE_SYNC_STATE.PENDING_SYNC,
    reconcile_status: ROUTE_RECONCILE_STATUS.HEALTHY,
    backend_missing_since: null,
    last_reconciled_at: now,
  })

  return stmts.getRoute.get(encodedKey)
})

export const finalizeRouteDelete = db.transaction((encodedKey, now = Date.now(), options = {}) => {
  const existing = stmts.getRoute.get(encodedKey)
  if (!existing) {
    return { route: null, previous: null, affectedAccounts: [] }
  }

  const deltas = new Map()
  if (isUsageCounted(existing)) {
    deltas.set(existing.account_id, (deltas.get(existing.account_id) ?? 0) - existing.size_bytes)
  }

  const { metadataVersion, updatedAt } = withVersion(existing, now)
  stmts.updateRouteState.run({
    encoded_key: encodedKey,
    state: ROUTE_STATE.DELETED,
    updated_at: updatedAt,
    deleted_at: now,
    metadata_version: metadataVersion,
    sync_state: ROUTE_SYNC_STATE.PENDING_SYNC,
    reconcile_status: options.reconcileStatus ?? ROUTE_RECONCILE_STATUS.HEALTHY,
    backend_missing_since: options.backendMissing ? (existing.backend_missing_since ?? now) : null,
    last_reconciled_at: now,
  })

  applyAccountAdjustments(deltas)

  return {
    route: stmts.getRoute.get(encodedKey),
    previous: existing,
    affectedAccounts: fetchAffectedAccounts([...deltas.keys()]),
  }
})

export const markRouteMissingBackend = db.transaction((encodedKey, now = Date.now()) => {
  const existing = stmts.getRoute.get(encodedKey)
  if (!existing || existing.state === ROUTE_STATE.DELETED) {
    return { route: existing ?? null, previous: existing ?? null, affectedAccounts: [] }
  }

  const deltas = new Map()
  if (isUsageCounted(existing) && existing.state !== ROUTE_STATE.MISSING_BACKEND) {
    deltas.set(existing.account_id, (deltas.get(existing.account_id) ?? 0) - existing.size_bytes)
  }

  const { metadataVersion, updatedAt } = withVersion(existing, now)
  stmts.updateRouteState.run({
    encoded_key: encodedKey,
    state: ROUTE_STATE.MISSING_BACKEND,
    updated_at: updatedAt,
    deleted_at: null,
    metadata_version: metadataVersion,
    sync_state: ROUTE_SYNC_STATE.PENDING_SYNC,
    reconcile_status: ROUTE_RECONCILE_STATUS.NEEDS_REVIEW,
    backend_missing_since: existing.backend_missing_since ?? now,
    last_reconciled_at: now,
  })

  applyAccountAdjustments(deltas)

  return {
    route: stmts.getRoute.get(encodedKey),
    previous: existing,
    affectedAccounts: fetchAffectedAccounts([...deltas.keys()]),
  }
})

export const upsertReconciledRoute = db.transaction((route) => {
  const existing = stmts.getRoute.get(route.encoded_key)
  const now = Date.now()
  const { metadataVersion, updatedAt } = withVersion(existing, now)
  const normalized = normalizeRouteForWrite({
    ...route,
    metadata_version: route.metadata_version ?? metadataVersion,
    updated_at: route.updated_at ?? updatedAt,
    sync_state: route.sync_state ?? ROUTE_SYNC_STATE.PENDING_SYNC,
    last_reconciled_at: route.last_reconciled_at ?? now,
  }, existing)

  stmts.upsertRoute.run(normalized)
  return stmts.getRoute.get(normalized.encoded_key)
})

export const upsertBucketRecord = db.transaction((bucketRow) => {
  const now = Date.now()
  const existing = stmts.getBucket.get(bucketRow.bucket)
  stmts.upsertBucket.run({
    bucket: bucketRow.bucket,
    created_at: toTimestamp(bucketRow.created_at, existing?.created_at ?? now),
    updated_at: toTimestamp(bucketRow.updated_at, now),
    deleted_at: toNullableTimestamp(bucketRow.deleted_at ?? existing?.deleted_at ?? null),
    versioning_status: bucketRow.versioning_status ?? existing?.versioning_status ?? BUCKET_VERSIONING_STATUS.NONE,
  })

  return stmts.getBucket.get(bucketRow.bucket)
})

export function upsertMultipartUpload(upload) {
  stmts.upsertMultipartUpload.run({
    upload_id: upload.upload_id,
    account_id: upload.account_id,
    bucket: upload.bucket,
    object_key: upload.object_key,
    backend_key: upload.backend_key ?? upload.object_key,
    started_at: upload.started_at ?? Date.now(),
  })
}

export function getMultipartUpload(uploadId) {
  return stmts.getMultipartUpload.get(uploadId)
}

export function deleteMultipartUpload(uploadId) {
  stmts.deleteMultipartUpload.run(uploadId)
}

export function deactivateMissingAccounts(accountIds) {
  const ids = [...new Set(accountIds)]

  if (ids.length === 0) {
    return
  }

  const placeholders = ids.map(() => '?').join(', ')
  db.prepare(`UPDATE accounts SET active = 0 WHERE account_id NOT IN (${placeholders})`).run(...ids)
}

export function getActiveObjectStatsByBucket() {
  return stmts.activeObjectStatsByBucket.all()
}

export function getLogicalBytesByBucketAccount() {
  return stmts.logicalBytesByBucketAccount.all()
}

export function getRouteStateCountsByAccount() {
  return stmts.stateCountsByAccount.all()
}
