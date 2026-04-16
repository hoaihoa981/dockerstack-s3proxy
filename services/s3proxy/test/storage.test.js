/**
 * test/storage.test.js
 * Storage-layer verification for metadata control plane behavior.
 */

import { mkdirSync, existsSync, unlinkSync } from 'fs'

process.env.PROXY_API_KEY = process.env.PROXY_API_KEY || 'test'
process.env.FIREBASE_RTDB_URL = process.env.FIREBASE_RTDB_URL || 'https://dummy.firebaseio.com'
process.env.FIREBASE_DB_SECRET = process.env.FIREBASE_DB_SECRET || 'dummy'

const TEST_DB_DIR = '../../.docker-volumes/s3proxy-data'
const TEST_DB = `${TEST_DB_DIR}/test-t3-storage.db`
process.env.SQLITE_PATH = TEST_DB
mkdirSync(TEST_DB_DIR, { recursive: true })
if (existsSync(TEST_DB)) unlinkSync(TEST_DB)

let passed = 0
let failed = 0
const ok = (label) => { console.log(`✅ ${label}`); passed++ }
const fail = (label, err) => { console.error(`❌ ${label}\n   ${err?.message || err}`); failed++ }

const MB = 1024 * 1024
const GB = 1024 * MB

const {
  db,
  commitUploadedObjectMetadata,
  ensureBucketsFromActiveRoutes,
  finalizeRouteDelete,
  getAllActiveAccounts,
  getAllRoutes,
  getBucket,
  getRoute,
  listVisibleObjectsPage,
  markRouteMissingBackend,
  ROUTE_STATE,
  setUsedBytesAbsolute,
  upsertAccount,
  upsertRoute,
  deleteRoute,
  countRoutes,
  deactivateMissingAccounts,
} = await import('../src/db.js')
const { cacheGet, cacheSet, cacheDelete, cacheClear } = await import('../src/cache.js')
const {
  selectAccountForUpload,
  StorageFullError,
  reloadAccountsFromSQLite,
  syncAccountsFromRows,
} = await import('../src/accountPool.js')
const { buildBackendKey } = await import('../src/metadata.js')

function seedAccounts() {
  upsertAccount({
    account_id: 'acc1',
    access_key_id: 'k1',
    secret_key: 's1',
    endpoint: 'https://a1.supabase.co/storage/v1/s3',
    region: 'auto',
    bucket: 'b1',
    quota_bytes: 5 * GB,
    used_bytes: 0,
    active: 1,
    added_at: Date.now(),
  })
  upsertAccount({
    account_id: 'acc2',
    access_key_id: 'k2',
    secret_key: 's2',
    endpoint: 'https://a2.supabase.co/storage/v1/s3',
    region: 'auto',
    bucket: 'b2',
    quota_bytes: 5 * GB,
    used_bytes: 4600 * MB,
    active: 1,
    added_at: Date.now(),
  })
  reloadAccountsFromSQLite()
}

async function testUpsertAndOrder() {
  try {
    seedAccounts()
    const accounts = getAllActiveAccounts()
    if (accounts.length === 2 && accounts[0].account_id === 'acc1' && accounts[1].account_id === 'acc2') {
      ok('upsertAccount + getAllActiveAccounts tra dung thu tu used_bytes ASC')
    } else {
      fail('upsertAccount + getAllActiveAccounts', new Error(JSON.stringify(accounts.map((account) => account.account_id))))
    }
  } catch (err) {
    fail('upsertAccount + getAllActiveAccounts', err)
  }
}

async function testSelectAccount() {
  try {
    reloadAccountsFromSQLite()
    const selected = selectAccountForUpload(100 * MB)
    if (selected.account_id === 'acc1') {
      ok('selectAccountForUpload(100MB) -> acc1 (acc2 vuot threshold 90%)')
    } else {
      fail('selectAccountForUpload', new Error(`Got: ${selected.account_id}`))
    }
  } catch (err) {
    fail('selectAccountForUpload', err)
  }
}

async function testSelectAccountByPublicBucket() {
  try {
    upsertAccount({
      account_id: 'acc-public',
      access_key_id: 'kp',
      secret_key: 'sp',
      endpoint: 'https://public.supabase.co/storage/v1/s3',
      region: 'auto',
      bucket: 'public-bucket',
      public_bucket: 1,
      quota_bytes: 5 * GB,
      used_bytes: 10 * MB,
      active: 1,
      added_at: Date.now(),
    })
    reloadAccountsFromSQLite()

    const privateSelected = selectAccountForUpload(1 * MB)
    const publicSelected = selectAccountForUpload(1 * MB, { publicBucket: true })

    if (privateSelected.account_id === 'acc1' && publicSelected.account_id === 'acc-public') {
      ok('selectAccountForUpload tach rieng private/public backend dung theo publicBucket')
    } else {
      fail('selectAccountForUpload public/private separation', new Error(`private=${privateSelected.account_id} public=${publicSelected.account_id}`))
    }
  } catch (err) {
    fail('selectAccountForUpload public/private separation', err)
  }
}

async function testCommitUploadMetadata() {
  try {
    const committed = commitUploadedObjectMetadata({
      encoded_key: 'dGVzdC9sb2dpY2FsL2EudHh0',
      account_id: 'acc1',
      bucket: 'test',
      object_key: 'logical/a.txt',
      backend_key: buildBackendKey('test', 'logical/a.txt'),
      size_bytes: 100 * MB,
      etag: 'etag-a',
      last_modified: Date.now(),
      content_type: 'text/plain',
      uploaded_at: Date.now(),
      updated_at: Date.now(),
      instance_id: 'test',
    })
    syncAccountsFromRows(committed.affectedAccounts)

    const route = getRoute('dGVzdC9sb2dpY2FsL2EudHh0')
    const account = getAllActiveAccounts().find((entry) => entry.account_id === 'acc1')

    if (!route || route.state !== ROUTE_STATE.ACTIVE) {
      throw new Error(`route=${JSON.stringify(route)}`)
    }
    if (route.backend_key !== 'test/logical/a.txt') {
      throw new Error(`backend_key=${route.backend_key}`)
    }
    if (account.used_bytes !== 100 * MB) {
      throw new Error(`used_bytes=${account.used_bytes}`)
    }

    ok('commitUploadedObjectMetadata luu route ACTIVE va cap nhat used_bytes giao dich')
  } catch (err) {
    fail('commitUploadedObjectMetadata', err)
  }
}

async function testUpsertRouteAutoCreatesBucket() {
  try {
    const bucket = 'auto-bucket'
    const objectKey = 'seed/from-rtdb.txt'
    const now = Date.now()

    upsertRoute({
      encoded_key: Buffer.from(`${bucket}/${objectKey}`).toString('base64url'),
      account_id: 'acc1',
      bucket,
      object_key: objectKey,
      backend_key: buildBackendKey(bucket, objectKey),
      size_bytes: 1,
      uploaded_at: now,
      updated_at: now,
      state: ROUTE_STATE.ACTIVE,
      deleted_at: null,
      instance_id: 'test',
    })

    const bucketRecord = getBucket(bucket)
    if (!bucketRecord || bucketRecord.deleted_at !== null) {
      throw new Error(`bucketRecord=${JSON.stringify(bucketRecord)}`)
    }

    ok('upsertRoute ACTIVE tu dong tao record bucket de HEAD /:bucket khong bi 404')
  } catch (err) {
    fail('upsertRoute ACTIVE auto-create bucket', err)
  }
}

async function testEnsureBucketsFromActiveRoutesRepairsMissingBuckets() {
  try {
    const bucket = 'repair-bucket'
    const objectKey = 'repair/path.txt'
    const encoded = Buffer.from(`${bucket}/${objectKey}`).toString('base64url')
    const now = Date.now()

    commitUploadedObjectMetadata({
      encoded_key: encoded,
      account_id: 'acc1',
      bucket,
      object_key: objectKey,
      backend_key: buildBackendKey(bucket, objectKey),
      size_bytes: 32,
      uploaded_at: now,
      updated_at: now,
      instance_id: 'test',
    })

    db.prepare('DELETE FROM buckets WHERE bucket = ?').run(bucket)
    if (getBucket(bucket)) {
      throw new Error('bucket should be removed before repair')
    }

    const repaired = ensureBucketsFromActiveRoutes(Date.now())
    const bucketRecord = getBucket(bucket)

    if (!bucketRecord || bucketRecord.deleted_at !== null) {
      throw new Error(`bucketRecord=${JSON.stringify(bucketRecord)} repaired=${JSON.stringify(repaired)}`)
    }
    if ((repaired.created + repaired.revived) < 1) {
      throw new Error(`repair summary=${JSON.stringify(repaired)}`)
    }

    ok('ensureBucketsFromActiveRoutes tu phuc hoi bucket record bi drift')
  } catch (err) {
    fail('ensureBucketsFromActiveRoutes repair', err)
  }
}

async function testVisibleListAndTombstone() {
  try {
    commitUploadedObjectMetadata({
      encoded_key: 'dGVzdC9sb2dpY2FsL2IudHh0',
      account_id: 'acc1',
      bucket: 'test',
      object_key: 'logical/b.txt',
      backend_key: buildBackendKey('test', 'logical/b.txt'),
      size_bytes: 50 * MB,
      etag: 'etag-b',
      last_modified: Date.now(),
      uploaded_at: Date.now(),
      updated_at: Date.now(),
      instance_id: 'test',
    })

    const deleted = finalizeRouteDelete('dGVzdC9sb2dpY2FsL2IudHh0', Date.now())
    syncAccountsFromRows(deleted.affectedAccounts)

    const visible = listVisibleObjectsPage('test', { lowerBound: '', limit: 10 })
    const deletedRoute = getRoute('dGVzdC9sb2dpY2FsL2IudHh0')

    if (visible.some((route) => route.encoded_key === 'dGVzdC9sb2dpY2FsL2IudHh0')) {
      throw new Error('deleted object still visible in list')
    }
    if (!deletedRoute || deletedRoute.state !== ROUTE_STATE.DELETED || deletedRoute.deleted_at === null) {
      throw new Error(`deletedRoute=${JSON.stringify(deletedRoute)}`)
    }

    ok('listVisibleObjectsPage chi hien object ACTIVE, tombstone bi an khoi list')
  } catch (err) {
    fail('listVisibleObjectsPage / tombstone', err)
  }
}

async function testMissingBackendTransition() {
  try {
    const before = getAllActiveAccounts().find((account) => account.account_id === 'acc1')
    const result = markRouteMissingBackend('dGVzdC9sb2dpY2FsL2EudHh0', Date.now())
    syncAccountsFromRows(result.affectedAccounts)

    const route = getRoute('dGVzdC9sb2dpY2FsL2EudHh0')
    const after = getAllActiveAccounts().find((account) => account.account_id === 'acc1')

    if (route.state !== ROUTE_STATE.MISSING_BACKEND) {
      throw new Error(`state=${route.state}`)
    }
    if (after.used_bytes >= before.used_bytes) {
      throw new Error(`before=${before.used_bytes} after=${after.used_bytes}`)
    }

    ok('markRouteMissingBackend danh dau drift va giam used_bytes da tinh')
  } catch (err) {
    fail('markRouteMissingBackend', err)
  }
}

async function testCacheSetGet() {
  try {
    cacheClear()
    const key = 'dGVzdC9mb28udHh0'
    const value = { accountId: 'acc1', bucket: 'test', objectKey: 'foo.txt', sizeBytes: 1024 }
    cacheSet(key, value)
    const hit = cacheGet(key)
    if (hit && hit.accountId === 'acc1') ok('cacheSet / cacheGet hit')
    else fail('cacheSet / cacheGet', new Error(JSON.stringify(hit)))
  } catch (err) {
    fail('cacheSet / cacheGet', err)
  }
}

async function testCacheDelete() {
  try {
    const key = 'dGVzdC9iYXIudHh0'
    cacheSet(key, { accountId: 'acc1', bucket: 'b', objectKey: 'bar.txt', sizeBytes: 512 })
    cacheDelete(key)
    if (cacheGet(key) === undefined) ok('cacheDelete -> miss')
    else fail('cacheDelete', new Error('still exists'))
  } catch (err) {
    fail('cacheDelete', err)
  }
}

async function testMigrateIdempotent() {
  try {
    db.exec('CREATE TABLE IF NOT EXISTS accounts (account_id TEXT PRIMARY KEY, access_key_id TEXT NOT NULL, secret_key TEXT NOT NULL, endpoint TEXT NOT NULL, region TEXT NOT NULL, bucket TEXT NOT NULL, quota_bytes INTEGER NOT NULL DEFAULT 5368709120, used_bytes INTEGER NOT NULL DEFAULT 0, active INTEGER NOT NULL DEFAULT 1, added_at INTEGER NOT NULL);')
    ok('SQLite migrate chay 2 lan -> khong loi (idempotent)')
  } catch (err) {
    fail('SQLite migrate idempotent', err)
  }
}

async function testStorageFullError() {
  try {
    setUsedBytesAbsolute('acc1', 4600 * MB)
    reloadAccountsFromSQLite()
    let threw = false
    try {
      selectAccountForUpload(100 * MB)
    } catch (err) {
      if (err instanceof StorageFullError) threw = true
      else throw err
    }
    if (threw) ok('selectAccountForUpload khi tat ca full -> throw StorageFullError')
    else fail('StorageFullError', new Error('Did not throw'))
  } catch (err) {
    fail('StorageFullError', err)
  }
}

async function testDeactivateMissingAccountsEmptyInput() {
  try {
    seedAccounts()
    deactivateMissingAccounts([])
    const active = getAllActiveAccounts()
    if (active.length >= 2) {
      ok('deactivateMissingAccounts([]) khong deactivate toan bo accounts')
    } else {
      fail('deactivateMissingAccounts([])', new Error(`active=${active.length}`))
    }
  } catch (err) {
    fail('deactivateMissingAccounts([])', err)
  }
}

async function testLegacyRouteCrud() {
  try {
    const route = {
      encoded_key: 'dGVzdC9zYW1wbGU',
      account_id: 'acc1',
      bucket: 'test',
      object_key: 'sample.txt',
      backend_key: 'test/sample.txt',
      size_bytes: 2048,
      uploaded_at: Date.now(),
      updated_at: Date.now(),
      instance_id: 'test',
    }
    upsertRoute(route)
    const fetched = getRoute(route.encoded_key)
    if (!fetched || fetched.account_id !== 'acc1') throw new Error(`getRoute: ${JSON.stringify(fetched)}`)
    if (countRoutes() < 1) throw new Error('countRoutes=0')
    if (!getAllRoutes().find((row) => row.encoded_key === route.encoded_key)) throw new Error('getAllRoutes miss')
    deleteRoute(route.encoded_key)
    if (getRoute(route.encoded_key) !== undefined) throw new Error('still exists after delete')
    ok('upsertRoute / getRoute / countRoutes / getAllRoutes / deleteRoute - low-level CRUD OK')
  } catch (err) {
    fail('Route CRUD', err)
  }
}

async function main() {
  console.log('─'.repeat(60))
  console.log('T3 - Storage Layer Tests')
  console.log('─'.repeat(60))

  await testUpsertAndOrder()
  await testSelectAccount()
  await testSelectAccountByPublicBucket()
  await testCommitUploadMetadata()
  await testUpsertRouteAutoCreatesBucket()
  await testEnsureBucketsFromActiveRoutesRepairsMissingBuckets()
  await testVisibleListAndTombstone()
  await testMissingBackendTransition()
  await testCacheSetGet()
  await testCacheDelete()
  await testMigrateIdempotent()
  await testStorageFullError()
  await testDeactivateMissingAccountsEmptyInput()
  await testLegacyRouteCrud()

  console.log('─'.repeat(60))
  console.log(`Results: ${passed} passed, ${failed} failed`)

  try {
    db.close()
    for (const file of [TEST_DB, `${TEST_DB}-shm`, `${TEST_DB}-wal`]) {
      if (existsSync(file)) unlinkSync(file)
    }
  } catch {
    // ignore cleanup failure
  }

  process.exit(failed > 0 ? 1 : 0)
}

main()
