/**
 * test/accounts-api.test.js
 * Admin accounts API integration with a local fake RTDB server.
 */

import { createServer } from 'http'
import { mkdirSync, existsSync, unlinkSync } from 'fs'

process.env.PROXY_API_KEY = process.env.PROXY_API_KEY || 'test'
process.env.FIREBASE_DB_SECRET = process.env.FIREBASE_DB_SECRET || 'dummy'
const TEST_DB_DIR = '../../.docker-volumes/s3proxy-data'
process.env.SQLITE_PATH = `${TEST_DB_DIR}/test-accounts-api.db`
process.env.LOG_LEVEL = 'fatal'

const TEST_DB = process.env.SQLITE_PATH
mkdirSync(TEST_DB_DIR, { recursive: true })
for (const file of [TEST_DB, `${TEST_DB}-shm`, `${TEST_DB}-wal`]) {
  if (existsSync(file)) unlinkSync(file)
}

let passed = 0
let failed = 0

function ok(label) {
  console.log(`✅ ${label}`)
  passed++
}

function fail(label, err) {
  console.error(`❌ ${label}`)
  console.error(`   ${err?.message || err}`)
  failed++
}

function assert(condition, message) {
  if (!condition) throw new Error(message)
}

function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value))
}

function decodePath(pathname) {
  const withoutJson = pathname.endsWith('.json')
    ? pathname.slice(0, -5)
    : pathname
  const trimmed = withoutJson.replace(/^\/+|\/+$/g, '')
  return trimmed ? trimmed.split('/') : []
}

function getAtPath(root, parts) {
  let current = root
  for (const part of parts) {
    if (!current || typeof current !== 'object' || !(part in current)) {
      return null
    }
    current = current[part]
  }
  return clone(current)
}

function setAtPath(root, parts, value) {
  if (parts.length === 0) {
    Object.keys(root).forEach((key) => delete root[key])
    if (value && typeof value === 'object') {
      for (const [key, entry] of Object.entries(value)) {
        root[key] = clone(entry)
      }
    }
    return
  }

  let current = root
  for (let i = 0; i < parts.length - 1; i++) {
    const part = parts[i]
    if (!current[part] || typeof current[part] !== 'object') {
      current[part] = {}
    }
    current = current[part]
  }

  const last = parts.at(-1)
  if (value === null) {
    delete current[last]
  } else {
    current[last] = clone(value)
  }
}

function mergeAtPath(root, parts, patch) {
  if (parts.length === 0) {
    for (const [path, value] of Object.entries(patch ?? {})) {
      setAtPath(root, decodePath(path), value)
    }
    return
  }

  const existing = getAtPath(root, parts)
  const next = {
    ...(existing && typeof existing === 'object' ? existing : {}),
    ...(patch && typeof patch === 'object' ? patch : {}),
  }
  setAtPath(root, parts, next)
}

async function readJsonBody(req) {
  const chunks = []
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  }
  const raw = Buffer.concat(chunks).toString('utf8')
  return raw ? JSON.parse(raw) : null
}

async function startFakeRtdb() {
  const state = {
    accounts: {},
    routes: {},
    instances: {},
  }

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1')
    const parts = decodePath(url.pathname)

    try {
      if (req.method === 'GET') {
        const value = getAtPath(state, parts)
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(value))
        return
      }

      if (req.method === 'PUT') {
        const body = await readJsonBody(req)
        setAtPath(state, parts, body)
        const value = getAtPath(state, parts)
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(value))
        return
      }

      if (req.method === 'PATCH') {
        const body = await readJsonBody(req)
        mergeAtPath(state, parts, body)
        const value = parts.length === 0 ? state : getAtPath(state, parts)
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end(JSON.stringify(value))
        return
      }

      if (req.method === 'DELETE') {
        setAtPath(state, parts, null)
        res.statusCode = 200
        res.setHeader('Content-Type', 'application/json')
        res.end('null')
        return
      }

      res.statusCode = 405
      res.end('method not allowed')
    } catch (err) {
      res.statusCode = 500
      res.end(err.message)
    }
  })

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()

  return {
    state,
    url: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve())),
  }
}

const fakeRtdb = await startFakeRtdb()
process.env.FIREBASE_RTDB_URL = fakeRtdb.url

const Fastify = (await import('fastify')).default
const authPlugin = (await import('../src/plugins/auth.js')).default
const errorHandler = (await import('../src/plugins/errorHandler.js')).default
const healthRoutes = (await import('../src/routes/health.js')).default
const metricsRoutes = (await import('../src/routes/metrics.js')).default
const accountRoutes = (await import('../src/routes/accounts.js')).default
const { db, getAllAccounts } = await import('../src/db.js')

async function createApp() {
  const fastify = Fastify({ logger: false })
  fastify.decorate('config', { INSTANCE_ID: 'test-accounts-api' })
  await fastify.register(authPlugin)
  await fastify.register(errorHandler)
  await fastify.register(healthRoutes)
  await fastify.register(metricsRoutes)
  await fastify.register(accountRoutes)
  return fastify
}

async function testSingleAccountImport(fastify) {
  try {
    const res = await fastify.inject({
      method: 'POST',
      url: '/admin/accounts',
      headers: {
        'x-api-key': 'test',
        'content-type': 'application/json',
      },
      payload: {
        accountId: 'acc01',
        accessKeyId: 'key-01',
        secretAccessKey: 'secret-01',
        endpoint: 'https://project-01.supabase.co/storage/v1/s3',
        region: 'ap-southeast-1',
        bucket: 'bucket-01',
        addressingStyle: 'virtual',
        payloadSigningMode: 'signed',
      },
    })

    const body = res.json()
    assert(res.statusCode === 200, `single import status=${res.statusCode}`)
    assert(body.imported === 1, `single import count=${body.imported}`)
    assert(body.rtdbSynced === true, `single import rtdbSynced=${body.rtdbSynced}`)
    assert(body.accounts[0].accountId === 'acc01', `single import accountId=${body.accounts[0].accountId}`)
    assert(body.accounts[0].action === 'created', `single import action=${body.accounts[0].action}`)
    assert(body.accounts[0].hasSecret === true, `single import hasSecret=${body.accounts[0].hasSecret}`)
    assert(body.accounts[0].addressingStyle === 'virtual', `single import addressingStyle=${body.accounts[0].addressingStyle}`)
    assert(body.accounts[0].payloadSigningMode === 'signed', `single import payloadSigningMode=${body.accounts[0].payloadSigningMode}`)
    assert(!('secretAccessKey' in body.accounts[0]), 'single import response leaked secretAccessKey')
    assert(fakeRtdb.state.accounts.acc01?.accountId === 'acc01', 'single import missing accountId in fake RTDB')
    assert(fakeRtdb.state.accounts.acc01?.addressingStyle === 'virtual', 'single import missing addressingStyle in fake RTDB')
    assert(fakeRtdb.state.accounts.acc01?.payloadSigningMode === 'signed', 'single import missing payloadSigningMode in fake RTDB')
    assert(fakeRtdb.state.accounts.acc01?.bucket === 'bucket-01', 'single import missing in fake RTDB')
    ok('POST /admin/accounts -> import 1 account vao SQLite + RTDB va khong tra ve secret')
  } catch (err) {
    fail('POST /admin/accounts single import', err)
  }
}

async function testBulkImportFromMapAndExportShape(fastify) {
  try {
    const bulkRes = await fastify.inject({
      method: 'POST',
      url: '/admin/accounts/import',
      headers: {
        'x-api-key': 'test',
        'content-type': 'application/json',
      },
      payload: {
        accounts: {
          acc02: {
            accessKeyId: 'key-02',
            secretAccessKey: 'secret-02',
            endpoint: 'https://project-02.supabase.co/storage/v1/s3',
            region: 'ap-southeast-1',
            bucket: 'bucket-02',
            quotaBytes: 12345,
          },
          acc03: {
            accessKeyId: 'key-03',
            secretAccessKey: 'secret-03',
            endpoint: 'https://project-03.supabase.co/storage/v1/s3',
            region: 'ap-southeast-1',
            bucket: 'bucket-03',
            active: false,
          },
        },
      },
    })

    const exportRes = await fastify.inject({
      method: 'POST',
      url: '/admin/accounts/import',
      headers: {
        'x-api-key': 'test',
        'content-type': 'application/json',
      },
      payload: {
        accounts: {
          acc04: {
            accessKeyId: 'key-04',
            secretAccessKey: 'secret-04',
            endpoint: 'https://project-04.supabase.co/storage/v1/s3',
            region: 'ap-southeast-1',
            bucket: 'bucket-04',
          },
        },
        routes: {
          ignored: true,
        },
      },
    })

    const bulkBody = bulkRes.json()
    const exportBody = exportRes.json()
    assert(bulkRes.statusCode === 200, `bulk import status=${bulkRes.statusCode}`)
    assert(exportRes.statusCode === 200, `export-shape import status=${exportRes.statusCode}`)
    assert(bulkBody.imported === 2, `bulk import count=${bulkBody.imported}`)
    assert(exportBody.imported === 1, `export-shape import count=${exportBody.imported}`)
    assert(fakeRtdb.state.accounts.acc02?.quotaBytes === 12345, 'bulk import missing acc02 in fake RTDB')
    assert(fakeRtdb.state.accounts.acc03?.active === false, 'bulk import missing active=false in fake RTDB')
    assert(fakeRtdb.state.accounts.acc04?.bucket === 'bucket-04', 'export-shape import missing acc04 in fake RTDB')
    ok('POST /admin/accounts/import -> import nhieu account tu map va dang export RTDB')
  } catch (err) {
    fail('POST /admin/accounts/import bulk import', err)
  }
}

async function testListAccounts(fastify) {
  try {
    const res = await fastify.inject({
      method: 'GET',
      url: '/admin/accounts',
      headers: { 'x-api-key': 'test' },
    })
    const body = res.json()
    const localAccounts = getAllAccounts()

    assert(res.statusCode === 200, `list accounts status=${res.statusCode}`)
    assert(body.total === 4, `list accounts total=${body.total}`)
    assert(body.accounts.length === 4, `list accounts length=${body.accounts.length}`)
    assert(body.accounts.every((account) => !('secret_key' in account) && !('secretAccessKey' in account)), 'list accounts leaked secrets')
    assert(localAccounts.length === 4, `local SQLite accounts=${localAccounts.length}`)
    ok('GET /admin/accounts -> liet ke account da import va khong lo secret')
  } catch (err) {
    fail('GET /admin/accounts', err)
  }
}

async function testInvalidImportValidation(fastify) {
  try {
    const before = getAllAccounts().length
    const res = await fastify.inject({
      method: 'POST',
      url: '/admin/accounts',
      headers: {
        'x-api-key': 'test',
        'content-type': 'application/json',
      },
      payload: {
        accounts: [
          {
            accountId: 'broken',
            accessKeyId: 'broken-key',
            secretAccessKey: 'broken-secret',
          },
          {
            accountId: 'good-after-bad',
            accessKeyId: 'good-key',
            secretAccessKey: 'good-secret',
            endpoint: 'https://good.supabase.co/storage/v1/s3',
            region: 'ap-southeast-1',
            bucket: 'good-bucket',
          },
        ],
      },
    })
    const body = res.json()
    const after = getAllAccounts().length

    assert(res.statusCode === 400, `invalid import status=${res.statusCode}`)
    assert(Array.isArray(body.errors) && body.errors.length > 0, 'invalid import missing errors array')
    assert(after === before, `invalid import changed local account count before=${before} after=${after}`)
    ok('POST /admin/accounts -> payload sai tra 400 va khong ghi du lieu loi')
  } catch (err) {
    fail('POST /admin/accounts invalid payload', err)
  }
}

async function testRealtimeSafeAccountIdValidation(fastify) {
  try {
    const before = getAllAccounts().length

    const singleRes = await fastify.inject({
      method: 'POST',
      url: '/admin/accounts',
      headers: {
        'x-api-key': 'test',
        'content-type': 'application/json',
      },
      payload: {
        accountId: 'supabaseS3-o861.cloudflare',
        accessKeyId: 'key-dot',
        secretAccessKey: 'secret-dot',
        endpoint: 'https://dot.supabase.co/storage/v1/s3',
        region: 'ap-southeast-1',
        bucket: 'dot-bucket',
      },
    })

    const mapRes = await fastify.inject({
      method: 'POST',
      url: '/admin/accounts/import',
      headers: {
        'x-api-key': 'test',
        'content-type': 'application/json',
      },
      payload: {
        accounts: {
          'map.with.dot': {
            accessKeyId: 'key-map-dot',
            secretAccessKey: 'secret-map-dot',
            endpoint: 'https://map-dot.supabase.co/storage/v1/s3',
            region: 'ap-southeast-1',
            bucket: 'map-dot-bucket',
          },
        },
      },
    })

    const singleBody = singleRes.json()
    const mapBody = mapRes.json()
    const after = getAllAccounts().length

    assert(singleRes.statusCode === 400, `single dot-id status=${singleRes.statusCode}`)
    assert(mapRes.statusCode === 400, `map dot-id status=${mapRes.statusCode}`)
    assert(singleBody.errors.some((value) => value.includes('account.accountId')), 'single dot-id missing accountId error')
    assert(singleBody.errors.some((value) => value.includes('suggested: supabaseS3-o861-cloudflare')), 'single dot-id missing suggested normalized id')
    assert(mapBody.errors.some((value) => value.includes('accounts.map.with.dot.accountId')), 'map dot-id missing accountId error')
    assert(mapBody.errors.some((value) => value.includes('suggested: map-with-dot')), 'map dot-id missing suggested normalized id')
    assert(after === before, `dot-id validation changed local account count before=${before} after=${after}`)
    assert(!fakeRtdb.state.accounts['supabaseS3-o861.cloudflare'], 'dot-id unexpectedly written to fake RTDB')
    assert(!fakeRtdb.state.accounts['map.with.dot'], 'map dot-id unexpectedly written to fake RTDB')
    ok('accountId duoc validate theo chuan realtime va chan ky tu khong hop le')
  } catch (err) {
    fail('accountId realtime-safe validation', err)
  }
}

async function main() {
  console.log('─'.repeat(60))
  console.log('T7 - Accounts API Tests')
  console.log('─'.repeat(60))

  const fastify = await createApp()

  try {
    await testSingleAccountImport(fastify)
    await testBulkImportFromMapAndExportShape(fastify)
    await testListAccounts(fastify)
    await testInvalidImportValidation(fastify)
    await testRealtimeSafeAccountIdValidation(fastify)
  } finally {
    await fastify.close().catch(() => {})
    await fakeRtdb.close().catch(() => {})
    db.close()
    for (const file of [TEST_DB, `${TEST_DB}-shm`, `${TEST_DB}-wal`]) {
      if (existsSync(file)) unlinkSync(file)
    }
  }

  console.log('─'.repeat(60))
  console.log(`Results: ${passed} passed, ${failed} failed`)
  process.exit(failed > 0 ? 1 : 0)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
