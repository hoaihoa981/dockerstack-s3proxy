/**
 * test/public-bucket-proxy.test.js
 * Public bucket proxy integration tests.
 */

import { createServer } from 'http'
import { mkdirSync, existsSync, unlinkSync } from 'fs'

process.env.PROXY_API_KEY = process.env.PROXY_API_KEY || 'test'
process.env.FIREBASE_RTDB_URL = process.env.FIREBASE_RTDB_URL || 'https://dummy.firebaseio.com'
process.env.FIREBASE_DB_SECRET = process.env.FIREBASE_DB_SECRET || 'dummy'
const TEST_DB_DIR = '../../.docker-volumes/s3proxy-data'
process.env.SQLITE_PATH = `${TEST_DB_DIR}/test-public-bucket-proxy.db`
process.env.LOG_LEVEL = 'fatal'

const TEST_DB = process.env.SQLITE_PATH
mkdirSync(TEST_DB_DIR, { recursive: true })
for (const file of [TEST_DB, `${TEST_DB}-shm`, `${TEST_DB}-wal`]) {
  if (existsSync(file)) unlinkSync(file)
}

const Fastify = (await import('fastify')).default
const authPlugin = (await import('../src/plugins/auth.js')).default
const errorHandler = (await import('../src/plugins/errorHandler.js')).default
const metricsRoutes = (await import('../src/routes/metrics.js')).default
const publicBucketProxyRoutes = (await import('../src/routes/publicBucketProxy.js')).default
const s3Routes = (await import('../src/routes/s3.js')).default
const { db, getRoute, ROUTE_SCOPE, ROUTE_STATE, upsertAccount } = await import('../src/db.js')
const { PUBLIC_PROXY_BUCKET } = await import('../src/metadata.js')
const { reloadAccountsFromSQLite } = await import('../src/accountPool.js')

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
  if (!condition) {
    throw new Error(message)
  }
}

function encodedKey(bucket, objectKey) {
  return Buffer.from(`${bucket}/${objectKey}`).toString('base64url')
}

async function readBody(req) {
  const chunks = []
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  }
  return Buffer.concat(chunks)
}

async function startFakeS3() {
  const objects = new Map()

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1')
    const parts = url.pathname.split('/').filter(Boolean)
    const bucket = parts[0] || ''
    const key = parts.slice(1).join('/')
    const objectId = `${bucket}/${key}`

    if (req.method === 'PUT') {
      const body = await readBody(req)
      objects.set(objectId, {
        body,
        contentType: req.headers['content-type'] || 'application/octet-stream',
        lastModified: new Date().toUTCString(),
      })
      res.statusCode = 200
      res.setHeader('ETag', '"put-etag"')
      res.end('')
      return
    }

    if (req.method === 'GET' && key === '') {
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/xml')
      res.end('<?xml version="1.0" encoding="UTF-8"?><ListBucketResult></ListBucketResult>')
      return
    }

    if (req.method === 'GET') {
      const object = objects.get(objectId)
      if (!object) {
        res.statusCode = 404
        res.end('missing object')
        return
      }
      res.statusCode = 200
      res.setHeader('Content-Type', object.contentType)
      res.setHeader('Content-Length', object.body.length)
      res.setHeader('Last-Modified', object.lastModified)
      res.setHeader('ETag', '"get-etag"')
      res.end(object.body)
      return
    }

    if (req.method === 'HEAD') {
      const object = objects.get(objectId)
      if (!object) {
        res.statusCode = 404
        res.end('')
        return
      }
      res.statusCode = 200
      res.setHeader('Content-Type', object.contentType)
      res.setHeader('Content-Length', object.body.length)
      res.setHeader('Last-Modified', object.lastModified)
      res.setHeader('ETag', '"head-etag"')
      res.end('')
      return
    }

    if (req.method === 'DELETE') {
      objects.delete(objectId)
      res.statusCode = 204
      res.end('')
      return
    }

    res.statusCode = 405
    res.end('method not allowed')
  })

  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()

  return {
    endpoint: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolve, reject) => server.close((err) => err ? reject(err) : resolve())),
    hasObject(bucket, key) {
      return objects.has(`${bucket}/${key}`)
    },
  }
}

async function main() {
  console.log('─'.repeat(60))
  console.log('T8 - Public Bucket Proxy Tests')
  console.log('─'.repeat(60))

  const privateUpstream = await startFakeS3()
  const publicUpstream = await startFakeS3()
  const fastify = Fastify({ logger: false })

  try {
    upsertAccount({
      account_id: 'acc-private',
      access_key_id: 'key-private',
      secret_key: 'secret-private',
      endpoint: privateUpstream.endpoint,
      region: 'ap-southeast-1',
      bucket: 'private-physical',
      public_bucket: 0,
      quota_bytes: 5_000_000,
      used_bytes: 0,
      active: 1,
      added_at: Date.now(),
    })
    upsertAccount({
      account_id: 'acc-public',
      access_key_id: 'key-public',
      secret_key: 'secret-public',
      endpoint: publicUpstream.endpoint,
      region: 'ap-southeast-1',
      bucket: 'public-physical',
      public_bucket: 1,
      quota_bytes: 5_000_000,
      used_bytes: 0,
      active: 1,
      added_at: Date.now(),
    })
    reloadAccountsFromSQLite()

    fastify.decorate('config', { INSTANCE_ID: 'test-public-bucket-proxy' })
    await fastify.register(authPlugin)
    await fastify.register(errorHandler)
    await fastify.register(metricsRoutes)
    await fastify.register(publicBucketProxyRoutes)
    await fastify.register(s3Routes, { prefix: '/' })

    try {
      await fastify.inject({
        method: 'PUT',
        url: '/main-bucket',
        headers: { 'x-api-key': 'test' },
      })

      const putMain = await fastify.inject({
        method: 'PUT',
        url: '/main-bucket/private.txt',
        headers: {
          'x-api-key': 'test',
          'content-type': 'text/plain',
        },
        payload: 'private-data',
      })
      const mainRoute = getRoute(encodedKey('main-bucket', 'private.txt'))

      assert(putMain.statusCode === 200, `main PUT status=${putMain.statusCode}`)
      assert(mainRoute?.account_id === 'acc-private', `main route account=${mainRoute?.account_id}`)
      assert(privateUpstream.hasObject('private-physical', 'main-bucket/private.txt'), 'main object missing on private backend')
      assert(!publicUpstream.hasObject('public-physical', 'main-bucket/private.txt'), 'main object should not be stored on public backend')
      ok('Luong upload chinh loai tru backend public khoi rotation')
    } catch (err) {
      fail('Main upload excludes public backend', err)
    }

    try {
      const upload = await fastify.inject({
        method: 'PUT',
        url: '/public-bucket-proxy/files/docs/readme.txt',
        headers: {
          'content-type': 'application/octet-stream',
          'x-public-file-content-type': 'text/plain',
        },
        payload: 'hello public',
      })
      const uploadBody = upload.json()
      const publicRoute = getRoute(encodedKey(PUBLIC_PROXY_BUCKET, 'docs/readme.txt'))

      assert(upload.statusCode === 200, `public upload status=${upload.statusCode}`)
      assert(uploadBody.file.accountId === 'acc-public', `public upload account=${uploadBody.file.accountId}`)
      assert(uploadBody.file.directUrl.includes('/public-physical/public-bucket-proxy/docs/readme.txt'), `public directUrl=${uploadBody.file.directUrl}`)
      assert(publicRoute?.route_scope === ROUTE_SCOPE.PUBLIC, `public route scope=${publicRoute?.route_scope}`)
      assert(publicRoute?.state === ROUTE_STATE.ACTIVE, `public route state=${publicRoute?.state}`)
      assert(publicRoute?.public_url === uploadBody.file.directUrl, 'public route did not persist directUrl')
      assert(publicUpstream.hasObject('public-physical', 'public-bucket-proxy/docs/readme.txt'), 'public object missing on public backend')
      assert(!privateUpstream.hasObject('private-physical', 'public-bucket-proxy/docs/readme.txt'), 'public object should not be stored on private backend')
      ok('PUT /public-bucket-proxy/files/* luu vao backend public va tra directUrl')

      const listRes = await fastify.inject({
        method: 'GET',
        url: '/public-bucket-proxy/files',
      })
      const listBody = listRes.json()
      assert(listRes.statusCode === 200, `public list status=${listRes.statusCode}`)
      assert(listBody.total === 1, `public list total=${listBody.total}`)
      assert(listBody.files[0].path === 'docs/readme.txt', `public list path=${listBody.files[0]?.path}`)

      const getRes = await fastify.inject({
        method: 'GET',
        url: '/public-bucket-proxy/files/docs/readme.txt',
      })
      const getBody = getRes.json()
      assert(getRes.statusCode === 200, `public get status=${getRes.statusCode}`)
      assert(getBody.file.directUrl === uploadBody.file.directUrl, 'public get directUrl mismatch')

      const deleteRes = await fastify.inject({
        method: 'DELETE',
        url: '/public-bucket-proxy/files/docs/readme.txt',
      })
      const deleteBody = deleteRes.json()
      const deletedRoute = getRoute(encodedKey(PUBLIC_PROXY_BUCKET, 'docs/readme.txt'))

      assert(deleteRes.statusCode === 200, `public delete status=${deleteRes.statusCode}`)
      assert(deleteBody.deleted === true, `public delete deleted=${deleteBody.deleted}`)
      assert(deletedRoute?.state === ROUTE_STATE.DELETED, `deleted public route state=${deletedRoute?.state}`)
      assert(!publicUpstream.hasObject('public-physical', 'public-bucket-proxy/docs/readme.txt'), 'public object still exists on backend after delete')

      const afterDelete = await fastify.inject({
        method: 'GET',
        url: '/public-bucket-proxy/files/docs/readme.txt',
      })
      assert(afterDelete.statusCode === 404, `public get after delete status=${afterDelete.statusCode}`)
      ok('GET/LIST/DELETE public file dung voi metadata va backend')
    } catch (err) {
      fail('Public bucket proxy CRUD', err)
    }
  } finally {
    await fastify.close().catch(() => {})
    await privateUpstream.close().catch(() => {})
    await publicUpstream.close().catch(() => {})
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
