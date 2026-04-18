#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");
const https = require("https");
const crypto = require("crypto");

// ================================================================
// tailscale/tailscale-keep-ip.js
// Modes:
//   prepare     - optionally restore tailscaled.state from Firebase (base64),
//                 restore certs, and/or remove existing machine(s) by PROJECT_NAME
//   backup-loop - periodically backup tailscaled.state + certs to Firebase
//
// Environment:
//   TAILSCALE_KEEP_IP_ENABLE=true|false
//   TAILSCALE_KEEP_IP_REMOVE_HOSTNAME_ENABLE=true|false
//   TAILSCALE_KEEP_IP_FIREBASE_URL=<https://.../path.json?auth=...>
//   TAILSCALE_KEEP_IP_STATE_FILE=/var/lib/tailscale/tailscaled.state
//   TAILSCALE_KEEP_IP_CERTS_DIR=/var/lib/tailscale/certs
//   TAILSCALE_KEEP_IP_INTERVAL_SEC=30
//   PROJECT_NAME=<hostname to keep>
//   TAILSCALE_TS_TAILNET=- (or TS_TAILNET)
//   TAILSCALE_CLIENTID + TAILSCALE_AUTHKEY
//
// Firebase keys (under the same base URL):
//   - state: tailscaled.state payload
//   - certs: /var/lib/tailscale/certs snapshot payload
// ================================================================

function toBool(value, fallback = false) {
  if (value === undefined || value === null || value === "") return fallback;
  const v = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(v)) return true;
  if (["0", "false", "no", "n", "off"].includes(v)) return false;
  return fallback;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function sha256Base16(buffer) {
  return crypto.createHash("sha256").update(buffer).digest("hex");
}

function normalizeHostLabel(value) {
  if (typeof value !== "string") return "";
  return value.trim().toLowerCase();
}

function shortHostnameFromValue(value) {
  const v = normalizeHostLabel(value);
  if (!v) return "";
  const first = v.split(".")[0];
  return first || v;
}

function trimLeadingTrailingDots(value) {
  return String(value || "").replace(/^\.+/, "").replace(/\.+$/, "");
}

function ensureServeConfigFileFromEnv() {
  // Template to keep in code so we can tweak structure later without touching replacement logic.
  const serveConfigTemplate = {
    TCP: {
      "443": {
        HTTPS: true,
      },
    },
    Web: {
      "dockerstacks3proxy.tail03fb4e.ts.net:443": {
        Handlers: {
          "/": {
            Proxy: "http://127.0.0.1:80",
          },
        },
      },
    },
  };

  const projectName = trimLeadingTrailingDots(
    normalizeHostLabel(
      (process.env.PROJECT_NAME_TAILSCALE || process.env.PROJECT_NAME || "dockerstacks3proxy").trim(),
    ),
  );
  const tailnetDomain = trimLeadingTrailingDots(
    normalizeHostLabel((process.env.TAILSCALE_TAILNET_DOMAIN || "").trim()),
  );
  if (!tailnetDomain) {
    throw new Error("TAILSCALE_TAILNET_DOMAIN is required to build tailscale/serve.json");
  }
  const fqdn = [projectName, tailnetDomain].filter(Boolean).join(".");
  const webHostKey = `${fqdn}:443`;
  const templateWebEntry = Object.values(serveConfigTemplate.Web)[0];

  const serveConfig = {
    ...serveConfigTemplate,
    Web: {
      [webHostKey]: templateWebEntry,
    },
  };

  const serveJsonPath = path.join(__dirname, "serve.json");
  fs.mkdirSync(path.dirname(serveJsonPath), { recursive: true });
  fs.writeFileSync(serveJsonPath, `${JSON.stringify(serveConfig, null, 2)}\n`, "utf-8");
  console.log(`✅  serve-config: wrote ${serveJsonPath} (${webHostKey})`);
}

function pickDeviceId(device) {
  if (!device || typeof device !== "object") return "";
  return String(device.nodeId || device.id || device.deviceId || "").trim();
}

function collectDeviceHostCandidates(device) {
  if (!device || typeof device !== "object") return [];
  const values = [
    device.hostname,
    device.name,
    device.computedName,
    device.givenName,
    device.machineName,
    device.dnsName,
  ];

  const out = new Set();
  for (const raw of values) {
    const full = normalizeHostLabel(raw);
    if (!full) continue;
    out.add(full);
    const short = shortHostnameFromValue(full);
    if (short) out.add(short);
  }
  return [...out];
}

function apiRequest({ method, url, headers, body }) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(url);
    const payload = body === undefined ? "" : JSON.stringify(body);
    const reqHeaders = { Accept: "application/json", ...(headers || {}) };
    if (body !== undefined) {
      reqHeaders["Content-Type"] = "application/json";
      reqHeaders["Content-Length"] = Buffer.byteLength(payload);
    }

    const req = https.request(
      {
        protocol: parsed.protocol,
        hostname: parsed.hostname,
        port: parsed.port || 443,
        path: `${parsed.pathname}${parsed.search}`,
        method,
        headers: reqHeaders,
      },
      (res) => {
        let raw = "";
        res.on("data", (chunk) => {
          raw += chunk;
        });
        res.on("end", () => {
          let json = null;
          try {
            json = raw ? JSON.parse(raw) : null;
          } catch {
            json = null;
          }
          resolve({
            status: res.statusCode || 0,
            headers: res.headers || {},
            raw,
            body: json,
          });
        });
      },
    );

    req.on("error", reject);
    if (body !== undefined) req.write(payload);
    req.end();
  });
}

function apiRequestTailscale({ method, endpointPath, accessToken, body }) {
  return apiRequest({
    method,
    url: `https://api.tailscale.com/api/v2${endpointPath}`,
    headers: { Authorization: `Bearer ${accessToken}` },
    body,
  });
}

async function getOAuthAccessToken(clientId, clientSecret) {
  const form = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: clientId,
    client_secret: clientSecret,
  }).toString();

  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        protocol: "https:",
        hostname: "api.tailscale.com",
        port: 443,
        path: "/api/v2/oauth/token",
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          "Content-Length": Buffer.byteLength(form),
          Accept: "application/json",
        },
      },
      (res) => {
        let raw = "";
        res.on("data", (chunk) => {
          raw += chunk;
        });
        res.on("end", () => {
          let json = null;
          try {
            json = raw ? JSON.parse(raw) : null;
          } catch {
            json = null;
          }
          resolve({
            status: res.statusCode || 0,
            body: json,
            raw,
          });
        });
      },
    );

    req.on("error", reject);
    req.write(form);
    req.end();
  });
}

function isLikelyFirebaseUrl(url) {
  if (!url) return false;
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== "https:") return false;
    return parsed.pathname.endsWith(".json");
  } catch {
    return false;
  }
}

function firebaseChildUrl(firebaseUrl, childKey) {
  if (!isLikelyFirebaseUrl(firebaseUrl)) return "";
  const parsed = new URL(firebaseUrl);
  const cleanedChild = String(childKey || "").trim().replace(/[^A-Za-z0-9_-]/g, "");
  if (!cleanedChild) return firebaseUrl;
  parsed.pathname = parsed.pathname.replace(/\.json$/, `/${cleanedChild}.json`);
  return parsed.toString();
}

function toPosixRelativePath(rootDir, absPath) {
  return path.relative(rootDir, absPath).split(path.sep).join("/");
}

function isSafeRelativePosixPath(value) {
  if (!value || typeof value !== "string") return false;
  const normalized = path.posix.normalize(value.trim());
  if (!normalized || normalized === ".") return false;
  if (normalized.startsWith("../") || normalized === "..") return false;
  if (normalized.includes("\\")) return false;
  if (path.posix.isAbsolute(normalized)) return false;
  return true;
}

function collectFilesRecursive(rootDir) {
  if (!fs.existsSync(rootDir)) return [];
  const out = [];
  const stack = [rootDir];
  while (stack.length) {
    const current = stack.pop();
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const abs = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(abs);
        continue;
      }
      if (!entry.isFile()) continue;
      const rel = toPosixRelativePath(rootDir, abs);
      if (!isSafeRelativePosixPath(rel)) continue;
      const buf = fs.readFileSync(abs);
      const stat = fs.statSync(abs);
      out.push({
        relPath: rel,
        mode: stat.mode & 0o777,
        data: buf,
      });
    }
  }
  out.sort((a, b) => a.relPath.localeCompare(b.relPath));
  return out;
}

function sha256ForFiles(files) {
  const h = crypto.createHash("sha256");
  for (const f of files) {
    h.update(f.relPath);
    h.update("\0");
    h.update(f.data);
    h.update("\0");
  }
  return h.digest("hex");
}

function readStateFile(stateFilePath) {
  if (!fs.existsSync(stateFilePath)) return null;
  try {
    return fs.readFileSync(stateFilePath);
  } catch (err) {
    throw new Error(`cannot read state file ${stateFilePath}: ${err.message}`);
  }
}

function looksLikeTailscaleState(buffer) {
  if (!buffer || buffer.length < 3) return false;
  const text = buffer.toString("utf-8");
  if (!text || text[0] !== "{") return false;
  return text.includes("_machinekey") || text.includes("_profiles") || text.includes("profile-");
}

async function backupState({
  firebaseUrl,
  stateFilePath,
  hostname,
  tailnet,
  modeLabel,
  lastHashRef,
}) {
  const stateBuffer = readStateFile(stateFilePath);
  if (!stateBuffer || stateBuffer.length === 0) {
    console.log(`ℹ️  ${modeLabel}: state file not found yet: ${stateFilePath}`);
    return false;
  }
  if (!looksLikeTailscaleState(stateBuffer)) {
    console.log(`ℹ️  ${modeLabel}: state file looks incomplete (${stateBuffer.length} bytes), skip upload.`);
    return false;
  }

  const hash = sha256Base16(stateBuffer);
  if (lastHashRef.value && lastHashRef.value === hash) {
    return false;
  }

  const payload = {
    version: 1,
    updatedAt: new Date().toISOString(),
    hostname,
    tailnet,
    sizeBytes: stateBuffer.length,
    sha256: hash,
    stateBase64: stateBuffer.toString("base64"),
  };

  const putRes = await apiRequest({
    method: "PUT",
    url: firebaseUrl,
    body: payload,
  });

  if (putRes.status < 200 || putRes.status >= 300) {
    throw new Error(`${modeLabel}: Firebase PUT failed (HTTP ${putRes.status})`);
  }

  lastHashRef.value = hash;
  console.log(`✅  ${modeLabel}: uploaded state (${stateBuffer.length} bytes, sha256=${hash.slice(0, 12)}...)`);
  return true;
}

async function restoreState({ firebaseUrl, stateFilePath }) {
  const getRes = await apiRequest({
    method: "GET",
    url: firebaseUrl,
  });

  if (getRes.status === 404) {
    console.log("ℹ️  restore: no backup found (404).");
    return false;
  }
  if (getRes.status < 200 || getRes.status >= 300) {
    throw new Error(`restore: Firebase GET failed (HTTP ${getRes.status})`);
  }

  const doc = getRes.body;
  if (!doc) {
    console.log("ℹ️  restore: backup document is empty.");
    return false;
  }

  const base64 = typeof doc === "string" ? doc : doc.stateBase64;
  if (!base64 || typeof base64 !== "string") {
    console.log("ℹ️  restore: no stateBase64 field found.");
    return false;
  }

  const data = Buffer.from(base64, "base64");
  if (!data.length) {
    console.log("ℹ️  restore: decoded state is empty.");
    return false;
  }
  if (!looksLikeTailscaleState(data)) {
    console.log(`ℹ️  restore: backup state looks incomplete (${data.length} bytes), skipping restore.`);
    return false;
  }

  fs.mkdirSync(path.dirname(stateFilePath), { recursive: true });
  fs.writeFileSync(stateFilePath, data);
  console.log(`✅  restore: wrote ${stateFilePath} (${data.length} bytes)`);
  return true;
}

async function backupCerts({
  firebaseUrl,
  certsDirPath,
  hostname,
  tailnet,
  modeLabel,
  lastHashRef,
}) {
  const files = collectFilesRecursive(certsDirPath);
  if (!files.length) {
    if (lastHashRef.value !== "__EMPTY__") {
      console.log(`ℹ️  ${modeLabel}: certs dir has no files yet: ${certsDirPath}`);
    }
    lastHashRef.value = "__EMPTY__";
    return false;
  }

  const hash = sha256ForFiles(files);
  if (lastHashRef.value && lastHashRef.value === hash) {
    return false;
  }

  const payload = {
    version: 1,
    updatedAt: new Date().toISOString(),
    hostname,
    tailnet,
    dirPath: certsDirPath,
    fileCount: files.length,
    sizeBytes: files.reduce((sum, f) => sum + f.data.length, 0),
    sha256: hash,
    files: files.map((f) => ({
      path: f.relPath,
      mode: f.mode,
      dataBase64: f.data.toString("base64"),
    })),
  };

  const putRes = await apiRequest({
    method: "PUT",
    url: firebaseUrl,
    body: payload,
  });

  if (putRes.status < 200 || putRes.status >= 300) {
    throw new Error(`${modeLabel}: Firebase PUT certs failed (HTTP ${putRes.status})`);
  }

  lastHashRef.value = hash;
  console.log(
    `✅  ${modeLabel}: uploaded certs (${payload.fileCount} files, ${payload.sizeBytes} bytes, sha256=${hash.slice(0, 12)}...)`,
  );
  return true;
}

async function restoreCerts({ firebaseUrl, certsDirPath }) {
  const getRes = await apiRequest({
    method: "GET",
    url: firebaseUrl,
  });

  if (getRes.status === 404) {
    console.log("ℹ️  certs restore: no backup found (404).");
    return false;
  }
  if (getRes.status < 200 || getRes.status >= 300) {
    throw new Error(`certs restore: Firebase GET failed (HTTP ${getRes.status})`);
  }

  const doc = getRes.body;
  if (!doc || typeof doc !== "object" || !Array.isArray(doc.files)) {
    console.log("ℹ️  certs restore: no files payload found.");
    return false;
  }

  let restored = 0;
  for (const item of doc.files) {
    const rel = typeof item?.path === "string" ? item.path : "";
    if (!isSafeRelativePosixPath(rel)) continue;
    const b64 = typeof item?.dataBase64 === "string" ? item.dataBase64 : "";
    if (!b64) continue;

    const abs = path.join(certsDirPath, ...rel.split("/"));
    const buf = Buffer.from(b64, "base64");
    fs.mkdirSync(path.dirname(abs), { recursive: true });
    fs.writeFileSync(abs, buf);

    const mode = Number(item.mode);
    if (Number.isInteger(mode) && mode >= 0 && mode <= 0o777) {
      try {
        fs.chmodSync(abs, mode);
      } catch {
        // ignore mode errors across platforms/filesystems
      }
    }
    restored += 1;
  }

  if (!restored) {
    console.log("ℹ️  certs restore: payload found but no valid files restored.");
    return false;
  }

  console.log(`✅  certs restore: wrote ${restored} file(s) into ${certsDirPath}`);
  return true;
}

async function removeHostnameFromTailnet({ hostname, tailnet, clientSecret, clientId }) {
  if (!hostname) {
    console.log("⚠️  remove-hostname: PROJECT_NAME is empty, skipping.");
    return;
  }
  if (!clientSecret || !clientId) {
    console.log("⚠️  remove-hostname: missing TAILSCALE_AUTHKEY or TAILSCALE_CLIENTID, skipping.");
    return;
  }

  let accessToken = "";
  try {
    const tokenRes = await getOAuthAccessToken(clientId, clientSecret);
    if (tokenRes.status === 200 && tokenRes.body && tokenRes.body.access_token) {
      accessToken = tokenRes.body.access_token;
    } else {
      console.log(`⚠️  remove-hostname: OAuth token request failed (HTTP ${tokenRes.status}), skipping.`);
      return;
    }
  } catch (err) {
    console.log(`⚠️  remove-hostname: cannot get OAuth token (${err.message}), skipping.`);
    return;
  }

  const encodedTailnet = encodeURIComponent(tailnet || "-");
  const devicesRes = await apiRequestTailscale({
    method: "GET",
    endpointPath: `/tailnet/${encodedTailnet}/devices`,
    accessToken,
  });

  if (devicesRes.status !== 200) {
    console.log(`⚠️  remove-hostname: cannot list devices (HTTP ${devicesRes.status}), skipping.`);
    return;
  }

  const devices = Array.isArray(devicesRes.body?.devices)
    ? devicesRes.body.devices
    : Array.isArray(devicesRes.body)
      ? devicesRes.body
      : [];

  const target = normalizeHostLabel(hostname);
  const matched = devices.filter((d) => collectDeviceHostCandidates(d).includes(target));

  if (!matched.length) {
    console.log(`ℹ️  remove-hostname: no existing device matched "${hostname}".`);
    return;
  }

  let removed = 0;
  for (const device of matched) {
    const deviceId = pickDeviceId(device);
    if (!deviceId) continue;
    const delRes = await apiRequestTailscale({
      method: "DELETE",
      endpointPath: `/device/${encodeURIComponent(deviceId)}`,
      accessToken,
    });

    if ([200, 202, 204, 404].includes(delRes.status)) {
      removed += 1;
      continue;
    }
    console.log(`⚠️  remove-hostname: failed delete id=${deviceId} (HTTP ${delRes.status})`);
  }

  console.log(`✅  remove-hostname: processed ${removed}/${matched.length} matched device(s).`);
}

async function run() {
  ensureServeConfigFileFromEnv();

  const mode = (process.argv[2] || "prepare").trim().toLowerCase();
  const keepIpEnabled = toBool(process.env.TAILSCALE_KEEP_IP_ENABLE, false);
  const removeHostnameEnabled = toBool(
    process.env.TAILSCALE_KEEP_IP_REMOVE_HOSTNAME_ENABLE,
    keepIpEnabled,
  );

  const firebaseUrl = (process.env.TAILSCALE_KEEP_IP_FIREBASE_URL || "").trim();
  const stateFilePath = (process.env.TAILSCALE_KEEP_IP_STATE_FILE || "/var/lib/tailscale/tailscaled.state").trim();
  const certsDirPath = (
    process.env.TAILSCALE_KEEP_IP_CERTS_DIR ||
    path.join(path.dirname(stateFilePath), "certs")
  ).trim();
  const intervalSecRaw = (process.env.TAILSCALE_KEEP_IP_INTERVAL_SEC || "30").trim();
  const intervalSec = Number.isInteger(Number(intervalSecRaw)) ? Number(intervalSecRaw) : 30;
  const hostname = (process.env.PROJECT_NAME || "").trim();
  const tailnet = (process.env.TAILSCALE_TS_TAILNET || process.env.TS_TAILNET || "-").trim() || "-";
  const clientSecret = (process.env.TAILSCALE_AUTHKEY || "").trim();
  const clientId = (process.env.TAILSCALE_CLIENTID || "").trim();
  const hasFirebaseUrl = isLikelyFirebaseUrl(firebaseUrl);
  const firebaseStateUrl = hasFirebaseUrl ? firebaseChildUrl(firebaseUrl, "state") : "";
  const firebaseCertsUrl = hasFirebaseUrl ? firebaseChildUrl(firebaseUrl, "certs") : "";

  console.log(`\n🔐  Tailscale Keep IP (${mode})`);
  console.log(`    keep_ip_enabled           : ${keepIpEnabled}`);
  console.log(`    remove_hostname_enabled   : ${removeHostnameEnabled}`);
  console.log(`    state_path                : ${stateFilePath}`);
  console.log(`    certs_path                : ${certsDirPath}`);
  console.log(`    host                      : ${hostname || "(missing)"}`);
  console.log(`    tailnet                   : ${tailnet}`);
  console.log(`    firebase_url_valid        : ${hasFirebaseUrl}\n`);

  if (mode === "prepare") {
    if (!hasFirebaseUrl) {
      console.log("⚠️  prepare: TAILSCALE_KEEP_IP_FIREBASE_URL is invalid/missing, skipping cert/state restore.");
    } else {
      try {
        await restoreCerts({ firebaseUrl: firebaseCertsUrl, certsDirPath });
      } catch (err) {
        console.log(`⚠️  prepare: certs restore failed (${err.message}), continuing.`);
      }

      if (keepIpEnabled) {
        let restored = false;
        try {
          restored = await restoreState({ firebaseUrl: firebaseStateUrl, stateFilePath });
        } catch (err) {
          console.error(`❌  prepare: state restore failed (${err.message})`);
          process.exit(1);
        }

        // Backward compatibility: fallback to legacy root payload.
        if (!restored && firebaseStateUrl !== firebaseUrl) {
          console.log("ℹ️  prepare: no state under key 'state', trying legacy root payload...");
          try {
            await restoreState({ firebaseUrl, stateFilePath });
          } catch (err) {
            console.error(`❌  prepare: legacy state restore failed (${err.message})`);
            process.exit(1);
          }
        }
      } else {
        console.log("ℹ️  prepare: keep-ip restore disabled by TAILSCALE_KEEP_IP_ENABLE=false.");
      }
    }

    if (keepIpEnabled && !hasFirebaseUrl) {
      console.error("❌  TAILSCALE_KEEP_IP_ENABLE=true requires valid TAILSCALE_KEEP_IP_FIREBASE_URL.");
      process.exit(1);
    }

    if (removeHostnameEnabled) {
      await removeHostnameFromTailnet({ hostname, tailnet, clientSecret, clientId });
    } else {
      console.log("ℹ️  prepare: remove-hostname disabled by TAILSCALE_KEEP_IP_REMOVE_HOSTNAME_ENABLE=false.");
    }

    if (!keepIpEnabled && !removeHostnameEnabled && !hasFirebaseUrl) {
      console.log("ℹ️  prepare: no enabled action, done.");
    }

    console.log("\n✅  prepare complete.\n");
    process.exit(0);
  }

  if (mode === "backup-once") {
    if (!hasFirebaseUrl) {
      console.error("❌  TAILSCALE_KEEP_IP_FIREBASE_URL is invalid or missing (must be https URL ending with .json).");
      process.exit(1);
    }

    await backupCerts({
      firebaseUrl: firebaseCertsUrl,
      certsDirPath,
      hostname,
      tailnet,
      modeLabel: "backup-once/certs",
      lastHashRef: { value: "" },
    });

    if (keepIpEnabled) {
      await backupState({
        firebaseUrl: firebaseStateUrl,
        stateFilePath,
        hostname,
        tailnet,
        modeLabel: "backup-once/state",
        lastHashRef: { value: "" },
      });
    } else {
      console.log("ℹ️  backup-once: state backup disabled by TAILSCALE_KEEP_IP_ENABLE=false.");
    }

    console.log("\n✅  backup-once complete.\n");
    process.exit(0);
  }

  if (mode === "backup-loop") {
    if (!hasFirebaseUrl) {
      console.error("❌  TAILSCALE_KEEP_IP_FIREBASE_URL is invalid or missing (must be https URL ending with .json).");
      process.exit(1);
    }

    const everyMs = Math.max(5, intervalSec) * 1000;
    const lastStateHashRef = { value: "" };
    const lastCertsHashRef = { value: "" };
    console.log(`ℹ️  backup-loop: interval ${Math.max(5, intervalSec)}s`);
    if (!keepIpEnabled) {
      console.log("ℹ️  backup-loop: state backup disabled by TAILSCALE_KEEP_IP_ENABLE=false.");
    }
    console.log("ℹ️  backup-loop: certs backup is always enabled.");

    let stopping = false;
    const stop = (signal) => {
      if (stopping) return;
      stopping = true;
      console.log(`\nℹ️  received ${signal}, stopping backup-loop...`);
    };
    process.on("SIGINT", () => stop("SIGINT"));
    process.on("SIGTERM", () => stop("SIGTERM"));

    while (!stopping) {
      if (keepIpEnabled) {
        try {
          await backupState({
            firebaseUrl: firebaseStateUrl,
            stateFilePath,
            hostname,
            tailnet,
            modeLabel: "backup-loop/state",
            lastHashRef: lastStateHashRef,
          });
        } catch (err) {
          console.log(`⚠️  backup-loop/state: ${err.message}`);
        }
      }

      try {
        await backupCerts({
          firebaseUrl: firebaseCertsUrl,
          certsDirPath,
          hostname,
          tailnet,
          modeLabel: "backup-loop/certs",
          lastHashRef: lastCertsHashRef,
        });
      } catch (err) {
        console.log(`⚠️  backup-loop/certs: ${err.message}`);
      }
      await sleep(everyMs);
    }

    console.log("✅  backup-loop stopped.\n");
    process.exit(0);
  }

  console.error(`❌  Unknown mode: ${mode}`);
  console.error("    Use one of: prepare, backup-once, backup-loop");
  process.exit(1);
}

run().catch((err) => {
  console.error(`❌  Unexpected error: ${err.message}`);
  process.exit(1);
});
