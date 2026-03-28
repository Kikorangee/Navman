const express = require('express');
const axios = require('axios');
const xml2js = require('xml2js');
const path = require('path');
const fs = require('fs');

const PORT = Number(process.env.PORT || 3500);
const API_URL = 'https://onlineavl2api-au.navmanwireless.com/onlineavl/api/V4.3/service.asmx';
const SOAP_NS = 'http://onlineavl2.navmanwireless.com/0907/';
const NAVMAN_LOGIN_URI_AU = 'https://onlineavl2api-au.navmanwireless.com/onlineavl/api/V1.9/service.asmx';
const NAVMAN_LOGIN_URI_NZ = 'https://onlineavl2api-nz.navmanwireless.com/onlineavl/api/V1.9/service.asmx';

const MAX_TOTAL_MS = 90 * 24 * 3600_000;
const MAX_CHUNK_MS = 6 * 3600_000;
const OWNER_ACTIVITY_CHUNK_MS = 24 * 3600_000;
const OWNER_ACTIVITY_MAX_LOOPS = 12;
const OWNER_ACTIVITY_MAX_MS_PER_CHUNK = 25_000;
const MAX_PAGE_LOOPS = 60;
const VEHICLE_CACHE_TTL_MS = 60 * 60_000;
const RESPONSE_CACHE_TTL_MS = 5 * 60_000;
const NAVMAN_SESSION_TTL_MS = 55 * 60_000;
const NO_DATA_CODES = new Set(['1160', '1161']);
const GEO_AUTH_TTL_MS = 50 * 60_000;
const GEO_MAX_RESULTS = 10_000;
const GEO_MAX_CHUNK_MS = 24 * 3600_000;

const cfgPath = path.join(__dirname, 'config.json');
let cfg = loadConfig();

const navmanLoginUris = Array.from(new Set([
  String(cfg.NAVMAN_LOGIN_URI || process.env.NAVMAN_LOGIN_URI || NAVMAN_LOGIN_URI_AU).trim(),
  NAVMAN_LOGIN_URI_NZ,
  API_URL
].filter(Boolean)));

const creds = {
  UserName: cfg.NAVMAN_USER || process.env.NAVMAN_USER || '',
  Password: cfg.NAVMAN_PASS || process.env.NAVMAN_PASS || '',
  ApplicationID: cfg.NAVMAN_APPID || process.env.NAVMAN_APPID || '00000000-0000-0000-0000-000000000000',
  ClientID: cfg.NAVMAN_CLIENTID || process.env.NAVMAN_CLIENTID || '00000000-0000-0000-0000-000000000000',
  OwnerID: cfg.NAVMAN_OWNERID || process.env.NAVMAN_OWNERID || '',
  UserID: cfg.NAVMAN_USERID || process.env.NAVMAN_USERID || '',
  SessionID: process.env.NAVMAN_SESSION || cfg.NAVMAN_SESSION || ''
};

const geoCreds = {
  Server: cfg.GEOTAB_SERVER || process.env.GEOTAB_SERVER || 'my.geotab.com',
  Database: cfg.GEOTAB_DATABASE || process.env.GEOTAB_DATABASE || '',
  UserName: cfg.GEOTAB_USER || process.env.GEOTAB_USER || '',
  Password: cfg.GEOTAB_PASS || process.env.GEOTAB_PASS || '',
  SessionServer: cfg.GEOTAB_SESSION_SERVER || process.env.GEOTAB_SESSION_SERVER || '',
  SessionDatabase: cfg.GEOTAB_SESSION_DATABASE || process.env.GEOTAB_SESSION_DATABASE || '',
  SessionUserName: cfg.GEOTAB_SESSION_USER || process.env.GEOTAB_SESSION_USER || '',
  SessionID: cfg.GEOTAB_SESSIONID || process.env.GEOTAB_SESSIONID || '',
  SessionExpiry: Number(cfg.GEOTAB_SESSION_EXPIRES || process.env.GEOTAB_SESSION_EXPIRES || 0)
};

if (!creds.SessionID) {
  navmanSessionState.expiresAt = 0;
}

const vehicleCache = {
  bySession: '',
  ts: 0,
  list: []
};

const driverCache = {
  bySession: '',
  ts: 0,
  list: []
};

const geotabCache = {
  bySession: '',
  ts: 0,
  devices: [],
  users: [],
  rules: []
};

const responseCache = new Map();
const navmanSessionState = {
  expiresAt: Date.now() + NAVMAN_SESSION_TTL_MS
};
let navmanLoginInFlight = null;

class NavmanApiError extends Error {
  constructor(action, code, message, statusMessage) {
    super(message || `Navman call failed: ${action}`);
    this.name = 'NavmanApiError';
    this.action = action;
    this.code = code || '';
    this.statusMessage = statusMessage || null;
  }
}

class GeotabApiError extends Error {
  constructor(method, code, message, details) {
    super(message || `Geotab call failed: ${method}`);
    this.name = 'GeotabApiError';
    this.method = method;
    this.code = code || '';
    this.details = details || null;
  }
}

function loadConfig() {
  if (!fs.existsSync(cfgPath)) return {};
  const raw = fs.readFileSync(cfgPath, 'utf8').replace(/^\uFEFF/, '');
  try {
    return JSON.parse(raw || '{}');
  } catch (_err) {
    return {};
  }
}

function saveConfig() {
  cfg = {
    ...cfg,
    NAVMAN_USER: creds.UserName || '',
    NAVMAN_PASS: creds.Password || '',
    NAVMAN_APPID: creds.ApplicationID || '',
    NAVMAN_CLIENTID: creds.ClientID || '',
    NAVMAN_OWNERID: creds.OwnerID || '',
    NAVMAN_USERID: creds.UserID || '',
    NAVMAN_SESSION: creds.SessionID || '',
    NAVMAN_LOGIN_URI: navmanLoginUris[0] || NAVMAN_LOGIN_URI_AU,
    GEOTAB_SERVER: geoCreds.Server || '',
    GEOTAB_DATABASE: geoCreds.Database || '',
    GEOTAB_USER: geoCreds.UserName || '',
    GEOTAB_PASS: geoCreds.Password || '',
    GEOTAB_SESSION_SERVER: geoCreds.SessionServer || '',
    GEOTAB_SESSION_DATABASE: geoCreds.SessionDatabase || '',
    GEOTAB_SESSION_USER: geoCreds.SessionUserName || '',
    GEOTAB_SESSIONID: geoCreds.SessionID || '',
    GEOTAB_SESSION_EXPIRES: geoCreds.SessionExpiry || 0
  };
  fs.writeFileSync(cfgPath, JSON.stringify(cfg, null, 4), 'utf8');
}

function clearNavmanCaches() {
  vehicleCache.bySession = '';
  vehicleCache.ts = 0;
  vehicleCache.list = [];
  driverCache.bySession = '';
  driverCache.ts = 0;
  driverCache.list = [];
  responseCache.clear();
}

function setNavmanSession(sessionId) {
  const sid = String(sessionId || '').trim();
  creds.SessionID = sid;
  navmanSessionState.expiresAt = sid ? Date.now() + NAVMAN_SESSION_TTL_MS : 0;
  clearNavmanCaches();
  saveConfig();
}

function xmlEscape(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function cacheKey(routeName, req) {
  return JSON.stringify({
    routeName,
    sid: creds.SessionID,
    geoSid: geoCreds.SessionID,
    start: String(req.query.start || ''),
    end: String(req.query.end || ''),
    vehicles: String(req.query.vehicles || ''),
    drivers: String(req.query.drivers || ''),
    eventType: String(req.query.eventType || ''),
    requestType: String(req.query.requestType || '')
  });
}

function getCached(key) {
  const hit = responseCache.get(key);
  if (!hit) return null;
  if (Date.now() - hit.ts > RESPONSE_CACHE_TTL_MS) {
    responseCache.delete(key);
    return null;
  }
  return hit.payload;
}

function setCached(key, payload) {
  responseCache.set(key, { ts: Date.now(), payload });
  if (responseCache.size > 100) {
    const oldest = responseCache.keys().next().value;
    responseCache.delete(oldest);
  }
}

function toUtcNoMs(isoLike) {
  const dt = new Date(isoLike);
  return dt.toISOString().replace(/\.\d{3}Z$/, '');
}

function textOf(node, keys) {
  for (const key of keys) {
    const val = node?.[key];
    if (Array.isArray(val) && val.length > 0) {
      const first = val[0];
      if (typeof first === 'string') return first;
      if (first && typeof first === 'object' && typeof first._ === 'string') return first._;
      if (first !== undefined && first !== null) return String(first);
    }
    if (typeof val === 'string') return val;
  }
  return '';
}

function boolOf(value) {
  return String(value || '').toLowerCase() === 'true';
}

function safeNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function geotabRefId(value) {
  if (!value) return '';
  if (typeof value === 'string') return value;
  if (typeof value === 'object') return String(value.id || value.Id || '').trim();
  return '';
}

function geotabRefName(value) {
  if (!value || typeof value !== 'object') return '';
  return String(value.name || value.Name || '').trim();
}

function geotabToIso(value) {
  if (!value) return '';
  const dt = new Date(value);
  return Number.isNaN(dt.valueOf()) ? '' : dt.toISOString();
}

function parseDurationSeconds(value) {
  if (value === null || value === undefined || value === '') return 0;

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return 0;
    if (value > 100_000) return value / 1000; // likely milliseconds
    return value; // likely seconds
  }

  const s = String(value).trim();
  if (!s) return 0;

  const hms = s.match(/^(\d{1,3}):(\d{2}):(\d{2})(?:\.\d+)?$/);
  if (hms) {
    const h = Number(hms[1]);
    const m = Number(hms[2]);
    const sec = Number(hms[3]);
    return (h * 3600) + (m * 60) + sec;
  }

  const iso = s.match(/^P(?:\d+Y)?(?:\d+M)?(?:\d+D)?(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?$/i);
  if (iso) {
    const h = Number(iso[1] || 0);
    const m = Number(iso[2] || 0);
    const sec = Number(iso[3] || 0);
    return (h * 3600) + (m * 60) + sec;
  }

  const numeric = Number(s);
  if (Number.isFinite(numeric)) {
    if (numeric > 100_000) return numeric / 1000;
    return numeric;
  }

  return 0;
}

function parseDurationMinutes(value) {
  return parseDurationSeconds(value) / 60;
}

function validGuid(value) {
  return /^[0-9a-fA-F-]{36}$/.test(String(value || ''));
}

function emptyGuid(value) {
  return String(value || '').toLowerCase() === '00000000-0000-0000-0000-000000000000';
}

function composeName(fullName, firstName, lastName) {
  const full = String(fullName || '').trim();
  if (full) return full;
  const first = String(firstName || '').trim();
  const last = String(lastName || '').trim();
  return `${first} ${last}`.trim();
}

function parseGuidList(csvOrEmpty) {
  return String(csvOrEmpty || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
    .filter(s => /^[0-9a-fA-F-]{36}$/.test(s));
}

function parseCsvList(csvOrEmpty) {
  return String(csvOrEmpty || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

function guidListXml(tagName, ids) {
  if (!ids || ids.length === 0) return '';
  const inner = ids.map(id => `<guid>${xmlEscape(id)}</guid>`).join('');
  return `<${tagName}>${inner}</${tagName}>`;
}

function clampRange(startIso, endIso) {
  const end = endIso ? new Date(endIso) : new Date();
  let start = startIso ? new Date(startIso) : new Date(end.getTime() - 24 * 3600_000);

  if (Number.isNaN(start.valueOf()) || Number.isNaN(end.valueOf())) {
    throw new Error('Invalid date range');
  }
  if (end < start) [start, end] = [end, start];
  if (end - start > MAX_TOTAL_MS) {
    start = new Date(end.getTime() - MAX_TOTAL_MS);
  }

  return {
    start: start.toISOString(),
    end: end.toISOString()
  };
}

function chunkRange(startIso, endIso) {
  const chunks = [];
  let cursor = new Date(startIso).getTime();
  const endMs = new Date(endIso).getTime();

  while (cursor < endMs) {
    const next = Math.min(cursor + MAX_CHUNK_MS, endMs);
    chunks.push({
      start: new Date(cursor).toISOString(),
      end: new Date(next).toISOString()
    });
    cursor = next;
  }

  return chunks.length ? chunks : [{ start: startIso, end: endIso }];
}

function chunkRangeBySize(startIso, endIso, maxChunkMs) {
  const chunks = [];
  let cursor = new Date(startIso).getTime();
  const endMs = new Date(endIso).getTime();

  while (cursor < endMs) {
    const next = Math.min(cursor + maxChunkMs, endMs);
    chunks.push({
      start: new Date(cursor).toISOString(),
      end: new Date(next).toISOString()
    });
    cursor = next;
  }

  return chunks.length ? chunks : [{ start: startIso, end: endIso }];
}

async function soapCall(action, envelope, options = {}) {
  const uri = options.uri || API_URL;
  const timeout = Number(options.timeout || 90000);
  const response = await axios.post(uri, envelope, {
    headers: {
      'Content-Type': 'text/xml; charset=utf-8',
      SOAPAction: `${SOAP_NS}${action}`
    },
    timeout,
    validateStatus: () => true
  });

  const xml = String(response.data || '');
  try {
    return await xml2js.parseStringPromise(xml, { explicitArray: true });
  } catch (_err) {
    const shortBody = xml.slice(0, 400);
    throw new Error(`Invalid XML from ${action} (HTTP ${response.status}): ${shortBody}`);
  }
}

function getResult(parsed, responseTag, resultTag) {
  return parsed?.['soap:Envelope']?.['soap:Body']?.[0]?.[responseTag]?.[0]?.[resultTag]?.[0];
}

function getStatusInfo(result) {
  const msgGroup =
    result?.StatusMessage?.[0]?.SystemMessage?.[0] ||
    result?.StatusMessages?.[0]?.SystemMessage?.[0] ||
    null;

  const code = textOf(msgGroup, ['MessageCode']);
  const exception = textOf(msgGroup, ['ExceptionMessage']);
  const exceptionType = textOf(msgGroup, ['ExceptionType']);
  const stack = textOf(msgGroup, ['ExceptionStackTrace']);

  return {
    code,
    exception,
    exceptionType,
    stack,
    raw: msgGroup
  };
}

function ensureSuccess(action, result) {
  if (!result) {
    throw new NavmanApiError(action, '', 'Empty SOAP result');
  }

  const opOk = boolOf(textOf(result, ['OperationStatus']));
  if (opOk) {
    return { noData: false };
  }

  const info = getStatusInfo(result);
  if (NO_DATA_CODES.has(info.code)) {
    return { noData: true };
  }

  const message =
    info.exception ||
    (info.code ? `MessageCode ${info.code}` : 'OperationStatus=false');

  throw new NavmanApiError(action, info.code, message, info);
}

function navmanLoginReady() {
  return Boolean(creds.UserName && creds.Password);
}

function loginEnvelope(sessionSeed, clientVersion, ipAddress) {
  const clock = toUtcNoMs(new Date().toISOString());
  return `
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <DoLogin xmlns="${SOAP_NS}">
      <request>
        <Session><SessionId>${xmlEscape(sessionSeed)}</SessionId></Session>
        <UserCredential>
          <UserName>${xmlEscape(creds.UserName)}</UserName>
          <Password>${xmlEscape(creds.Password)}</Password>
          <ApplicationID>${xmlEscape(creds.ApplicationID || '00000000-0000-0000-0000-000000000000')}</ApplicationID>
          <ClientID>${xmlEscape(creds.ClientID || '00000000-0000-0000-0000-000000000000')}</ClientID>
          <ClientVersion>${xmlEscape(clientVersion)}</ClientVersion>
        </UserCredential>
        <IPAddress>${xmlEscape(ipAddress)}</IPAddress>
        <ClockVerificationUtc>${clock}</ClockVerificationUtc>
      </request>
    </DoLogin>
  </soap:Body>
</soap:Envelope>`;
}

function loginAttempts() {
  return [
    { sessionSeed: '00000000-0000-0000-0000-000000000000', clientVersion: '0', ipAddress: '' },
    { sessionSeed: '00000000-0000-0000-0000-000000000000', clientVersion: '1.0', ipAddress: '127.0.0.1' },
    { sessionSeed: '', clientVersion: '1.0', ipAddress: '127.0.0.1' }
  ];
}

function parseLoginResult(result) {
  const info = getStatusInfo(result || {});
  const opOk = boolOf(textOf(result, ['OperationStatus']));
  const authOk = boolOf(textOf(result, ['Authenticated']));
  const sid = textOf(result?.SecurityProfile?.[0]?.Session?.[0] || {}, ['SessionId']);
  const ownerId = textOf(result?.SecurityProfile?.[0]?.User?.[0] || {}, ['OwnerID', 'OwnerId']);
  const userId = textOf(result?.SecurityProfile?.[0]?.User?.[0] || {}, ['UserID', 'UserId']);
  return { opOk, authOk, sid, ownerId, userId, info };
}

function preferLoginError(current, next) {
  const rank = (err) => {
    if (err instanceof NavmanApiError) {
      if (err.code === '3001') return 100;
      if (err.code === '3000') return 90;
      if (err.code) return 80;
      return 60;
    }
    if (err instanceof Error) return 40;
    return 0;
  };
  if (!current) return next;
  return rank(next) >= rank(current) ? next : current;
}

async function navmanDoLogin(force = false) {
  if (!force && creds.SessionID && Date.now() < navmanSessionState.expiresAt) {
    return creds.SessionID;
  }
  if (!navmanLoginReady()) {
    throw new Error('Navman credentials missing. Set NAVMAN_USER and NAVMAN_PASS in config.json.');
  }
  if (navmanLoginInFlight) {
    return navmanLoginInFlight;
  }

  navmanLoginInFlight = (async () => {
    let lastErr = null;

    for (const uri of navmanLoginUris) {
      for (const attempt of loginAttempts()) {
        const envelope = loginEnvelope(attempt.sessionSeed, attempt.clientVersion, attempt.ipAddress);
        try {
          const parsed = await soapCall('DoLogin', envelope, { uri, timeout: 30000 });
          const result = getResult(parsed, 'DoLoginResponse', 'DoLoginResult');
          const login = parseLoginResult(result);
          if (login.opOk && login.authOk && validGuid(login.sid)) {
            if (validGuid(login.ownerId)) creds.OwnerID = login.ownerId;
            if (validGuid(login.userId)) creds.UserID = login.userId;
            setNavmanSession(login.sid);
            return creds.SessionID;
          }

          const message =
            login.info.exception ||
            (login.info.code ? `DoLogin MessageCode ${login.info.code}` : 'DoLogin failed');
          lastErr = preferLoginError(lastErr, new NavmanApiError('DoLogin', login.info.code, message, login.info));
        } catch (err) {
          lastErr = preferLoginError(lastErr, err);
        }
      }
    }

    if (lastErr instanceof NavmanApiError && lastErr.code === '3001') {
      throw new Error('Navman login blocked (MessageCode 3001: max sessions reached). Run DoLogoff for old sessions, then retry.');
    }
    throw lastErr || new Error('Navman login failed: no valid response received.');
  })();

  try {
    return await navmanLoginInFlight;
  } finally {
    navmanLoginInFlight = null;
  }
}

async function ensureNavmanSession(options = {}) {
  const forceRefresh = Boolean(options.forceRefresh);
  if (
    forceRefresh ||
    !creds.SessionID
  ) {
    await navmanDoLogin(forceRefresh);
  }
  return getCurrentSession();
}

async function withNavmanSessionRetry(work) {
  const initialSession = await ensureNavmanSession();
  try {
    return await work(initialSession);
  } catch (err) {
    if (err instanceof NavmanApiError && (err.code === '1238' || err.code === '3001')) {
      const freshSession = await navmanDoLogin(true);
      return work(freshSession);
    }
    throw err;
  }
}

function getCurrentSession() {
  if (!creds.SessionID) {
    throw new Error('No Navman session is available.');
  }
  return creds.SessionID;
}

async function fetchVehicles(sessionId) {
  const now = Date.now();
  if (
    vehicleCache.bySession === sessionId &&
    vehicleCache.list.length > 0 &&
    now - vehicleCache.ts < VEHICLE_CACHE_TTL_MS
  ) {
    return vehicleCache.list;
  }

  let version = 0;
  let loops = 0;
  const map = new Map();

  while (loops < MAX_PAGE_LOOPS) {
    loops += 1;

    const envelope = `
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetVehicles xmlns="${SOAP_NS}">
      <request>
        <Session><SessionId>${xmlEscape(sessionId)}</SessionId></Session>
        <Version>${version}</Version>
        <OwnerId>${xmlEscape(creds.OwnerID)}</OwnerId>
        <IsProfile>false</IsProfile>
        <PopulateTypeDisplayName>true</PopulateTypeDisplayName>
      </request>
    </GetVehicles>
  </soap:Body>
</soap:Envelope>`;

    const parsed = await soapCall('GetVehicles', envelope);
    const result = getResult(parsed, 'GetVehiclesResponse', 'GetVehiclesResult');
    let state;
    try {
      state = ensureSuccess('GetVehicles', result);
    } catch (err) {
      if (err instanceof NavmanApiError && err.code === '3000') {
        if (map.size > 0) break;
        if (
          vehicleCache.bySession === sessionId &&
          vehicleCache.list.length > 0
        ) {
          return vehicleCache.list;
        }
        if (loops < 3) {
          await sleep(1000 * loops);
          continue;
        }
      }
      throw err;
    }
    if (state.noData) break;

    const vehicles = [
      ...(result?.Vehicles?.[0]?.VehicleInfo || []),
      ...(result?.Vehicles?.[0]?.Vehicle || [])
    ];
    for (const v of vehicles) {
      const id = textOf(v, ['VehicleId', 'VehicleID', 'Id']);
      if (!/^[0-9a-fA-F-]{36}$/.test(id)) continue;
      if (!map.has(id)) {
        map.set(id, {
          VehicleID: id,
          VehicleName: textOf(v, ['DisplayName', 'Name', 'VehicleName']),
          Registration: textOf(v, ['Registration', 'RegistrationNumber']),
          VehicleType: textOf(v, ['VehicleTypeDisplayName', 'TypeDisplayName'])
        });
      }
    }

    // Some fleets return IDs in RemovedItems with no Vehicle objects.
    const removed = result?.RemovedItems?.[0]?.guid || [];
    for (const g of removed) {
      const id = typeof g === 'string' ? g : (g?._ || '');
      if (!/^[0-9a-fA-F-]{36}$/.test(id)) continue;
      if (!map.has(id)) {
        map.set(id, {
          VehicleID: id,
          VehicleName: '',
          Registration: '',
          VehicleType: ''
        });
      }
    }

    const more = boolOf(textOf(result, ['MoreItemsAvailable']));
    const nextVersion = Number(textOf(result, ['Version']) || version);
    if (!more) break;
    if (!Number.isFinite(nextVersion) || nextVersion <= version) break;
    version = nextVersion;
  }

  const list = Array.from(map.values());
  vehicleCache.bySession = sessionId;
  vehicleCache.ts = now;
  vehicleCache.list = list;
  return list;
}

async function fetchDrivers(sessionId) {
  const now = Date.now();
  if (
    driverCache.bySession === sessionId &&
    driverCache.list.length > 0 &&
    now - driverCache.ts < VEHICLE_CACHE_TTL_MS
  ) {
    return driverCache.list;
  }

  let version = 0;
  let loops = 0;
  const map = new Map();

  while (loops < MAX_PAGE_LOOPS) {
    loops += 1;

    const envelope = `
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetDrivers xmlns="${SOAP_NS}">
      <request>
        <Session><SessionId>${xmlEscape(sessionId)}</SessionId></Session>
        <Version>${version}</Version>
        <OwnerId>${xmlEscape(creds.OwnerID)}</OwnerId>
      </request>
    </GetDrivers>
  </soap:Body>
</soap:Envelope>`;

    const parsed = await soapCall('GetDrivers', envelope);
    const result = getResult(parsed, 'GetDriversResponse', 'GetDriversResult');
    let state;
    try {
      state = ensureSuccess('GetDrivers', result);
    } catch (err) {
      if (err instanceof NavmanApiError && err.code === '3000') {
        if (map.size > 0) break;
        if (
          driverCache.bySession === sessionId &&
          driverCache.list.length > 0
        ) {
          return driverCache.list;
        }
        if (loops < 3) {
          await sleep(1000 * loops);
          continue;
        }
      }
      throw err;
    }
    if (state.noData) break;

    const drivers = result?.Drivers?.[0]?.DriverHeader || [];
    for (const d of drivers) {
      const id = textOf(d, ['DriverID', 'DriverId']);
      if (!validGuid(id) || emptyGuid(id)) continue;
      const full = composeName(
        textOf(d, ['DriverFullName', 'FullName']),
        textOf(d, ['FirstName']),
        textOf(d, ['LastName', 'Surname'])
      );
      if (!map.has(id) || (full && !map.get(id))) {
        map.set(id, full || '');
      }
    }

    const more = boolOf(textOf(result, ['MoreItemsAvailable']));
    const nextVersion = Number(textOf(result, ['Version']) || version);
    if (!more) break;
    if (!Number.isFinite(nextVersion) || nextVersion <= version) break;
    version = nextVersion;
  }

  const list = Array.from(map.entries()).map(([DriverID, DriverName]) => ({ DriverID, DriverName }));
  driverCache.bySession = sessionId;
  driverCache.ts = now;
  driverCache.list = list;
  return list;
}

function attachDriverNames(rows, drivers) {
  const map = new Map(drivers.map(d => [d.DriverID, d.DriverName]));
  return rows.map(row => {
    const id = validGuid(row.DriverID) && !emptyGuid(row.DriverID) ? row.DriverID : '';
    const driverName = row.DriverName || map.get(id) || '';
    return {
      ...row,
      DriverID: id,
      DriverName: driverName,
      Driver: driverName || id || 'Unassigned'
    };
  });
}

function needsDriverLookup(rows) {
  return rows.some(row => {
    const id = validGuid(row.DriverID) && !emptyGuid(row.DriverID) ? row.DriverID : '';
    const name = String(row.DriverName || '').trim();
    return Boolean(id && !name);
  });
}

function normalizeSafetyEvent(raw) {
  const id = textOf(raw, ['EventTypeID', 'EventTypeId', 'EventType']);
  const eventTypeMap = {
    '2': 'Harsh Braking',
    '3': 'Harsh Acceleration',
    '4': 'Harsh Cornering',
    '5': 'Stop Sign Violation',
    '6': 'Speeding'
  };
  return {
    EventDateTime: textOf(raw, ['EventDateTime', 'DateTime', 'StartTime', 'CreatedDateTime']),
    EventTypeID: id,
    EventType: eventTypeMap[id] || '',
    VehicleID: textOf(raw, ['VehicleID', 'VehicleId']),
    DriverID: textOf(raw, ['DriverID', 'DriverId']),
    DriverName: composeName(
      textOf(raw, ['DriverFullName', 'DriverName']),
      textOf(raw, ['DriverFirstName', 'FirstName']),
      textOf(raw, ['DriverLastName', 'LastName', 'Surname'])
    ),
    DurationSeconds: textOf(raw, ['Duration']),
    Location: textOf(raw, ['Location', 'StreetName']),
    SiteID: textOf(raw, ['SiteID', 'SiteId']),
    Latitude: textOf(raw, ['Latitude', 'Lat']),
    Longitude: textOf(raw, ['Longitude', 'Lon']),
    PeakSpeedKph: textOf(raw, ['PeakSpeed']),
    PostedSpeedKph: textOf(raw, ['PostedSpeed'])
  };
}

function normalizeIdleInterval(raw, vehicleMap) {
  const vehicleId = textOf(raw, ['VehicleID', 'VehicleId', 'VehicleGuid']);
  const vehicle = vehicleMap.get(vehicleId);
  return {
    VehicleID: vehicleId,
    VehicleName: vehicle?.VehicleName || textOf(raw, ['VehicleName', 'Name']),
    Registration: vehicle?.Registration || '',
    VehicleType: vehicle?.VehicleType || '',
    StartTime: textOf(raw, ['IdleStartTime', 'StartTime', 'BeginTime', 'StartDateTime']),
    EndTime: textOf(raw, ['IdleEndTime', 'EndTime', 'FinishTime', 'EndDateTime']),
    DurationMinutes: textOf(raw, ['DurationMinutes', 'DurationMinute', 'IdleDuration', 'Duration']),
    Latitude: textOf(raw, ['Latitude', 'Lat']),
    Longitude: textOf(raw, ['Longitude', 'Lon']),
    Location: textOf(raw, ['Location', 'StreetName'])
  };
}

function subtypeToSafetyLabel(subtype) {
  const s = String(subtype || '').toUpperCase();
  if (s.includes('HARSH_BRAKING') || s.includes('HEAVY_BRAKING')) return 'Harsh Braking';
  if (s.includes('HARSH_ACCELERATION')) return 'Harsh Acceleration';
  if (s.includes('HARSH_LEFT_CORNERING') || s.includes('HARSH_RIGHT_CORNERING') || s.includes('HARSH_CORNERING')) return 'Harsh Cornering';
  if (s.includes('STOP_SIGN')) return 'Stop Sign Violation';
  if (s.includes('SPEED') || s.includes('OVERSPEED') || s.includes('POSTED_SPEED')) return 'Speeding';
  return '';
}

function normalizeOwnerActivity(raw) {
  const subtype = textOf(raw, ['EventSubType']);
  return {
    EventDateTime: textOf(raw, ['ActivityDateTime']),
    EventTypeID: '',
    EventType: subtypeToSafetyLabel(subtype),
    VehicleID: textOf(raw, ['VehicleID', 'VehicleId']),
    DriverID: textOf(raw, ['DriverID', 'DriverId']),
    DriverName: composeName(
      textOf(raw, ['DriverFullName', 'DriverName']),
      textOf(raw, ['DriverFirstName', 'FirstName']),
      textOf(raw, ['DriverLastName', 'LastName', 'Surname'])
    ),
    DurationSeconds: '',
    Location: textOf(raw, ['Location']),
    SiteID: textOf(raw, ['SiteID', 'SiteId']),
    Latitude: textOf(raw, ['Latitude']),
    Longitude: textOf(raw, ['Longitude']),
    PeakSpeedKph: textOf(raw, ['MaxSpeed', 'Speed']),
    PostedSpeedKph: '',
    EventSubType: subtype,
    EventTypeDescription: textOf(raw, ['EventTypeDescription']),
    VehicleName: textOf(raw, ['VehicleName'])
  };
}

function isSafetyLikeSubtype(subtype) {
  const s = String(subtype || '').toUpperCase();
  return s.includes('HARSH') || s.includes('BRAKING') || s.includes('ACCELERATION') || s.includes('CORNER') || s.includes('SPEED') || s.includes('STOP_SIGN');
}

function isSpeedSubtype(subtype) {
  const s = String(subtype || '').toUpperCase();
  return s.includes('SPEED') || s.includes('OVERSPEED') || s.includes('POSTED_SPEED');
}

async function fetchOwnerActivities({ sessionId, startIso, endIso }) {
  const rows = [];
  const chunks = chunkRangeBySize(startIso, endIso, OWNER_ACTIVITY_CHUNK_MS);

  for (const chunk of chunks) {
    let version = 0;
    let loops = 0;
    const chunkStarted = Date.now();

    while (loops < MAX_PAGE_LOOPS && loops < OWNER_ACTIVITY_MAX_LOOPS) {
      if (Date.now() - chunkStarted > OWNER_ACTIVITY_MAX_MS_PER_CHUNK) break;
      loops += 1;

      const envelope = `
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetOwnerPeriodActivity xmlns="${SOAP_NS}">
      <request>
        <Session><SessionId>${xmlEscape(sessionId)}</SessionId></Session>
        <Version>${version}</Version>
        <StartTime>${toUtcNoMs(chunk.start)}</StartTime>
        <EndTime>${toUtcNoMs(chunk.end)}</EndTime>
        <MaximumSerializableEventSubType>SMDP_EVENT_PREAUTH_LOGON_OK</MaximumSerializableEventSubType>
        <FetchTemperatureData>false</FetchTemperatureData>
        <IncludePrivateEvents>true</IncludePrivateEvents>
        <IncludeVehicleName>true</IncludeVehicleName>
        <IDs><guid>${xmlEscape(creds.OwnerID)}</guid></IDs>
      </request>
    </GetOwnerPeriodActivity>
  </soap:Body>
</soap:Envelope>`;

      const parsed = await soapCall('GetOwnerPeriodActivity', envelope);
      const result = getResult(parsed, 'GetOwnerPeriodActivityResponse', 'GetOwnerPeriodActivityResult');
      let state;
      try {
        state = ensureSuccess('GetOwnerPeriodActivity', result);
      } catch (err) {
        // 3000 can be returned on heavy slices; keep moving with partial data instead of hard-failing.
        if (err instanceof NavmanApiError && err.code === '3000') break;
        throw err;
      }
      if (state.noData) break;

      const acts = result?.Activities?.[0]?.VehicleActivity || [];
      for (const act of acts) rows.push(normalizeOwnerActivity(act));

      const more = boolOf(textOf(result, ['MoreItemsAvailable']));
      const nextVersion = Number(textOf(result, ['Version']) || version);
      if (!more) break;
      if (!Number.isFinite(nextVersion) || nextVersion <= version) break;
      version = nextVersion;
    }
  }

  return rows;
}

function isSoftNavmanEntityError(err) {
  return err instanceof NavmanApiError && (err.code === '3000' || err.code === '3002' || err.code === '1166');
}

function uniqueGuids(values) {
  return Array.from(new Set(values.filter(v => validGuid(v) && !emptyGuid(v))));
}

async function tryFetchDrivers(sessionId) {
  try {
    return await fetchDrivers(sessionId);
  } catch (err) {
    if (isSoftNavmanEntityError(err)) return [];
    throw err;
  }
}

async function fallbackVehicleIdsFromActivity({ sessionId, startIso, endIso }) {
  const acts = await fetchOwnerActivities({ sessionId, startIso, endIso });
  return uniqueGuids(acts.map(a => a.VehicleID));
}

async function fetchSafetyRows({
  sessionId,
  startIso,
  endIso,
  eventType,
  requestType,
  userId,
  vehicleIds,
  driverIds
}) {
  const rows = [];
  const chunks = chunkRange(startIso, endIso);

  for (const chunk of chunks) {
    let version = 0;
    let loops = 0;

    while (loops < MAX_PAGE_LOOPS) {
      loops += 1;

      const envelope = `
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetSafetyAnalyticsEvents xmlns="${SOAP_NS}">
      <request>
        <Version>${version}</Version>
        <Session><SessionId>${xmlEscape(sessionId)}</SessionId></Session>
        <RequestType>${xmlEscape(requestType)}</RequestType>
        <UserID>${xmlEscape(userId)}</UserID>
        <OwnerID>${xmlEscape(creds.OwnerID)}</OwnerID>
        ${guidListXml('VehicleIDs', vehicleIds)}
        ${guidListXml('DriverIDs', driverIds)}
        <StartTime>${toUtcNoMs(chunk.start)}</StartTime>
        <EndTime>${toUtcNoMs(chunk.end)}</EndTime>
        <EventType>${eventType}</EventType>
      </request>
    </GetSafetyAnalyticsEvents>
  </soap:Body>
</soap:Envelope>`;

      const parsed = await soapCall('GetSafetyAnalyticsEvents', envelope);
      const result = getResult(parsed, 'GetSafetyAnalyticsEventsResponse', 'GetSafetyAnalyticsEventsResult');
      const state = ensureSuccess('GetSafetyAnalyticsEvents', result);
      if (state.noData) break;

      const events = result?.Events?.[0]?.SafetyAnalyticsEvent || [];
      for (const ev of events) rows.push(normalizeSafetyEvent(ev));

      const more = boolOf(textOf(result, ['MoreItemsAvailable']));
      const nextVersion = Number(textOf(result, ['Version']) || version);
      if (!more) break;
      if (!Number.isFinite(nextVersion) || nextVersion <= version) break;
      version = nextVersion;
    }
  }

  return rows;
}

async function fetchIdleRows({ sessionId, startIso, endIso, vehicleIds, vehicleList = [] }) {
  const rows = [];
  const chunks = chunkRange(startIso, endIso);
  let vehicles = Array.isArray(vehicleList) ? vehicleList : [];
  if (vehicles.length === 0) {
    try {
      vehicles = await fetchVehicles(sessionId);
    } catch (err) {
      if (!isSoftNavmanEntityError(err)) throw err;
      vehicles = [];
    }
  }
  const vehicleMap = new Map(vehicles.map(v => [v.VehicleID, v]));

  for (const chunk of chunks) {
    let version = 0;
    let loops = 0;

    while (loops < MAX_PAGE_LOOPS) {
      loops += 1;

      const envelope = `
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetGpsIdleByVehicle xmlns="${SOAP_NS}">
      <request>
        <Session><SessionId>${xmlEscape(sessionId)}</SessionId></Session>
        <Version>${version}</Version>
        <StartTime>${toUtcNoMs(chunk.start)}</StartTime>
        <EndTime>${toUtcNoMs(chunk.end)}</EndTime>
        <OwnerID>${xmlEscape(creds.OwnerID)}</OwnerID>
        ${guidListXml('VehicleIDs', vehicleIds)}
        <DistanceMetre>0</DistanceMetre>
        <MinimumDurationMinute>0</MinimumDurationMinute>
      </request>
    </GetGpsIdleByVehicle>
  </soap:Body>
</soap:Envelope>`;

      const parsed = await soapCall('GetGpsIdleByVehicle', envelope);
      const result = getResult(parsed, 'GetGpsIdleByVehicleResponse', 'GetGpsIdleByVehicleResult');
      let state;
      try {
        state = ensureSuccess('GetGpsIdleByVehicle', result);
      } catch (err) {
        if (err instanceof NavmanApiError && err.code === '3000') break;
        throw err;
      }
      if (state.noData) break;

      const intervals =
        result?.Intervals?.[0]?.GpsIdleInterval ||
        result?.IdleRecords?.[0]?.IdleRecord ||
        [];

      for (const interval of intervals) rows.push(normalizeIdleInterval(interval, vehicleMap));

      const more = boolOf(textOf(result, ['MoreItemsAvailable']));
      const nextVersion = Number(textOf(result, ['Version']) || version);
      if (!more) break;
      if (!Number.isFinite(nextVersion) || nextVersion <= version) break;
      version = nextVersion;
    }
  }

  return rows;
}

function geotabConfigured() {
  return Boolean(geoCreds.Database && geoCreds.UserName && geoCreds.Password);
}

function geotabSessionValid() {
  return Boolean(
    geoCreds.SessionID &&
    geoCreds.SessionServer &&
    geoCreds.SessionDatabase &&
    geoCreds.SessionUserName &&
    Date.now() < (geoCreds.SessionExpiry - 30_000)
  );
}

function geotabRuntimeCredentials() {
  return {
    database: geoCreds.SessionDatabase || geoCreds.Database,
    userName: geoCreds.SessionUserName || geoCreds.UserName,
    sessionId: geoCreds.SessionID
  };
}

function resetGeotabSession(clearCredentials = false) {
  geoCreds.SessionServer = '';
  geoCreds.SessionDatabase = '';
  geoCreds.SessionUserName = '';
  geoCreds.SessionID = '';
  geoCreds.SessionExpiry = 0;
  if (clearCredentials) {
    geoCreds.Server = 'my.geotab.com';
    geoCreds.Database = '';
    geoCreds.UserName = '';
    geoCreds.Password = '';
  }
  geotabCache.bySession = '';
  geotabCache.ts = 0;
  geotabCache.devices = [];
  geotabCache.users = [];
  geotabCache.rules = [];
  responseCache.clear();
}

async function geotabHttpCall(host, method, params) {
  const response = await axios.post(`https://${host}/apiv1`, {
    method,
    params
  }, {
    headers: {
      'Content-Type': 'application/json',
      Accept: 'application/json'
    },
    timeout: 90000,
    validateStatus: () => true
  });

  let payload = response.data;
  if (typeof payload === 'string') {
    try {
      payload = JSON.parse(payload);
    } catch (_err) {
      throw new GeotabApiError(method, String(response.status), payload.slice(0, 400), null);
    }
  }

  if (response.status >= 400) {
    const detailMessage = payload?.error?.message || payload?.error?.name || `HTTP ${response.status}`;
    throw new GeotabApiError(method, String(response.status), detailMessage, payload?.error || payload);
  }

  if (payload?.error) {
    const err = payload.error;
    throw new GeotabApiError(method, String(err.code || err.name || ''), err.message || 'Geotab error', err);
  }

  if (!payload || !Object.prototype.hasOwnProperty.call(payload, 'result')) {
    throw new GeotabApiError(method, '', 'Invalid Geotab response format', payload);
  }

  return payload.result;
}

async function geotabAuthenticate(force = false) {
  if (!force && geotabSessionValid()) {
    return geotabRuntimeCredentials();
  }

  if (!geotabConfigured()) {
    throw new Error('Geotab credentials are not configured. Set server, database, username, and password first.');
  }

  let host = geoCreds.Server || 'my.geotab.com';
  const authParams = {
    database: geoCreds.Database,
    userName: geoCreds.UserName,
    password: geoCreds.Password
  };

  let authResult = await geotabHttpCall(host, 'Authenticate', authParams);
  if (authResult?.path && authResult.path !== 'ThisServer' && authResult.path !== host) {
    host = authResult.path;
    authResult = await geotabHttpCall(host, 'Authenticate', authParams);
  }

  const c = authResult?.credentials || authResult;
  const sessionId = String(c?.sessionId || c?.SessionId || '').trim();
  const db = String(c?.database || c?.Database || geoCreds.Database).trim();
  const userName = String(c?.userName || c?.UserName || geoCreds.UserName).trim();
  if (!sessionId || !db || !userName) {
    throw new Error('Geotab authentication did not return valid session credentials.');
  }

  geoCreds.SessionServer = host;
  geoCreds.SessionDatabase = db;
  geoCreds.SessionUserName = userName;
  geoCreds.SessionID = sessionId;
  geoCreds.SessionExpiry = Date.now() + GEO_AUTH_TTL_MS;
  saveConfig();

  geotabCache.bySession = '';
  geotabCache.ts = 0;
  geotabCache.devices = [];
  geotabCache.users = [];
  geotabCache.rules = [];

  return geotabRuntimeCredentials();
}

function isGeotabAuthError(error) {
  if (!(error instanceof GeotabApiError)) return false;
  const text = `${error.code} ${error.message}`.toLowerCase();
  return (
    text.includes('session') ||
    text.includes('auth') ||
    text.includes('credential') ||
    text.includes('invalid user') ||
    text.includes('token')
  );
}

async function geotabCall(method, params = {}, { retry = true } = {}) {
  await geotabAuthenticate(false);
  let host = geoCreds.SessionServer || geoCreds.Server || 'my.geotab.com';
  try {
    return await geotabHttpCall(host, method, {
      ...params,
      credentials: geotabRuntimeCredentials()
    });
  } catch (error) {
    if (retry && isGeotabAuthError(error)) {
      await geotabAuthenticate(true);
      host = geoCreds.SessionServer || geoCreds.Server || 'my.geotab.com';
      return geotabHttpCall(host, method, {
        ...params,
        credentials: geotabRuntimeCredentials()
      });
    }
    throw error;
  }
}

function geotabCoords(value) {
  const sources = [value, value?.location, value?.point, value?.startPoint, value?.stopPoint];
  for (const src of sources) {
    if (!src || typeof src !== 'object') continue;
    const lat = src.latitude ?? src.lat ?? src.y;
    const lon = src.longitude ?? src.lng ?? src.lon ?? src.x;
    if (lat !== undefined && lon !== undefined) {
      return { lat: safeNumber(lat, 0), lon: safeNumber(lon, 0) };
    }
  }
  return { lat: 0, lon: 0 };
}

function geotabTypeId(eventType) {
  if (eventType === 'Harsh Braking') return '2';
  if (eventType === 'Harsh Acceleration') return '3';
  if (eventType === 'Harsh Cornering') return '4';
  if (eventType === 'Stop Sign Violation') return '5';
  if (eventType === 'Speeding') return '6';
  return '';
}

function geotabRuleToType(ruleName) {
  const s = String(ruleName || '').toLowerCase();
  if (!s) return '';
  if (s.includes('speed')) return 'Speeding';
  if (s.includes('brak')) return 'Harsh Braking';
  if (s.includes('accel')) return 'Harsh Acceleration';
  if (s.includes('corner') || s.includes('turn')) return 'Harsh Cornering';
  if (s.includes('stop sign')) return 'Stop Sign Violation';
  return '';
}

async function fetchGeotabDevices() {
  const now = Date.now();
  if (
    geotabCache.bySession === geoCreds.SessionID &&
    geotabCache.devices.length > 0 &&
    now - geotabCache.ts < VEHICLE_CACHE_TTL_MS
  ) {
    return geotabCache.devices;
  }

  const result = await geotabCall('Get', {
    typeName: 'Device',
    search: {},
    resultsLimit: GEO_MAX_RESULTS
  });

  const devices = (Array.isArray(result) ? result : []).map(d => ({
    VehicleID: geotabRefId(d.id || d),
    VehicleName: String(d.name || d.Name || '').trim(),
    Registration: String(d.licensePlate || d.LicensePlate || '').trim(),
    VehicleType: String(d.vehicleType || '').trim()
  })).filter(d => d.VehicleID);

  geotabCache.bySession = geoCreds.SessionID;
  geotabCache.ts = now;
  geotabCache.devices = devices;
  return devices;
}

async function fetchGeotabUsers() {
  const now = Date.now();
  if (
    geotabCache.bySession === geoCreds.SessionID &&
    geotabCache.users.length > 0 &&
    now - geotabCache.ts < VEHICLE_CACHE_TTL_MS
  ) {
    return geotabCache.users;
  }

  const result = await geotabCall('Get', {
    typeName: 'User',
    search: {},
    resultsLimit: GEO_MAX_RESULTS
  });

  const users = (Array.isArray(result) ? result : []).map(u => {
    const id = geotabRefId(u.id || u);
    const name = composeName(u.name, u.firstName, u.lastName);
    return {
      DriverID: id,
      DriverName: name || String(u.userName || '').trim()
    };
  }).filter(u => u.DriverID);

  geotabCache.bySession = geoCreds.SessionID;
  geotabCache.ts = now;
  geotabCache.users = users;
  return users;
}

async function fetchGeotabRules() {
  const now = Date.now();
  if (
    geotabCache.bySession === geoCreds.SessionID &&
    geotabCache.rules.length > 0 &&
    now - geotabCache.ts < VEHICLE_CACHE_TTL_MS
  ) {
    return geotabCache.rules;
  }

  let result;
  try {
    result = await geotabCall('Get', {
      typeName: 'ExceptionRule',
      search: {},
      resultsLimit: GEO_MAX_RESULTS
    });
  } catch (_err) {
    result = await geotabCall('Get', {
      typeName: 'Rule',
      search: {},
      resultsLimit: GEO_MAX_RESULTS
    });
  }

  const rules = (Array.isArray(result) ? result : []).map(r => ({
    RuleID: geotabRefId(r.id || r),
    RuleName: String(r.name || r.Name || '').trim()
  })).filter(r => r.RuleID);

  geotabCache.bySession = geoCreds.SessionID;
  geotabCache.ts = now;
  geotabCache.rules = rules;
  return rules;
}

async function fetchGeotabTrips({ startIso, endIso, vehicleIds, driverIds }) {
  const chunks = chunkRangeBySize(startIso, endIso, GEO_MAX_CHUNK_MS);
  const deviceSet = new Set(vehicleIds || []);
  const driverSet = new Set(driverIds || []);
  const all = [];

  for (const chunk of chunks) {
    const rows = await geotabCall('Get', {
      typeName: 'Trip',
      search: {
        fromDate: chunk.start,
        toDate: chunk.end
      },
      resultsLimit: GEO_MAX_RESULTS
    });

    for (const row of Array.isArray(rows) ? rows : []) {
      const deviceId = geotabRefId(row.device);
      const driverId = geotabRefId(row.driver || row.user);
      if (deviceSet.size > 0 && !deviceSet.has(deviceId)) continue;
      if (driverSet.size > 0 && !driverSet.has(driverId)) continue;
      all.push(row);
    }
  }

  return all;
}

async function fetchGeotabExceptionEvents({ startIso, endIso, vehicleIds, driverIds }) {
  const chunks = chunkRangeBySize(startIso, endIso, GEO_MAX_CHUNK_MS);
  const deviceSet = new Set(vehicleIds || []);
  const driverSet = new Set(driverIds || []);
  const all = [];

  for (const chunk of chunks) {
    const rows = await geotabCall('Get', {
      typeName: 'ExceptionEvent',
      search: {
        fromDate: chunk.start,
        toDate: chunk.end
      },
      resultsLimit: GEO_MAX_RESULTS
    });

    for (const row of Array.isArray(rows) ? rows : []) {
      const deviceId = geotabRefId(row.device);
      const driverId = geotabRefId(row.driver || row.user);
      if (deviceSet.size > 0 && !deviceSet.has(deviceId)) continue;
      if (driverSet.size > 0 && !driverSet.has(driverId)) continue;
      all.push(row);
    }
  }

  return all;
}

function geotabTripToIdleRow(trip, deviceMap, driverMap) {
  const deviceId = geotabRefId(trip.device);
  const driverId = geotabRefId(trip.driver || trip.user);
  const coords = geotabCoords(trip.startPoint || trip);
  const durationMinutes = parseDurationMinutes(
    trip.idlingDuration || trip.idleDuration || trip.idlingTime || 0
  );
  return {
    VehicleID: deviceId,
    VehicleName: geotabRefName(trip.device) || (deviceMap.get(deviceId)?.VehicleName || ''),
    Registration: deviceMap.get(deviceId)?.Registration || '',
    VehicleType: deviceMap.get(deviceId)?.VehicleType || '',
    StartTime: geotabToIso(trip.start),
    EndTime: geotabToIso(trip.stop),
    DurationMinutes: durationMinutes.toFixed(2),
    Latitude: coords.lat ? String(coords.lat) : '',
    Longitude: coords.lon ? String(coords.lon) : '',
    Location: '',
    DriverID: driverId,
    DriverName: geotabRefName(trip.driver || trip.user) || (driverMap.get(driverId)?.DriverName || ''),
    Driver: geotabRefName(trip.driver || trip.user) || (driverMap.get(driverId)?.DriverName || '') || driverId || 'Unassigned'
  };
}

function geotabTripToSpeedRow(trip, deviceMap, driverMap) {
  const deviceId = geotabRefId(trip.device);
  const driverId = geotabRefId(trip.driver || trip.user);
  const coords = geotabCoords(trip.startPoint || trip);
  const peakSpeed = safeNumber(trip.maxSpeed ?? trip.maximumSpeed ?? trip.speed, 0);
  const postedSpeed = safeNumber(trip.speedLimit ?? trip.postedSpeed, 0);
  return {
    EventDateTime: geotabToIso(trip.start),
    EventTypeID: '6',
    EventType: 'Speeding',
    VehicleID: deviceId,
    DriverID: driverId,
    DriverName: geotabRefName(trip.driver || trip.user) || (driverMap.get(driverId)?.DriverName || ''),
    Driver: geotabRefName(trip.driver || trip.user) || (driverMap.get(driverId)?.DriverName || '') || driverId || 'Unassigned',
    DurationSeconds: String(parseDurationSeconds(trip.duration || trip.drivingDuration || 0).toFixed(0)),
    Location: '',
    SiteID: '',
    Latitude: coords.lat ? String(coords.lat) : '',
    Longitude: coords.lon ? String(coords.lon) : '',
    PeakSpeedKph: peakSpeed ? String(peakSpeed) : '',
    PostedSpeedKph: postedSpeed ? String(postedSpeed) : '',
    VehicleName: geotabRefName(trip.device) || (deviceMap.get(deviceId)?.VehicleName || ''),
    EventSubType: 'TRIP_MAX_SPEED'
  };
}

function geotabExceptionToSafetyRow(event, deviceMap, driverMap, ruleMap) {
  const deviceId = geotabRefId(event.device);
  const driverId = geotabRefId(event.driver || event.user);
  const ruleId = geotabRefId(event.rule);
  const ruleName = geotabRefName(event.rule) || ruleMap.get(ruleId) || '';
  const eventType = geotabRuleToType(ruleName);
  const coords = geotabCoords(event);
  return {
    EventDateTime: geotabToIso(event.activeFrom || event.start || event.dateTime),
    EventTypeID: geotabTypeId(eventType),
    EventType: eventType || 'Other',
    VehicleID: deviceId,
    DriverID: driverId,
    DriverName: geotabRefName(event.driver || event.user) || (driverMap.get(driverId)?.DriverName || ''),
    Driver: geotabRefName(event.driver || event.user) || (driverMap.get(driverId)?.DriverName || '') || driverId || 'Unassigned',
    DurationSeconds: String(parseDurationSeconds(event.duration || event.activeDuration || 0).toFixed(0)),
    Location: '',
    SiteID: '',
    Latitude: coords.lat ? String(coords.lat) : '',
    Longitude: coords.lon ? String(coords.lon) : '',
    PeakSpeedKph: String(safeNumber(event.speed ?? event.maxSpeed ?? event.maximumSpeed, 0) || ''),
    PostedSpeedKph: String(safeNumber(event.speedLimit ?? event.postedSpeed, 0) || ''),
    VehicleName: geotabRefName(event.device) || (deviceMap.get(deviceId)?.VehicleName || ''),
    EventSubType: ruleName || ''
  };
}

async function fetchGeotabSafetyRows({ startIso, endIso, vehicleIds, driverIds }) {
  const [devices, users, rules] = await Promise.all([
    fetchGeotabDevices(),
    fetchGeotabUsers(),
    fetchGeotabRules()
  ]);

  const deviceMap = new Map(devices.map(d => [d.VehicleID, d]));
  const driverMap = new Map(users.map(u => [u.DriverID, u]));
  const ruleMap = new Map(rules.map(r => [r.RuleID, r.RuleName]));

  const events = await fetchGeotabExceptionEvents({ startIso, endIso, vehicleIds, driverIds });
  let rows = events.map(ev => geotabExceptionToSafetyRow(ev, deviceMap, driverMap, ruleMap));

  if (rows.length === 0) {
    const trips = await fetchGeotabTrips({ startIso, endIso, vehicleIds, driverIds });
    rows = trips.map(trip => geotabTripToSpeedRow(trip, deviceMap, driverMap))
      .filter(r => safeNumber(r.PeakSpeedKph, 0) > 0);
    return { rows, source: 'TripFallback' };
  }

  return { rows, source: 'ExceptionEvent' };
}

async function fetchGeotabSpeedRows({ startIso, endIso, vehicleIds, driverIds }) {
  const [devices, users, rules] = await Promise.all([
    fetchGeotabDevices(),
    fetchGeotabUsers(),
    fetchGeotabRules()
  ]);

  const deviceMap = new Map(devices.map(d => [d.VehicleID, d]));
  const driverMap = new Map(users.map(u => [u.DriverID, u]));
  const ruleMap = new Map(rules.map(r => [r.RuleID, r.RuleName]));

  let rows = [];
  try {
    const events = await fetchGeotabExceptionEvents({ startIso, endIso, vehicleIds, driverIds });
    rows = events
      .map(ev => geotabExceptionToSafetyRow(ev, deviceMap, driverMap, ruleMap))
      .filter(r => r.EventType === 'Speeding');
  } catch (_err) {
    rows = [];
  }

  if (rows.length > 0) {
    return { rows, source: 'ExceptionEvent' };
  }

  const trips = await fetchGeotabTrips({ startIso, endIso, vehicleIds, driverIds });
  rows = trips
    .map(trip => geotabTripToSpeedRow(trip, deviceMap, driverMap))
    .filter(r => safeNumber(r.PeakSpeedKph, 0) > 0);
  return { rows, source: 'Trip' };
}

async function fetchGeotabIdleRows({ startIso, endIso, vehicleIds, driverIds }) {
  const [devices, users] = await Promise.all([
    fetchGeotabDevices(),
    fetchGeotabUsers()
  ]);

  const deviceMap = new Map(devices.map(d => [d.VehicleID, d]));
  const driverMap = new Map(users.map(u => [u.DriverID, u]));

  const trips = await fetchGeotabTrips({ startIso, endIso, vehicleIds, driverIds });
  const rows = trips
    .map(trip => geotabTripToIdleRow(trip, deviceMap, driverMap))
    .filter(row => safeNumber(row.DurationMinutes, 0) > 0);

  return { rows, source: 'Trip' };
}

function errorPayload(error) {
  if (error instanceof NavmanApiError) {
    const status = error.code === '1238' ? 401 : (error.code === '3001' ? 429 : 502);
    return {
      status,
      body: {
        ok: false,
        action: error.action,
        code: error.code || '',
        message: error.message,
        statusMessage: error.statusMessage || null
      }
    };
  }

  if (error instanceof GeotabApiError) {
    const status = isGeotabAuthError(error) ? 401 : 502;
    return {
      status,
      body: {
        ok: false,
        method: error.method,
        code: error.code || '',
        message: error.message,
        details: error.details || null
      }
    };
  }

  return {
    status: 500,
    body: {
      ok: false,
      message: String(error.message || error)
    }
  };
}

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/api/defaults', (_req, res) => {
  res.json({
    OwnerID: creds.OwnerID,
    UserID: creds.UserID,
    SessionSet: Boolean(creds.SessionID),
    Navman: {
      OwnerID: creds.OwnerID,
      UserID: creds.UserID,
      SessionSet: Boolean(creds.SessionID),
      AutoLoginReady: navmanLoginReady(),
      SessionExpiresAtUtc: creds.SessionID ? new Date(navmanSessionState.expiresAt).toISOString() : ''
    },
    Geotab: {
      Server: geoCreds.Server,
      Database: geoCreds.Database,
      UserName: geoCreds.UserName,
      Configured: geotabConfigured(),
      SessionSet: geotabSessionValid()
    }
  });
});

app.post('/api/session', (req, res) => {
  const sid = String(req.body?.sid || req.query?.sid || '').trim();
  if (!sid) return res.status(400).json({ ok: false, message: 'sid required' });

  setNavmanSession(sid);

  return res.json({ ok: true, SessionID: sid });
});

app.post('/api/logoff', async (_req, res) => {
  try {
    const sid = creds.SessionID;
    if (sid) {
      const envelope = `
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <DoLogoff xmlns="${SOAP_NS}">
      <request>
        <Session><SessionId>${xmlEscape(sid)}</SessionId></Session>
      </request>
    </DoLogoff>
  </soap:Body>
</soap:Envelope>`;
      await soapCall('DoLogoff', envelope);
    }
  } catch (_err) {
    // Intentionally swallow logoff errors so user can still clear local session.
  }

  setNavmanSession('');
  return res.json({ ok: true });
});

app.post('/api/geotab/config', async (req, res) => {
  try {
    const body = req.body || {};
    if (Object.prototype.hasOwnProperty.call(body, 'server')) {
      geoCreds.Server = String(body.server || '').trim() || 'my.geotab.com';
    }
    if (Object.prototype.hasOwnProperty.call(body, 'database')) {
      geoCreds.Database = String(body.database || '').trim();
    }
    if (Object.prototype.hasOwnProperty.call(body, 'userName')) {
      geoCreds.UserName = String(body.userName || '').trim();
    }
    if (Object.prototype.hasOwnProperty.call(body, 'password')) {
      const next = String(body.password || '');
      if (next) geoCreds.Password = next;
    }
    if (body.clearPassword === true) {
      geoCreds.Password = '';
    }

    resetGeotabSession(false);
    saveConfig();

    if (body.authenticate === true) {
      await geotabAuthenticate(true);
    }

    return res.json({
      ok: true,
      Geotab: {
        Server: geoCreds.Server,
        Database: geoCreds.Database,
        UserName: geoCreds.UserName,
        Configured: geotabConfigured(),
        SessionSet: geotabSessionValid()
      }
    });
  } catch (err) {
    const payload = errorPayload(err);
    return res.status(payload.status).json(payload.body);
  }
});

app.post('/api/geotab/auth', async (_req, res) => {
  try {
    await geotabAuthenticate(true);
    return res.json({
      ok: true,
      Geotab: {
        Server: geoCreds.SessionServer || geoCreds.Server,
        Database: geoCreds.SessionDatabase || geoCreds.Database,
        UserName: geoCreds.SessionUserName || geoCreds.UserName,
        SessionSet: geotabSessionValid()
      }
    });
  } catch (err) {
    const payload = errorPayload(err);
    return res.status(payload.status).json(payload.body);
  }
});

app.post('/api/geotab/logoff', async (_req, res) => {
  try {
    if (geotabSessionValid()) {
      await geotabCall('Logoff', {}, { retry: false });
    }
  } catch (_err) {
    // swallow logoff errors, we still clear local session
  }

  resetGeotabSession(false);
  saveConfig();
  return res.json({ ok: true });
});

app.get('/api/safety-events', async (req, res) => {
  const key = cacheKey('safety', req);
  const cached = getCached(key);
  if (cached) return res.json({ ...cached, cached: true });
  try {
    const payload = await withNavmanSessionRetry(async (sessionId) => {
      const { start, end } = clampRange(req.query.start, req.query.end);
      const requestType = String(req.query.requestType || 'Vehicle').toLowerCase() === 'driver' ? 'Driver' : 'Vehicle';
      const eventType = Number.isFinite(Number(req.query.eventType)) ? Number(req.query.eventType) : -1;

      let vehicleIds = parseGuidList(req.query.vehicles);
      const driverIds = parseGuidList(req.query.drivers);

      let rows = [];
      let source = 'GetSafetyAnalyticsEvents';
      if (requestType === 'Vehicle' && vehicleIds.length === 0) {
        // Avoid expensive/fragile GetVehicles lookup when no explicit filter is supplied.
        const activities = await fetchOwnerActivities({ sessionId, startIso: start, endIso: end });
        rows = activities.filter(r => isSafetyLikeSubtype(r.EventSubType));
        source = 'GetOwnerPeriodActivityFallback';
      } else {
        try {
          rows = await fetchSafetyRows({
            sessionId,
            startIso: start,
            endIso: end,
            eventType,
            requestType,
            userId: creds.UserID,
            vehicleIds,
            driverIds
          });
        } catch (innerErr) {
          if (isSoftNavmanEntityError(innerErr)) {
            const activities = await fetchOwnerActivities({ sessionId, startIso: start, endIso: end });
            rows = activities.filter(r => isSafetyLikeSubtype(r.EventSubType));
            source = 'GetOwnerPeriodActivityFallback';
          } else {
            throw innerErr;
          }
        }
      }

      const drivers = needsDriverLookup(rows) ? await tryFetchDrivers(sessionId) : [];
      rows = attachDriverNames(rows, drivers);

      const resolvedVehicleCount = vehicleIds.length || uniqueGuids(rows.map(r => r.VehicleID)).length;

      return {
        ok: true,
        count: rows.length,
        rows,
        meta: {
          start,
          end,
          requestType,
          eventType,
          vehicleCount: resolvedVehicleCount,
          driverCount: driverIds.length,
          mappedDrivers: drivers.length,
          source
        }
      };
    });
    setCached(key, payload);
    return res.json(payload);
  } catch (err) {
    const payload = errorPayload(err);
    return res.status(payload.status).json(payload.body);
  }
});

app.get('/api/speeding-events', async (req, res) => {
  const key = cacheKey('speeding', req);
  const cached = getCached(key);
  if (cached) return res.json({ ...cached, cached: true });
  try {
    const payload = await withNavmanSessionRetry(async (sessionId) => {
      const { start, end } = clampRange(req.query.start, req.query.end);
      let vehicleIds = parseGuidList(req.query.vehicles);
      const driverIds = parseGuidList(req.query.drivers);

      let rows = [];
      let source = 'GetSafetyAnalyticsEvents';
      if (vehicleIds.length === 0) {
        // Same strategy as safety endpoint: no explicit IDs => use owner activity fallback directly.
        const activities = await fetchOwnerActivities({ sessionId, startIso: start, endIso: end });
        rows = activities.filter(r => isSpeedSubtype(r.EventSubType));
        source = 'GetOwnerPeriodActivityFallback';
      } else {
        try {
          rows = await fetchSafetyRows({
            sessionId,
            startIso: start,
            endIso: end,
            eventType: 6,
            requestType: 'Vehicle',
            userId: creds.UserID,
            vehicleIds,
            driverIds
          });
        } catch (innerErr) {
          if (isSoftNavmanEntityError(innerErr)) {
            const activities = await fetchOwnerActivities({ sessionId, startIso: start, endIso: end });
            rows = activities.filter(r => isSpeedSubtype(r.EventSubType));
            source = 'GetOwnerPeriodActivityFallback';
          } else {
            throw innerErr;
          }
        }
      }

      const drivers = needsDriverLookup(rows) ? await tryFetchDrivers(sessionId) : [];
      rows = attachDriverNames(rows, drivers);

      return {
        ok: true,
        count: rows.length,
        rows,
        meta: {
          start,
          end,
          eventType: 6,
          vehicleCount: vehicleIds.length || uniqueGuids(rows.map(r => r.VehicleID)).length,
          driverCount: driverIds.length,
          mappedDrivers: drivers.length,
          source
        }
      };
    });
    setCached(key, payload);
    return res.json(payload);
  } catch (err) {
    const payload = errorPayload(err);
    return res.status(payload.status).json(payload.body);
  }
});

app.get('/api/idle-events', async (req, res) => {
  const key = cacheKey('idle', req);
  const cached = getCached(key);
  if (cached) return res.json({ ...cached, cached: true });
  try {
    const payload = await withNavmanSessionRetry(async (sessionId) => {
      const { start, end } = clampRange(req.query.start, req.query.end);

      let vehicleIds = parseGuidList(req.query.vehicles);
      let vehicleList = [];
      if (vehicleIds.length === 0) {
        try {
          vehicleList = await fetchVehicles(sessionId);
          vehicleIds = vehicleList.map(v => v.VehicleID);
        } catch (err) {
          if (isSoftNavmanEntityError(err)) {
            vehicleIds = [];
          } else {
            throw err;
          }
        }
        if (vehicleIds.length === 0) {
          vehicleIds = await fallbackVehicleIdsFromActivity({
            sessionId,
            startIso: start,
            endIso: end
          });
        }
      }

      if (vehicleIds.length === 0) {
        return {
          ok: true,
          count: 0,
          rows: [],
          meta: {
            start,
            end,
            vehicleCount: 0,
            source: 'NoVehicleIDsResolved'
          }
        };
      }

      const rows = await fetchIdleRows({
        sessionId,
        startIso: start,
        endIso: end,
        vehicleIds,
        vehicleList
      });

      return {
        ok: true,
        count: rows.length,
        rows,
        meta: {
          start,
          end,
          vehicleCount: vehicleIds.length
        }
      };
    });
    setCached(key, payload);
    return res.json(payload);
  } catch (err) {
    const payload = errorPayload(err);
    return res.status(payload.status).json(payload.body);
  }
});

app.get('/api/geotab/safety-events', async (req, res) => {
  const key = cacheKey('geotab-safety', req);
  const cached = getCached(key);
  if (cached) return res.json({ ...cached, cached: true });

  try {
    const { start, end } = clampRange(req.query.start, req.query.end);
    const vehicleIds = parseCsvList(req.query.vehicles);
    const driverIds = parseCsvList(req.query.drivers);

    const result = await fetchGeotabSafetyRows({
      startIso: start,
      endIso: end,
      vehicleIds,
      driverIds
    });

    const rows = (result.rows || []).map(r => ({
      ...r,
      Source: 'Geotab NZ',
      Country: 'New Zealand'
    }));

    const payload = {
      ok: true,
      count: rows.length,
      rows,
      meta: {
        start,
        end,
        vehicleCount: vehicleIds.length,
        driverCount: driverIds.length,
        source: result.source
      }
    };

    setCached(key, payload);
    return res.json(payload);
  } catch (err) {
    const payload = errorPayload(err);
    return res.status(payload.status).json(payload.body);
  }
});

app.get('/api/geotab/speeding-events', async (req, res) => {
  const key = cacheKey('geotab-speeding', req);
  const cached = getCached(key);
  if (cached) return res.json({ ...cached, cached: true });

  try {
    const { start, end } = clampRange(req.query.start, req.query.end);
    const vehicleIds = parseCsvList(req.query.vehicles);
    const driverIds = parseCsvList(req.query.drivers);

    const result = await fetchGeotabSpeedRows({
      startIso: start,
      endIso: end,
      vehicleIds,
      driverIds
    });

    const rows = (result.rows || []).map(r => ({
      ...r,
      Source: 'Geotab NZ',
      Country: 'New Zealand'
    }));

    const payload = {
      ok: true,
      count: rows.length,
      rows,
      meta: {
        start,
        end,
        vehicleCount: vehicleIds.length,
        driverCount: driverIds.length,
        source: result.source
      }
    };

    setCached(key, payload);
    return res.json(payload);
  } catch (err) {
    const payload = errorPayload(err);
    return res.status(payload.status).json(payload.body);
  }
});

app.get('/api/geotab/idle-events', async (req, res) => {
  const key = cacheKey('geotab-idle', req);
  const cached = getCached(key);
  if (cached) return res.json({ ...cached, cached: true });

  try {
    const { start, end } = clampRange(req.query.start, req.query.end);
    const vehicleIds = parseCsvList(req.query.vehicles);
    const driverIds = parseCsvList(req.query.drivers);

    const result = await fetchGeotabIdleRows({
      startIso: start,
      endIso: end,
      vehicleIds,
      driverIds
    });

    const rows = (result.rows || []).map(r => ({
      ...r,
      Source: 'Geotab NZ',
      Country: 'New Zealand'
    }));

    const payload = {
      ok: true,
      count: rows.length,
      rows,
      meta: {
        start,
        end,
        vehicleCount: vehicleIds.length,
        driverCount: driverIds.length,
        source: result.source
      }
    };

    setCached(key, payload);
    return res.json(payload);
  } catch (err) {
    const payload = errorPayload(err);
    return res.status(payload.status).json(payload.body);
  }
});

process.on('SIGINT', () => process.exit(0));
app.listen(PORT, () => {
  console.log(`Dashboard proxy running on http://localhost:${PORT}`);
});
