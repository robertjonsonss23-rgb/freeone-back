/* ============================================================
   GOTHAM / DAREDEVIL BACKEND — clean rewrite (Option C)
   ------------------------------------------------------------
   Goals:
   - DB is the single source of truth (no in-memory queues)
   - Atomic per-run claim (no race conditions, no duplicates)
   - Per (link, label) sequencing instead of global 10-min cooldown
   - Stuck detection based on processingStartedAt, NOT createdAt
   - No silent cancellation from execute-time minimum checks
   - Same HTTP surface so the existing frontend works unchanged
   ============================================================ */

const express  = require('express');
const cors     = require('cors');
const axios    = require('axios');
const mongoose = require('mongoose');

const app  = express();
const PORT = process.env.PORT || 5000;

/* ---- CORS ----
   Set ALLOWED_ORIGINS in Render to a comma-separated list of your frontend
   origins, e.g. "https://your-new-frontend.vercel.app,http://localhost:5173".
   If unset, all origins are allowed (fine for testing, tighten for prod). */
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

app.use(cors({
  origin(origin, cb) {
    if (!origin || ALLOWED_ORIGINS.length === 0) return cb(null, true);
    if (ALLOWED_ORIGINS.includes(origin)) return cb(null, true);
    return cb(new Error(`Origin ${origin} not allowed by CORS`));
  },
}));
app.use(express.json({ limit: '1mb' }));

/* ============================================================
   CONFIG
   ============================================================ */
const MONGODB_URI = process.env.MONGODB_URI;
if (!MONGODB_URI) {
  console.error('❌ FATAL: MONGODB_URI environment variable is required.');
  console.error('   Set it in Render → Environment → Add Environment Variable.');
  process.exit(1);
}

// How often the scheduler tick fires (ms)
const TICK_INTERVAL_MS = 5_000;

// A run marked "processing" longer than this is considered stuck and recovered
const STUCK_PROCESSING_MS = 5 * 60 * 1000; // 5 minutes

// Max attempts when provider keeps returning "active order" / "wait"
const MAX_RETRY_ATTEMPTS = 8;

// Backoff between retries when provider is busy (ms)
const RETRY_BACKOFF_MS = 3 * 60 * 1000; // 3 minutes

// How long a single provider HTTP call is allowed to take
const PROVIDER_HTTP_TIMEOUT_MS = 30_000;

// Hard upper bound on runs claimed per tick (safety guard)
const MAX_CLAIMS_PER_TICK = 50;

/* ============================================================
   LOGGING
   ============================================================ */
function log(...args)  { console.log(new Date().toISOString(), ...args); }
function warn(...args) { console.warn(new Date().toISOString(), '⚠️', ...args); }
function err(...args)  { console.error(new Date().toISOString(), '❌', ...args); }

/* ============================================================
   SCHEMAS
   ============================================================ */
const RunSchema = new mongoose.Schema({
  // Stable string id (no float collisions)
  id:                  { type: String, required: true, index: true, unique: true },
  schedulerOrderId:    { type: String, required: true, index: true },
  label:               { type: String, required: true }, // VIEWS / LIKES / SHARES / SAVES / REPOSTS / COMMENTS
  apiUrl:              { type: String, required: true },
  apiKey:              { type: String, required: true },
  service:             { type: String, required: true },
  link:                { type: String, required: true, index: true },
  quantity:            { type: Number, required: true },
  comments:            { type: String, default: null },

  time:                { type: Date,   required: true, index: true }, // scheduled time
  status: {
    type: String,
    enum: ['pending', 'processing', 'completed', 'failed', 'cancelled', 'paused'],
    default: 'pending',
    index: true,
  },

  attempts:            { type: Number, default: 0 },
  processingStartedAt: { type: Date,   default: null },
  executedAt:          { type: Date,   default: null },
  smmOrderId:          { type: mongoose.Schema.Types.Mixed, default: null },
  error:               { type: String, default: null },

  createdAt:           { type: Date,   default: Date.now },
});

// Compound indexes for the scheduler hot path
RunSchema.index({ status: 1, time: 1 });
RunSchema.index({ link: 1, label: 1, status: 1 });

const OrderSchema = new mongoose.Schema({
  schedulerOrderId: { type: String, required: true, unique: true, index: true },
  // Owner of this order. Indexed because every user-facing query filters on it.
  userId:           { type: String, required: true, index: true, default: '' },
  name:             { type: String, required: true },
  link:             { type: String, required: true },
  status: {
    type: String,
    enum: ['pending', 'running', 'paused', 'cancelled', 'completed', 'failed'],
    default: 'pending',
  },
  totalRuns:        { type: Number, required: true },
  completedRuns:    { type: Number, default: 0 },
  runStatuses:      [{ type: String }],
  createdAt:        { type: Date, default: Date.now },
  lastUpdatedAt:    { type: Date, default: Date.now },
});

const SettingsSchema = new mongoose.Schema({
  key:       { type: String, required: true, unique: true },
  value:     { type: mongoose.Schema.Types.Mixed, required: true },
  updatedAt: { type: Date, default: Date.now },
});

/* User accounts. Passwords are never stored in plain text — only a
   scrypt hash plus a per-user random salt. */
const UserSchema = new mongoose.Schema({
  email:        { type: String, required: true, unique: true, index: true, lowercase: true, trim: true },
  passwordHash: { type: String, required: true },
  salt:         { type: String, required: true },
  name:         { type: String, default: '' },
  isActive:     { type: Boolean, default: true },
  // Reserved so email verification can be switched on later without a migration.
  isVerified:   { type: Boolean, default: false },
  createdAt:    { type: Date, default: Date.now },
  lastLoginAt:  { type: Date, default: null },
});

/* Opaque session tokens. Only the SHA-256 of the token is stored, so a
   database leak does not hand out working sessions. */
const SessionSchema = new mongoose.Schema({
  tokenHash: { type: String, required: true, unique: true, index: true },
  userId:    { type: String, required: true, index: true },
  createdAt: { type: Date, default: Date.now },
  expiresAt: { type: Date, required: true, index: true },
});

/* Admin-managed panel configuration.
   Exactly ONE document (singleton, key: 'default') holds the SMM panel
   credentials and the service id for each engagement label. Regular users
   never send credentials — the server reads them from here. */
const PanelConfigSchema = new mongoose.Schema({
  key:        { type: String, required: true, unique: true, default: 'default' },
  panelName:  { type: String, default: '' },
  apiUrl:     { type: String, default: '' },
  apiKey:     { type: String, default: '' },
  serviceIds: {
    views:    { type: String, default: '' },
    likes:    { type: String, default: '' },
    shares:   { type: String, default: '' },
    saves:    { type: String, default: '' },
    comments: { type: String, default: '' },
    reposts:  { type: String, default: '' },
  },
  updatedAt:  { type: Date, default: Date.now },
});

const PanelConfig = mongoose.model('PanelConfig', PanelConfigSchema);

const User     = mongoose.model('User', UserSchema);
const Session  = mongoose.model('Session', SessionSchema);

const Run      = mongoose.model('Run', RunSchema);
const Order    = mongoose.model('Order', OrderSchema);
const Settings = mongoose.model('Settings', SettingsSchema);

/* ============================================================
   SETTINGS (loaded from DB at boot)
   ============================================================ */
let MIN_VIEWS_PER_RUN = 100;

/* ============================================================
   USER AUTH (email + password)
   Uses Node's built-in crypto — no extra dependencies.
   - Passwords: scrypt with a 16-byte random salt per user.
   - Sessions:  256-bit random token; only its SHA-256 is stored.
   ============================================================ */
const crypto = require('crypto');

const SESSION_TTL_DAYS = 30;
const SESSION_TTL_MS = SESSION_TTL_DAYS * 24 * 60 * 60 * 1000;

function hashPassword(password, salt) {
  // 64-byte derived key; scrypt is deliberately slow to resist brute force.
  return crypto.scryptSync(String(password), salt, 64).toString('hex');
}

function makeSalt() {
  return crypto.randomBytes(16).toString('hex');
}

function verifyPassword(password, salt, expectedHash) {
  const actual = Buffer.from(hashPassword(password, salt), 'hex');
  const expected = Buffer.from(String(expectedHash), 'hex');
  if (actual.length !== expected.length) return false;
  return crypto.timingSafeEqual(actual, expected);
}

function makeSessionToken() {
  return crypto.randomBytes(32).toString('hex');
}

function hashToken(token) {
  return crypto.createHash('sha256').update(String(token)).digest('hex');
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function normalizeEmail(value) {
  return String(value || '').trim().toLowerCase();
}

function publicUser(user) {
  return {
    id: String(user._id),
    email: user.email,
    name: user.name || '',
    createdAt: user.createdAt,
  };
}

async function createSession(userId) {
  const token = makeSessionToken();
  await Session.create({
    tokenHash: hashToken(token),
    userId: String(userId),
    expiresAt: new Date(Date.now() + SESSION_TTL_MS),
  });
  return token;
}

/** Pull the bearer token off the request, if present. */
function bearerToken(req) {
  const header = String(req.headers.authorization || '');
  return header.startsWith('Bearer ') ? header.slice(7).trim() : '';
}

/** Resolve the session -> user, or null. Also prunes expired sessions. */
async function resolveUser(req) {
  const token = bearerToken(req);
  if (!token) return null;

  const session = await Session.findOne({ tokenHash: hashToken(token) }).lean();
  if (!session) return null;

  if (new Date(session.expiresAt).getTime() < Date.now()) {
    await Session.deleteOne({ tokenHash: session.tokenHash }).catch(() => {});
    return null;
  }

  const user = await User.findById(session.userId);
  if (!user || !user.isActive) return null;
  return user;
}

/** Route guard: 401 unless a valid session is supplied. */
async function requireUser(req, res, next) {
  try {
    const user = await resolveUser(req);
    if (!user) return res.status(401).json({ error: 'Not signed in' });
    req.user = user;
    next();
  } catch (e) {
    err('requireUser:', e?.message || e);
    res.status(500).json({ error: 'Auth check failed' });
  }
}

/* ============================================================
   ADMIN AUTH
   Set ADMIN_PASSWORD in Render. Admin requests must send it in the
   `x-admin-password` header. Checked SERVER-SIDE, so the secret is
   never shipped in the frontend bundle.
   ============================================================ */
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || '';
if (!ADMIN_PASSWORD) {
  warn('ADMIN_PASSWORD is not set — admin routes are DISABLED until you set it in Render.');
}

function requireAdmin(req, res, next) {
  if (!ADMIN_PASSWORD) {
    return res.status(503).json({ error: 'Admin disabled: ADMIN_PASSWORD is not configured on the server.' });
  }
  const supplied = String(req.headers['x-admin-password'] || '');
  // Length-independent comparison; avoids leaking length via early exit.
  const a = Buffer.from(supplied);
  const b = Buffer.from(ADMIN_PASSWORD);
  const ok = a.length === b.length && crypto.timingSafeEqual(a, b);
  if (!ok) return res.status(401).json({ error: 'Invalid admin password' });
  next();
}

const SERVICE_LABELS = ['views', 'likes', 'shares', 'saves', 'comments', 'reposts'];

/** Read the singleton panel config, creating an empty one on first call. */
async function getPanelConfig() {
  let doc = await PanelConfig.findOne({ key: 'default' });
  if (!doc) doc = await PanelConfig.create({ key: 'default' });
  return doc;
}

/** Strip the API key before sending config to any client. */
function publicPanelConfig(doc) {
  const serviceIds = {};
  for (const label of SERVICE_LABELS) {
    serviceIds[label] = String(doc?.serviceIds?.[label] || '');
  }
  return {
    panelName:  doc?.panelName || '',
    apiUrl:     doc?.apiUrl || '',
    hasApiKey:  Boolean(doc?.apiKey),
    apiKeyMask: doc?.apiKey ? `••••••••${String(doc.apiKey).slice(-4)}` : '',
    serviceIds,
    configured: Boolean(doc?.apiUrl && doc?.apiKey && doc?.serviceIds?.views),
    updatedAt:  doc?.updatedAt || null,
  };
}

async function loadSettings() {
  try {
    const setting = await Settings.findOne({ key: 'minViewsPerRun' }).lean();
    if (setting && typeof setting.value === 'number' && setting.value >= 1) {
      MIN_VIEWS_PER_RUN = setting.value;
      log(`✅ Loaded MIN_VIEWS_PER_RUN from DB: ${MIN_VIEWS_PER_RUN}`);
    } else {
      await Settings.findOneAndUpdate(
        { key: 'minViewsPerRun' },
        { key: 'minViewsPerRun', value: MIN_VIEWS_PER_RUN, updatedAt: new Date() },
        { upsert: true }
      );
      log(`✅ Saved default MIN_VIEWS_PER_RUN to DB: ${MIN_VIEWS_PER_RUN}`);
    }
  } catch (e) {
    warn('Could not load settings from DB:', e.message);
  }
}

async function saveMinViewsSetting(value) {
  await Settings.findOneAndUpdate(
    { key: 'minViewsPerRun' },
    { key: 'minViewsPerRun', value, updatedAt: new Date() },
    { upsert: true }
  );
}

/* ============================================================
   IN-FLIGHT TRACKING (rebuilt from DB at boot)
   Tracks which (link, label) pairs are currently executing,
   so we never fire two runs for the same combo at once.
   This is purely a runtime optimization — DB still wins.
   ============================================================ */
const inFlight = new Set(); // values: "link|||LABEL"
const inFlightKey = (link, label) => `${link}|||${label}`;

/* ============================================================
   ID GENERATOR
   ============================================================ */
function makeRunId() {
  return `r-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}
function makeOrderId() {
  return `sched-${Date.now()}-${Math.random().toString(36).slice(2, 11)}`;
}

/* ============================================================
   PROVIDER CALL
   ============================================================ */
async function callProvider({ apiUrl, apiKey, service, link, quantity, comments }) {
  const params = new URLSearchParams({
    key: apiKey,
    action: 'add',
    service: String(service),
    link: String(link),
    quantity: String(quantity),
  });
  if (comments) params.append('comments', comments);

  const response = await axios.post(apiUrl, params.toString(), {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    timeout: PROVIDER_HTTP_TIMEOUT_MS,
    validateStatus: () => true, // we handle status ourselves
  });
  return { status: response.status, data: response.data };
}

function isBusyError(msg) {
  if (!msg || typeof msg !== 'string') return false;
  const m = msg.toLowerCase();
  return (
    m.includes('active order')   ||
    m.includes('wait until')     ||
    m.includes('wait for')       ||
    m.includes('try again later')||
    m.includes('rate limit')     ||
    m.includes('too many')
  );
}

/* ============================================================
   ADD RUNS (called by POST /api/order)
   Minimum-quantity checks happen ONLY here, never at execute.
   ============================================================ */
async function addRuns(services, baseConfig, schedulerOrderId) {
  const docs = [];
  const nowMs = Date.now();

  for (const [key, serviceConfig] of Object.entries(services)) {
    if (!serviceConfig) continue;
    const label = key.toUpperCase();

    for (const run of (serviceConfig.runs || [])) {
      let quantity;
      let commentsText = null;

      if (label === 'VIEWS') {
        if (!run.quantity || run.quantity < MIN_VIEWS_PER_RUN) {
          log(`[ADD] SKIP VIEWS qty=${run.quantity} < MIN=${MIN_VIEWS_PER_RUN}`);
          continue;
        }
        quantity = run.quantity;
      } else if (label === 'REPOSTS') {
        if (!run.quantity || run.quantity < 10) {
          log(`[ADD] SKIP REPOSTS qty=${run.quantity} < 10`);
          continue;
        }
        quantity = run.quantity;
      } else if (label === 'COMMENTS') {
        if (!run.comments) continue;
        let lines = String(run.comments).split('\n').map(c => c.trim()).filter(Boolean);
        if (lines.length < 1) continue;
        if (lines.length > 10) lines = lines.sort(() => Math.random() - 0.5).slice(0, 10);
        commentsText = lines.join('\n');
        quantity = lines.length;
      } else {
        if (!run.quantity || run.quantity <= 0) continue;
        quantity = run.quantity;
      }

      let scheduledTime;
      try {
        scheduledTime = new Date(run.time);
        if (isNaN(scheduledTime.getTime())) {
          warn(`[ADD] Invalid time, skipping: ${run.time}`);
          continue;
        }
        if (scheduledTime.getTime() < nowMs - 5 * 60 * 1000) {
          log(`[ADD] Skipping run scheduled >5 min in the past: ${scheduledTime.toISOString()}`);
          continue;
        }
        // Nudge near-past runs forward a few seconds so they fire immediately
        if (scheduledTime.getTime() < nowMs) {
          scheduledTime = new Date(nowMs + 2_000);
        }
      } catch (e) {
        warn(`[ADD] Error parsing time: ${run.time}`, e.message);
        continue;
      }

      docs.push({
        id: makeRunId(),
        schedulerOrderId,
        label,
        apiUrl: baseConfig.apiUrl,
        apiKey: baseConfig.apiKey,
        service: serviceConfig.serviceId,
        link: baseConfig.link,
        quantity,
        time: scheduledTime,
        status: 'pending',
        comments: commentsText,
        attempts: 0,
        smmOrderId: null,
        error: null,
        createdAt: new Date(),
      });
    }
  }

  if (docs.length === 0) return [];
  const inserted = await Run.insertMany(docs, { ordered: false });
  return inserted;
}

/* ============================================================
   ORDER STATUS RECOMPUTE
   ============================================================ */
async function recomputeOrderStatus(schedulerOrderId) {
  if (!schedulerOrderId) return;

  const order = await Order.findOne({ schedulerOrderId });
  if (!order) return;
  if (order.status === 'cancelled') return; // never auto-revive a cancelled order

  const runs = await Run.find(
    { schedulerOrderId },
    { status: 1 }
  ).lean();

  const total      = runs.length;
  const completed  = runs.filter(r => r.status === 'completed').length;
  const cancelled  = runs.filter(r => r.status === 'cancelled').length;
  const failed     = runs.filter(r => r.status === 'failed').length;
  const processing = runs.filter(r => r.status === 'processing').length;
  const paused     = runs.filter(r => r.status === 'paused').length;
  const pending    = runs.filter(r => r.status === 'pending').length;

  const active = total - cancelled;

  let newStatus;
  if (active === 0)                                   newStatus = 'cancelled';
  else if (completed + failed === active && failed > 0 && completed === 0)
                                                       newStatus = 'failed';
  else if (completed + failed === active)             newStatus = 'completed';
  else if (paused > 0 && processing === 0 && pending === 0)
                                                       newStatus = 'paused';
  else if (processing > 0 || completed > 0)           newStatus = 'running';
  else if (pending > 0)                                newStatus = 'pending';
  else                                                 newStatus = order.status;

  // Refetch full statuses for runStatuses array (frontend uses it)
  const fullRuns = await Run.find({ schedulerOrderId }, { status: 1 }).lean();

  await Order.updateOne(
    { schedulerOrderId },
    {
      $set: {
        status: newStatus,
        totalRuns: total,
        completedRuns: completed,
        runStatuses: fullRuns.map(r => r.status),
        lastUpdatedAt: new Date(),
      },
    }
  );
}

/* ============================================================
   EXECUTE A RUN
   Called only after the run has been atomically claimed
   (status moved from pending → processing).
   ============================================================ */
async function executeRun(run) {
  const key = inFlightKey(run.link, run.label);
  log(`[EXEC ${run.label}] start id=${run.id} qty=${run.quantity} link=${run.link.slice(0, 60)}`);

  try {
    const { status, data } = await callProvider({
      apiUrl: run.apiUrl,
      apiKey: run.apiKey,
      service: run.service,
      link: run.link,
      quantity: run.quantity,
      comments: run.comments,
    });

    if (data && data.order) {
      // ✅ success
      await Run.updateOne(
        { _id: run._id, status: 'processing' },
        {
          $set: {
            status: 'completed',
            smmOrderId: data.order,
            executedAt: new Date(),
            error: null,
          },
        }
      );
      log(`[EXEC ${run.label}] ✅ SUCCESS id=${run.id} smmOrder=${data.order}`);
      return;
    }

    // Provider rejected
    const errorMsg = (data && (data.error || data.message)) || `HTTP ${status} no order in response`;

    if (isBusyError(errorMsg) && run.attempts + 1 < MAX_RETRY_ATTEMPTS) {
      const nextAttempt = run.attempts + 1;
      const retryAt     = new Date(Date.now() + RETRY_BACKOFF_MS);
      await Run.updateOne(
        { _id: run._id, status: 'processing' },
        {
          $set: {
            status: 'pending',
            time: retryAt,
            attempts: nextAttempt,
            error: errorMsg,
            processingStartedAt: null,
          },
        }
      );
      warn(`[EXEC ${run.label}] busy id=${run.id} → retry #${nextAttempt} at ${retryAt.toISOString()}`);
      return;
    }

    // permanent failure
    await Run.updateOne(
      { _id: run._id, status: 'processing' },
      { $set: { status: 'failed', error: errorMsg, executedAt: new Date() } }
    );
    err(`[EXEC ${run.label}] FAILED id=${run.id} :: ${errorMsg}`);
  } catch (e) {
    const errorMsg = e?.response?.data?.error || e?.message || 'Unknown network error';

    if (isBusyError(errorMsg) && run.attempts + 1 < MAX_RETRY_ATTEMPTS) {
      const nextAttempt = run.attempts + 1;
      const retryAt     = new Date(Date.now() + RETRY_BACKOFF_MS);
      await Run.updateOne(
        { _id: run._id, status: 'processing' },
        {
          $set: {
            status: 'pending',
            time: retryAt,
            attempts: nextAttempt,
            error: errorMsg,
            processingStartedAt: null,
          },
        }
      );
      warn(`[EXEC ${run.label}] busy(catch) id=${run.id} → retry #${nextAttempt}`);
      return;
    }

    // Network blow-up or non-retryable error
    await Run.updateOne(
      { _id: run._id, status: 'processing' },
      { $set: { status: 'failed', error: errorMsg, executedAt: new Date() } }
    );
    err(`[EXEC ${run.label}] EXCEPTION id=${run.id} :: ${errorMsg}`);
  } finally {
    inFlight.delete(key);
    // Best-effort order rollup
    try { await recomputeOrderStatus(run.schedulerOrderId); }
    catch (e) { warn('recomputeOrderStatus failed:', e.message); }
  }
}

/* ============================================================
   STUCK-RUN RECOVERY (uses processingStartedAt, NOT createdAt)
   ============================================================ */
async function recoverStuckRuns() {
  const cutoff = new Date(Date.now() - STUCK_PROCESSING_MS);
  const result = await Run.updateMany(
    { status: 'processing', processingStartedAt: { $lt: cutoff } },
    {
      $set: {
        status: 'pending',
        processingStartedAt: null,
        error: 'Recovered from stuck processing state',
      },
    }
  );
  if (result.modifiedCount > 0) {
    log(`♻️  Recovered ${result.modifiedCount} stuck run(s) → pending`);
    // Drop their in-flight markers, since we don't know the actual link/label tuple here
    // (safe to clear all because they'll repopulate on the next claim)
  }
}

/* ============================================================
   SCHEDULER TICK
   1) Recover stuck runs
   2) Find candidate pending runs (oldest-time first)
   3) For each (link, label) not already in-flight, atomically claim
   4) Fire executeRun asynchronously
   ============================================================ */
let tickRunning = false;
async function schedulerTick() {
  if (tickRunning) return;
  tickRunning = true;
  try {
    await recoverStuckRuns();

    const now = new Date();
    const candidates = await Run.find(
      { status: 'pending', time: { $lte: now } },
      null,
      { sort: { time: 1 }, limit: 200 }
    ).lean();

    if (candidates.length === 0) return;

    let claimedThisTick = 0;

    for (const c of candidates) {
      if (claimedThisTick >= MAX_CLAIMS_PER_TICK) break;
      const key = inFlightKey(c.link, c.label);
      if (inFlight.has(key)) continue; // same (link, label) is already executing

      // Also check the DB to be sure (covers crash-restart scenario)
      const alreadyProcessing = await Run.exists({
        link: c.link, label: c.label, status: 'processing',
      });
      if (alreadyProcessing) { inFlight.add(key); continue; }

      // Verify the order isn't cancelled / paused before claiming
      const order = await Order.findOne(
        { schedulerOrderId: c.schedulerOrderId },
        { status: 1 }
      ).lean();
      if (!order) {
        await Run.updateOne(
          { _id: c._id, status: 'pending' },
          { $set: { status: 'cancelled', error: 'Parent order not found' } }
        );
        continue;
      }
      if (order.status === 'cancelled') {
        await Run.updateOne(
          { _id: c._id, status: 'pending' },
          { $set: { status: 'cancelled', error: 'Order was cancelled' } }
        );
        continue;
      }
      if (order.status === 'paused') {
        // leave it as pending; will be eligible after resume
        continue;
      }

      // Atomic claim: only one worker can flip this run pending → processing
      const claimed = await Run.findOneAndUpdate(
        { _id: c._id, status: 'pending' },
        { $set: { status: 'processing', processingStartedAt: new Date() } },
        { new: true }
      );
      if (!claimed) continue; // someone else got it

      inFlight.add(key);
      claimedThisTick++;
      // fire and forget — different (link, label) tuples run in parallel
      executeRun(claimed).catch((e) => {
        err('executeRun unhandled:', e?.message || e);
        inFlight.delete(key);
      });
    }

    if (claimedThisTick > 0) {
      log(`[TICK] claimed ${claimedThisTick} run(s); inFlight=${inFlight.size}`);
    }
  } catch (e) {
    err('schedulerTick error:', e?.message || e);
  } finally {
    tickRunning = false;
  }
}

/* ============================================================
   STARTUP
   ============================================================ */
async function start() {
  await mongoose.connect(MONGODB_URI, { serverSelectionTimeoutMS: 30_000 });
  log('✅ MongoDB connected');

  await loadSettings();

  // Drop expired sessions so the collection doesn't grow without bound.
  const purged = await Session.deleteMany({ expiresAt: { $lt: new Date() } });
  if (purged.deletedCount > 0) log(`🧹 Purged ${purged.deletedCount} expired session(s)`);

  // On boot, clear any leftover processing markers; they'll be re-claimed.
  // We don't know if those runs actually completed at the provider, but the
  // safer choice is to reset them — providers usually deduplicate identical
  // (link, service, qty) calls within seconds.
  const reset = await Run.updateMany(
    { status: 'processing' },
    { $set: { status: 'pending', processingStartedAt: null } }
  );
  if (reset.modifiedCount > 0) log(`♻️  Reset ${reset.modifiedCount} in-flight run(s) on boot`);

  // Repopulate in-flight set from currently-processing runs (should be 0 after the reset above,
  // but kept for safety if multiple instances ever run)
  const procs = await Run.find({ status: 'processing' }, { link: 1, label: 1 }).lean();
  procs.forEach(p => inFlight.add(inFlightKey(p.link, p.label)));
  log(`Initial in-flight tuples: ${inFlight.size}`);

  // Start scheduler
  setInterval(schedulerTick, TICK_INTERVAL_MS);
  log(`🚀 Scheduler running every ${TICK_INTERVAL_MS / 1000}s`);

  app.listen(PORT, '0.0.0.0', () => {
    log(`========================================`);
    log(`Server listening on port ${PORT}`);
    log(`MIN_VIEWS_PER_RUN = ${MIN_VIEWS_PER_RUN}`);
    log(`========================================`);
  });
}

start().catch((e) => {
  err('Startup failed:', e);
  process.exit(1);
});

process.on('SIGTERM', async () => {
  log('SIGTERM received; closing Mongo connection.');
  await mongoose.connection.close().catch(() => {});
  process.exit(0);
});

/* ============================================================
   ROUTES
   ============================================================ */

// ---- Create order ----
app.post('/api/order', requireUser, async (req, res) => {
  try {
    const { link, services, name } = req.body || {};
    if (!link || !services) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    /* Credentials come from the admin-managed config, NEVER from the client.
       Service ids are likewise resolved server-side, so a user cannot order
       an arbitrary service by tampering with the request. */
    const cfg = await getPanelConfig();
    if (!cfg.apiUrl || !cfg.apiKey) {
      return res.status(503).json({ error: 'No SMM panel configured yet. Ask the admin to set one up.' });
    }

    const resolved = {};
    for (const [label, value] of Object.entries(services)) {
      if (!value) continue;
      const normalized = String(label).toLowerCase();
      if (!SERVICE_LABELS.includes(normalized)) continue;

      const serviceId = String(cfg.serviceIds?.[normalized] || '').trim();
      if (!serviceId) {
        log(`[ORDER] Skipping "${normalized}" — no service id configured by admin`);
        continue;
      }
      resolved[normalized] = { serviceId, runs: value.runs || [] };
    }

    if (Object.keys(resolved).length === 0) {
      return res.status(400).json({ error: 'None of the requested services are configured by the admin.' });
    }

    const schedulerOrderId = makeOrderId();
    const runs = await addRuns(
      resolved,
      { apiUrl: cfg.apiUrl, apiKey: cfg.apiKey, link },
      schedulerOrderId
    );

    const orderDoc = await Order.create({
      schedulerOrderId,
      userId: String(req.user._id),
      name: name || `Order ${schedulerOrderId}`,
      link,
      status: runs.length === 0 ? 'cancelled' : 'pending',
      totalRuns: runs.length,
      completedRuns: 0,
      runStatuses: runs.map(() => 'pending'),
      createdAt: new Date(),
      lastUpdatedAt: new Date(),
    });

    log(`📦 Order created ${schedulerOrderId} with ${runs.length} run(s)`);
    return res.json({
      success: true,
      message: 'Order scheduled',
      schedulerOrderId,
      status: orderDoc.status,
      completedRuns: 0,
      totalRuns: runs.length,
    });
  } catch (e) {
    err('POST /api/order:', e?.message || e);
    return res.status(500).json({ error: e?.message || 'Internal error' });
  }
});

// ---- Detect panel currency and obtain a current INR conversion rate ----
// Most standard SMM APIs expose account currency through action=balance.
const ISO_CURRENCY_RE = /^[A-Z]{3}$/;
const exchangeRateCache = new Map();

function normalizeCurrency(value) {
  const raw = String(value || '').trim();
  const code = raw.toUpperCase();
  const aliases = {
    '$': 'USD', 'US$': 'USD', USDOLLAR: 'USD', DOLLAR: 'USD', DOLLARS: 'USD',
    '₹': 'INR', RS: 'INR', 'RS.': 'INR', RUPEE: 'INR', RUPEES: 'INR',
    '€': 'EUR', EURO: 'EUR', EUROS: 'EUR',
    '£': 'GBP', POUND: 'GBP', POUNDS: 'GBP',
    '₽': 'RUB', RUBLE: 'RUB', RUBLES: 'RUB',
    '₺': 'TRY', LIRA: 'TRY',
    '৳': 'BDT', TAKA: 'BDT',
  };
  return aliases[code] || (ISO_CURRENCY_RE.test(code) ? code : null);
}

function extractPanelCurrency(body) {
  const candidates = [
    body?.currency, body?.currency_code, body?.currencyCode,
    body?.data?.currency, body?.data?.currency_code,
    body?.account?.currency, body?.user?.currency,
  ];
  for (const candidate of candidates) {
    const normalized = normalizeCurrency(candidate);
    if (normalized) return normalized;
  }
  return null;
}

function validateProviderUrl(value) {
  try {
    const parsed = new URL(String(value || '').trim());
    return parsed.protocol === 'https:' || parsed.protocol === 'http:' ? parsed.toString() : null;
  } catch { return null; }
}

async function getInrExchangeRate(currency) {
  if (currency === 'INR') return { rate: 1, updatedAt: new Date().toISOString() };
  const cached = exchangeRateCache.get(currency);
  if (cached && Date.now() - cached.fetchedAt < 6 * 60 * 60 * 1000) return cached;

  try {
    const response = await axios.get(`https://open.er-api.com/v6/latest/${encodeURIComponent(currency)}`, {
      timeout: 15_000,
      validateStatus: () => true,
    });
    const rate = Number(response.data?.rates?.INR);
    if (response.status < 200 || response.status >= 300 || !Number.isFinite(rate) || rate <= 0) {
      throw new Error(`INR exchange rate is unavailable for ${currency}`);
    }
    const parsedUpdateTime = Date.parse(response.data?.time_last_update_utc || '');
    const result = {
      rate,
      updatedAt: Number.isFinite(parsedUpdateTime)
        ? new Date(parsedUpdateTime).toISOString()
        : new Date().toISOString(),
      fetchedAt: Date.now(),
    };
    exchangeRateCache.set(currency, result);
    return result;
  } catch (e) {
    // A temporarily unavailable FX provider should not break pricing if this
    // process has a previously verified rate. The original timestamp is kept.
    if (cached?.rate > 0) {
      warn(`Using stale cached ${currency}/INR rate:`, e?.message || e);
      return cached;
    }
    throw e;
  }
}

// ---- Single order status ----
app.get('/api/order/status/:schedulerOrderId', requireUser, async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;
    const order = await Order.findOne({ schedulerOrderId, userId: String(req.user._id) }).lean();
    if (!order) return res.status(404).json({ error: 'Order not found' });

    const runs = await Run.find({ schedulerOrderId }).lean();
    return res.json({
      schedulerOrderId: order.schedulerOrderId,
      name: order.name,
      link: order.link,
      status: order.status,
      totalRuns: order.totalRuns,
      completedRuns: order.completedRuns,
      runStatuses: order.runStatuses,
      createdAt: order.createdAt,
      lastUpdatedAt: order.lastUpdatedAt,
      runs: runs.map(r => ({
        id: r.id,
        label: r.label,
        quantity: r.quantity,
        time: r.time,
        status: r.status,
        smmOrderId: r.smmOrderId,
        executedAt: r.executedAt,
        error: r.error,
        attempts: r.attempts,
      })),
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'Internal error' });
  }
});

// ---- All orders status ----
app.get('/api/orders/status', requireUser, async (req, res) => {
  try {
    const orders = await Order.find({ userId: String(req.user._id) }).sort({ createdAt: -1 }).lean();
    const result = await Promise.all(orders.map(async (o) => {
      const runs = await Run.find(
        { schedulerOrderId: o.schedulerOrderId },
        { id: 1, label: 1, quantity: 1, time: 1, status: 1, smmOrderId: 1 }
      ).lean();
      return { ...o, runs };
    }));
    return res.json({ total: orders.length, orders: result });
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'Internal error' });
  }
});

// ---- Pause / resume / cancel ----
app.post('/api/order/control', requireUser, async (req, res) => {
  try {
    const { schedulerOrderId, action } = req.body || {};
    if (!schedulerOrderId || !action) {
      return res.status(400).json({ error: 'Missing schedulerOrderId or action' });
    }

    // Scoped by userId so one account cannot pause/cancel another's order.
    const order = await Order.findOne({ schedulerOrderId, userId: String(req.user._id) });
    if (!order) return res.status(404).json({ error: 'Order not found' });

    if (action === 'cancel') {
      await Run.updateMany(
        { schedulerOrderId, status: { $in: ['pending', 'processing', 'paused'] } },
        { $set: { status: 'cancelled', error: 'Cancelled by user' } }
      );
      order.status = 'cancelled';
      await order.save();
      await recomputeOrderStatus(schedulerOrderId);
    } else if (action === 'pause') {
      // Only pause pending runs; let processing ones finish naturally
      await Run.updateMany(
        { schedulerOrderId, status: 'pending' },
        { $set: { status: 'paused' } }
      );
      order.status = 'paused';
      await order.save();
      await recomputeOrderStatus(schedulerOrderId);
    } else if (action === 'resume') {
      await Run.updateMany(
        { schedulerOrderId, status: 'paused' },
        { $set: { status: 'pending' } }
      );
      order.status = 'running';
      await order.save();
      await recomputeOrderStatus(schedulerOrderId);
    } else {
      return res.status(400).json({ error: 'Invalid action' });
    }

    const fresh = await Order.findOne({ schedulerOrderId }).lean();
    return res.json({
      success: true,
      status: fresh.status,
      completedRuns: fresh.completedRuns,
      runStatuses: fresh.runStatuses,
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'Internal error' });
  }
});

// ---- Order runs only ----
app.get('/api/order/runs/:schedulerOrderId', requireUser, async (req, res) => {
  try {
    const owned = await Order.findOne(
      { schedulerOrderId: req.params.schedulerOrderId, userId: String(req.user._id) },
      { _id: 1 }
    ).lean();
    if (!owned) return res.status(404).json({ error: 'Order not found' });

    const runs = await Run.find({ schedulerOrderId: req.params.schedulerOrderId }).lean();
    return res.json({
      schedulerOrderId: req.params.schedulerOrderId,
      runs: runs.map(r => ({
        id: r.id,
        label: r.label,
        quantity: r.quantity,
        time: r.time,
        status: r.status,
        smmOrderId: r.smmOrderId,
        executedAt: r.executedAt,
        error: r.error,
        attempts: r.attempts,
      })),
    });
  } catch (e) {
    return res.status(500).json({ error: e?.message || 'Internal error' });
  }
});

// ---- Min-views setting ----
app.get('/api/settings/min-views', (_req, res) => {
  res.json({ minViewsPerRun: MIN_VIEWS_PER_RUN });
});

app.post('/api/settings/min-views', async (req, res) => {
  const { minViewsPerRun } = req.body || {};
  if (typeof minViewsPerRun !== 'number' || minViewsPerRun < 1) {
    return res.status(400).json({ error: 'Invalid minViewsPerRun value' });
  }
  MIN_VIEWS_PER_RUN = Math.floor(minViewsPerRun);
  await saveMinViewsSetting(MIN_VIEWS_PER_RUN);
  log(`MIN_VIEWS_PER_RUN updated → ${MIN_VIEWS_PER_RUN}`);
  res.json({ success: true, minViewsPerRun: MIN_VIEWS_PER_RUN });
});

// ---- Queue / system status ----
app.get('/api/queues/status', async (_req, res) => {
  try {
    const pending    = await Run.countDocuments({ status: 'pending' });
    const processing = await Run.countDocuments({ status: 'processing' });
    res.json({
      pending,
      processing,
      inFlightTuples: inFlight.size,
      minViewsPerRun: MIN_VIEWS_PER_RUN,
      // Backward-compatible structure for old frontend
      views:    { queueLength: 0, isExecuting: false },
      likes:    { queueLength: 0, isExecuting: false },
      shares:   { queueLength: 0, isExecuting: false },
      saves:    { queueLength: 0, isExecuting: false },
      reposts:  { queueLength: 0, isExecuting: false },
      comments: { queueLength: 0, isExecuting: false },
    });
  } catch (e) {
    res.status(500).json({ error: e?.message || 'Internal error' });
  }
});

// ---- Force-retry stuck runs ----
app.post('/api/runs/retry-stuck', async (_req, res) => {
  try {
    const r1 = await Run.updateMany(
      { status: 'processing', processingStartedAt: { $lt: new Date(Date.now() - 60_000) } },
      { $set: { status: 'pending', processingStartedAt: null } }
    );
    // Wipe in-flight set so we re-check from DB
    inFlight.clear();
    const procs = await Run.find({ status: 'processing' }, { link: 1, label: 1 }).lean();
    procs.forEach(p => inFlight.add(inFlightKey(p.link, p.label)));
    res.json({ success: true, resetCount: r1.modifiedCount });
  } catch (e) {
    res.status(500).json({ error: e?.message || 'Internal error' });
  }
});

// ---- Manual scheduler nudge ----
app.post('/api/scheduler/trigger', async (_req, res) => {
  await schedulerTick();
  res.json({ success: true });
});

/* ============================================================
   AUTH ROUTES
   ============================================================ */

// ---- Sign up ----
app.post('/api/auth/signup', async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    const password = String(req.body?.password || '');
    const name = String(req.body?.name || '').trim();

    if (!EMAIL_RE.test(email)) {
      return res.status(400).json({ error: 'Enter a valid email address' });
    }
    if (password.length < 8) {
      return res.status(400).json({ error: 'Password must be at least 8 characters' });
    }

    const existing = await User.findOne({ email }).lean();
    if (existing) {
      return res.status(409).json({ error: 'An account with this email already exists' });
    }

    const salt = makeSalt();
    const user = await User.create({
      email,
      salt,
      passwordHash: hashPassword(password, salt),
      name,
      lastLoginAt: new Date(),
    });

    const token = await createSession(user._id);
    log(`👤 New account: ${email}`);
    return res.status(201).json({ success: true, token, user: publicUser(user) });
  } catch (e) {
    // Unique index can still fire under a race; report it cleanly.
    if (e?.code === 11000) {
      return res.status(409).json({ error: 'An account with this email already exists' });
    }
    err('POST /api/auth/signup:', e?.message || e);
    return res.status(500).json({ error: 'Could not create account' });
  }
});

// ---- Log in ----
app.post('/api/auth/login', async (req, res) => {
  try {
    const email = normalizeEmail(req.body?.email);
    const password = String(req.body?.password || '');
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' });
    }

    const user = await User.findOne({ email });
    // Same message whether the email is unknown or the password is wrong,
    // so the endpoint can't be used to enumerate registered accounts.
    const INVALID = 'Incorrect email or password';
    if (!user) return res.status(401).json({ error: INVALID });
    if (!verifyPassword(password, user.salt, user.passwordHash)) {
      return res.status(401).json({ error: INVALID });
    }
    if (!user.isActive) {
      return res.status(403).json({ error: 'This account has been disabled' });
    }

    user.lastLoginAt = new Date();
    await user.save();

    const token = await createSession(user._id);
    return res.json({ success: true, token, user: publicUser(user) });
  } catch (e) {
    err('POST /api/auth/login:', e?.message || e);
    return res.status(500).json({ error: 'Could not sign in' });
  }
});

// ---- Who am I (used to restore a session on page load) ----
app.get('/api/auth/me', requireUser, (req, res) => {
  res.json({ user: publicUser(req.user) });
});

// ---- Log out (invalidate this session only) ----
app.post('/api/auth/logout', async (req, res) => {
  try {
    const token = bearerToken(req);
    if (token) await Session.deleteOne({ tokenHash: hashToken(token) });
    res.json({ success: true });
  } catch (e) {
    res.json({ success: true });
  }
});

// ---- Change password (invalidates all other sessions) ----
app.post('/api/auth/change-password', requireUser, async (req, res) => {
  try {
    const currentPassword = String(req.body?.currentPassword || '');
    const newPassword = String(req.body?.newPassword || '');
    if (newPassword.length < 8) {
      return res.status(400).json({ error: 'New password must be at least 8 characters' });
    }
    if (!verifyPassword(currentPassword, req.user.salt, req.user.passwordHash)) {
      return res.status(401).json({ error: 'Current password is incorrect' });
    }

    const salt = makeSalt();
    req.user.salt = salt;
    req.user.passwordHash = hashPassword(newPassword, salt);
    await req.user.save();

    // Drop every session, then issue a fresh one for this device.
    await Session.deleteMany({ userId: String(req.user._id) });
    const token = await createSession(req.user._id);
    res.json({ success: true, token });
  } catch (e) {
    err('POST /api/auth/change-password:', e?.message || e);
    res.status(500).json({ error: 'Could not change password' });
  }
});

/* ============================================================
   PANEL CONFIG ROUTES
   ============================================================ */

// ---- Public: what the New Order page needs (NEVER returns the API key) ----
/* ---- Price quote ----
   The panel's per-service rates can only be read with the admin API key, so
   the calculation happens here and only the final numbers go to the browser.
   Body: { services: { views: 12000, likes: 300, ... } }  (total units each) */
app.post('/api/quote', requireUser, async (req, res) => {
  try {
    const cfg = await getPanelConfig();
    if (!cfg.apiUrl || !cfg.apiKey) {
      return res.status(503).json({ error: 'No SMM panel configured yet.' });
    }

    const requested = req.body?.services && typeof req.body.services === 'object'
      ? req.body.services
      : {};

    // Pull the live catalogue so rates are always current.
    const params = new URLSearchParams({ key: cfg.apiKey, action: 'services' });
    const response = await axios.post(cfg.apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: PROVIDER_HTTP_TIMEOUT_MS,
      validateStatus: () => true,
    });

    const data = response.data;
    if (data && data.error) return res.status(400).json({ error: String(data.error) });
    const catalogue = Array.isArray(data) ? data
      : Array.isArray(data?.services) ? data.services
      : Array.isArray(data?.data) ? data.data
      : [];

    const rateById = new Map();
    for (const row of catalogue) {
      const id = String(row?.service ?? row?.id ?? '').trim();
      const rate = Number(String(row?.rate ?? row?.price ?? row?.cost ?? '')
        .replace(/[^\d.]/g, ''));
      if (id && Number.isFinite(rate)) rateById.set(id, rate);
    }

    // Detect the panel's currency so we can convert to INR.
    let currency = null;
    try {
      const balanceParams = new URLSearchParams({ key: cfg.apiKey, action: 'balance' });
      const balanceRes = await axios.post(cfg.apiUrl, balanceParams.toString(), {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        timeout: PROVIDER_HTTP_TIMEOUT_MS,
        validateStatus: () => true,
      });
      currency = extractPanelCurrency(balanceRes.data);
    } catch { /* fall through */ }

    let exchangeRateToInr = 1;
    let rateUpdatedAt = null;
    if (currency && currency !== 'INR') {
      try {
        const fx = await getInrExchangeRate(currency);
        exchangeRateToInr = fx.rate;
        rateUpdatedAt = fx.updatedAt;
      } catch (e) {
        warn('Quote: FX lookup failed:', e?.message || e);
        return res.json({
          available: false,
          reason: `Could not convert ${currency} to INR right now.`,
        });
      }
    }

    const breakdown = {};
    let nativeTotal = 0;
    let missingRate = false;

    for (const label of SERVICE_LABELS) {
      const units = Math.max(0, Math.floor(Number(requested[label]) || 0));
      if (units <= 0) continue;

      const serviceId = String(cfg.serviceIds?.[label] || '').trim();
      if (!serviceId) continue;

      const rate = rateById.get(serviceId);
      if (!Number.isFinite(rate) || rate <= 0) { missingRate = true; continue; }

      // Standard SMM panels quote a price per 1,000 units.
      const native = (units / 1000) * rate;
      nativeTotal += native;
      breakdown[label] = Math.round(native * exchangeRateToInr * 100) / 100;
    }

    if (missingRate && nativeTotal === 0) {
      return res.json({ available: false, reason: 'Panel did not return usable rates.' });
    }

    return res.json({
      available: true,
      total: Math.round(nativeTotal * exchangeRateToInr * 100) / 100,
      breakdown,
      currency: currency || 'INR',
      nativeTotal: Math.round(nativeTotal * 10000) / 10000,
      exchangeRateToInr,
      exchangeRateUpdatedAt: rateUpdatedAt,
      partial: missingRate,
    });
  } catch (e) {
    err('POST /api/quote:', e?.message || e);
    return res.status(500).json({ error: e?.message || 'Could not calculate price' });
  }
});

app.get('/api/panel-config', async (_req, res) => {
  try {
    const doc = await getPanelConfig();
    res.json(publicPanelConfig(doc));
  } catch (e) {
    err('GET /api/panel-config:', e?.message || e);
    res.status(500).json({ error: 'Could not load panel configuration' });
  }
});

// ---- Admin: verify password (used by the admin login screen) ----
app.post('/api/admin/verify', requireAdmin, (_req, res) => {
  res.json({ success: true });
});

// ---- Admin: read config, including which service ids are set ----
app.get('/api/admin/panel-config', requireAdmin, async (_req, res) => {
  try {
    const doc = await getPanelConfig();
    res.json(publicPanelConfig(doc));
  } catch (e) {
    err('GET /api/admin/panel-config:', e?.message || e);
    res.status(500).json({ error: 'Could not load panel configuration' });
  }
});

// ---- Admin: save panel + service ids ----
app.post('/api/admin/panel-config', requireAdmin, async (req, res) => {
  try {
    const { panelName, apiUrl, apiKey, serviceIds } = req.body || {};

    const doc = await getPanelConfig();

    if (typeof panelName === 'string') doc.panelName = panelName.trim();

    if (typeof apiUrl === 'string' && apiUrl.trim()) {
      const valid = validateProviderUrl(apiUrl);
      if (!valid) return res.status(400).json({ error: 'API URL must be a valid http(s) URL' });
      doc.apiUrl = apiUrl.trim();
    }

    // Only overwrite the key when a new non-empty value is supplied, so the
    // admin can edit service ids without re-typing the key every time.
    if (typeof apiKey === 'string' && apiKey.trim()) {
      doc.apiKey = apiKey.trim();
    }

    if (serviceIds && typeof serviceIds === 'object') {
      for (const label of SERVICE_LABELS) {
        if (label in serviceIds) {
          doc.serviceIds[label] = String(serviceIds[label] ?? '').trim();
        }
      }
    }

    doc.updatedAt = new Date();
    await doc.save();
    log(`⚙️  Panel config updated (${doc.panelName || doc.apiUrl})`);
    res.json({ success: true, ...publicPanelConfig(doc) });
  } catch (e) {
    err('POST /api/admin/panel-config:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not save panel configuration' });
  }
});

// ---- Admin: list registered users ----
app.get('/api/admin/users', requireAdmin, async (_req, res) => {
  try {
    const users = await User.find().sort({ createdAt: -1 }).limit(500).lean();
    const counts = await Order.aggregate([
      { $group: { _id: '$userId', total: { $sum: 1 } } },
    ]);
    const byUser = new Map(counts.map(c => [String(c._id), c.total]));
    res.json({
      total: users.length,
      users: users.map(u => ({
        id: String(u._id),
        email: u.email,
        name: u.name || '',
        isActive: u.isActive !== false,
        createdAt: u.createdAt,
        lastLoginAt: u.lastLoginAt,
        orderCount: byUser.get(String(u._id)) || 0,
      })),
    });
  } catch (e) {
    err('GET /api/admin/users:', e?.message || e);
    res.status(500).json({ error: 'Could not load users' });
  }
});

// ---- Admin: enable / disable an account ----
app.post('/api/admin/users/:id/active', requireAdmin, async (req, res) => {
  try {
    const isActive = Boolean(req.body?.isActive);
    const user = await User.findById(req.params.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    user.isActive = isActive;
    await user.save();
    // Disabling an account must also kill its live sessions.
    if (!isActive) await Session.deleteMany({ userId: String(user._id) });
    res.json({ success: true, id: String(user._id), isActive });
  } catch (e) {
    err('POST /api/admin/users/:id/active:', e?.message || e);
    res.status(500).json({ error: 'Could not update user' });
  }
});

// ---- Admin: fetch the service catalogue using the STORED credentials ----
app.post('/api/admin/services', requireAdmin, async (req, res) => {
  try {
    const doc = await getPanelConfig();
    // Allow testing an unsaved panel by passing overrides.
    const apiUrl = String(req.body?.apiUrl || doc.apiUrl || '').trim();
    const apiKey = String(req.body?.apiKey || doc.apiKey || '').trim();
    if (!apiUrl || !apiKey) {
      return res.status(400).json({ error: 'Panel URL and API key are required' });
    }
    if (!validateProviderUrl(apiUrl)) {
      return res.status(400).json({ error: 'API URL must be a valid http(s) URL' });
    }

    const params = new URLSearchParams({ key: apiKey, action: 'services' });
    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: PROVIDER_HTTP_TIMEOUT_MS,
      validateStatus: () => true,
    });

    const data = response.data;
    if (data && data.error) return res.status(400).json({ error: String(data.error) });

    const list = Array.isArray(data) ? data
      : Array.isArray(data?.services) ? data.services
      : Array.isArray(data?.data) ? data.data
      : [];

    res.json({ success: true, services: list });
  } catch (e) {
    err('POST /api/admin/services:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not reach the SMM panel' });
  }
});

// ---- Root (Render health check / uptime pinger) ----
app.get('/', (_req, res) => {
  res.json({ status: 'ok', service: 'truesmm-backend', uptime: process.uptime() });
});

// ---- Health ----
app.get('/api/health', async (_req, res) => {
  res.json({
    status: 'ok',
    mongoConnected: mongoose.connection.readyState === 1,
    uptime: process.uptime(),
    inFlightTuples: inFlight.size,
    minViewsPerRun: MIN_VIEWS_PER_RUN,
  });
});
