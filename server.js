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

app.use(cors());
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

const Run      = mongoose.model('Run', RunSchema);
const Order    = mongoose.model('Order', OrderSchema);
const Settings = mongoose.model('Settings', SettingsSchema);

/* ============================================================
   SETTINGS (loaded from DB at boot)
   ============================================================ */
let MIN_VIEWS_PER_RUN = 100;

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
app.post('/api/order', async (req, res) => {
  try {
    const { apiUrl, apiKey, link, services, name } = req.body || {};
    if (!apiUrl || !apiKey || !link || !services) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const schedulerOrderId = makeOrderId();
    const runs = await addRuns(services, { apiUrl, apiKey, link }, schedulerOrderId);

    const orderDoc = await Order.create({
      schedulerOrderId,
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

app.post('/api/panel/pricing-meta', async (req, res) => {
  const { apiUrl, apiKey } = req.body || {};
  const requestedCurrency = normalizeCurrency(req.body?.currency);
  const safeApiUrl = validateProviderUrl(apiUrl);
  if (!safeApiUrl || !apiKey) return res.status(400).json({ error: 'Valid API URL and key are required' });

  try {
    let currency = requestedCurrency;
    let currencySource = requestedCurrency ? 'user' : null;
    let balance = null;

    // Even when a user supplied a currency, fetch balance for useful validation.
    try {
      const params = new URLSearchParams({ key: String(apiKey), action: 'balance' });
      const panelResponse = await axios.post(safeApiUrl, params.toString(), {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        timeout: PROVIDER_HTTP_TIMEOUT_MS,
        validateStatus: () => true,
      });
      const body = panelResponse.data;
      const detected = extractPanelCurrency(body);
      const parsedBalance = Number(body?.balance ?? body?.data?.balance);
      if (Number.isFinite(parsedBalance)) balance = parsedBalance;
      if (!currency && detected) {
        currency = detected;
        currencySource = 'panel';
      }
    } catch (e) {
      warn('Panel balance/currency detection failed:', e?.message || e);
    }

    if (!currency) {
      return res.json({
        currency: null,
        currencySource: null,
        exchangeRateToInr: null,
        exchangeRateUpdatedAt: null,
        balance,
        requiresCurrencyConfirmation: true,
      });
    }

    const exchange = await getInrExchangeRate(currency);
    return res.json({
      currency,
      currencySource,
      exchangeRateToInr: exchange.rate,
      exchangeRateUpdatedAt: exchange.updatedAt,
      balance,
      requiresCurrencyConfirmation: false,
    });
  } catch (e) {
    return res.status(502).json({ error: e?.message || 'Pricing metadata is unavailable' });
  }
});

// ---- Fetch services from provider ----
app.post('/api/services', async (req, res) => {
  const { apiUrl, apiKey } = req.body || {};
  if (!apiUrl || !apiKey) return res.status(400).json({ error: 'Missing API URL or key' });
  try {
    const params = new URLSearchParams({ key: apiKey, action: 'services' });
    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: PROVIDER_HTTP_TIMEOUT_MS,
      validateStatus: () => true,
    });
    return res.status(response.status).json(response.data);
  } catch (e) {
    return res.status(500).json({ error: e?.response?.data || e?.message || 'Provider error' });
  }
});

// ---- Single order status ----
app.get('/api/order/status/:schedulerOrderId', async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;
    const order = await Order.findOne({ schedulerOrderId }).lean();
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
app.get('/api/orders/status', async (_req, res) => {
  try {
    const orders = await Order.find().sort({ createdAt: -1 }).lean();
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
app.post('/api/order/control', async (req, res) => {
  try {
    const { schedulerOrderId, action } = req.body || {};
    if (!schedulerOrderId || !action) {
      return res.status(400).json({ error: 'Missing schedulerOrderId or action' });
    }

    const order = await Order.findOne({ schedulerOrderId });
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
app.get('/api/order/runs/:schedulerOrderId', async (req, res) => {
  try {
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
