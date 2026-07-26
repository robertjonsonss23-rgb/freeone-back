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
app.use(express.json({ limit: '12mb' }));   // QR images travel as base64 data URLs

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

/* ------------------------------------------------------------
   KEEP-ALIVE (free-tier survival)
   Render's free instance sleeps after ~15 minutes without HTTP
   traffic, which stops the scheduler and makes scheduled runs
   fire late. Pinging ourselves keeps the process resident.

   Render exposes RENDER_EXTERNAL_URL automatically, so this
   normally needs no configuration. Set KEEP_ALIVE_URL to
   override, or KEEP_ALIVE=off to disable (e.g. on a paid plan
   where it is unnecessary).
   ------------------------------------------------------------ */
const KEEP_ALIVE_URL =
  process.env.KEEP_ALIVE === 'off'
    ? ''
    : (process.env.KEEP_ALIVE_URL || process.env.RENDER_EXTERNAL_URL || '').trim();

// Comfortably under Render's ~15 minute idle timeout.
const KEEP_ALIVE_INTERVAL_MS = 10 * 60 * 1000;

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
  // What the user paid, in paise. Used for pro-rata refunds on cancel.
  chargedPaise:     { type: Number, default: 0 },
  refundedPaise:    { type: Number, default: 0 },
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
  /* Owner accounts are yours: they can top up their own wallet without a
     deposit, and they see the markup (your commission) on each order. */
  isOwner:      { type: Boolean, default: false },
  /* Wallet balance in paise (integer) — never floats, so repeated
     debits can't drift. ₹50.00 is stored as 5000. */
  balancePaise: { type: Number, default: 0, min: 0 },
  createdAt:    { type: Date, default: Date.now },
  lastLoginAt:  { type: Date, default: null },
});

/* Immutable ledger. Every credit/debit writes one row, so a balance can
   always be explained and audited. */
const TransactionSchema = new mongoose.Schema({
  userId:       { type: String, required: true, index: true },
  type: {
    type: String,
    enum: ['deposit', 'order_debit', 'refund', 'admin_credit', 'admin_debit'],
    required: true,
  },
  amountPaise:  { type: Number, required: true },   // positive = credit
  balanceAfter: { type: Number, required: true },
  note:         { type: String, default: '' },
  reference:    { type: String, default: '' },       // schedulerOrderId / depositId
  createdAt:    { type: Date, default: Date.now, index: true },
});

/* A user-submitted top-up awaiting admin approval. */
const DepositSchema = new mongoose.Schema({
  userId:      { type: String, required: true, index: true },
  userEmail:   { type: String, default: '' },
  amountPaise: { type: Number, required: true },
  method:      { type: String, enum: ['upi', 'crypto'], required: true },
  methodId:    { type: String, default: '' },        // which UPI id / wallet was used
  reference:   { type: String, required: true },      // UTR or tx hash
  status: {
    type: String,
    enum: ['pending', 'approved', 'rejected'],
    default: 'pending',
    index: true,
  },
  adminNote:   { type: String, default: '' },
  createdAt:   { type: Date, default: Date.now, index: true },
  reviewedAt:  { type: Date, default: null },
});
DepositSchema.index({ status: 1, createdAt: -1 });

/* Admin-controlled payment settings. */
const PaymentSettingsSchema = new mongoose.Schema({
  key:              { type: String, required: true, unique: true, default: 'default' },
  minDepositPaise:  { type: Number, default: 5000 },   // ₹50
  markupPercent:    { type: Number, default: 30 },
  upiEnabled:       { type: Boolean, default: true },
  cryptoEnabled:    { type: Boolean, default: false },
  upiMethods: [{
    _id:         false,
    id:          { type: String, required: true },
    label:       { type: String, default: '' },
    upiId:       { type: String, default: '' },
    payeeName:   { type: String, default: '' },
    instructions:{ type: String, default: '' },
    // Scannable QR, stored as a base64 data URL.
    qrImage:     { type: String, default: '' },
    isActive:    { type: Boolean, default: true },
  }],
  cryptoMethods: [{
    _id:         false,
    id:          { type: String, required: true },
    label:       { type: String, default: '' },
    network:     { type: String, default: '' },
    address:     { type: String, default: '' },
    instructions:{ type: String, default: '' },
    qrImage:     { type: String, default: '' },
    isActive:    { type: Boolean, default: true },
  }],
  updatedAt:        { type: Date, default: Date.now },
});

/* Opaque session tokens. Only the SHA-256 of the token is stored, so a
   database leak does not hand out working sessions. */
const SessionSchema = new mongoose.Schema({
  tokenHash: { type: String, required: true, unique: true, index: true },
  userId:    { type: String, required: true, index: true },
  createdAt: { type: Date, default: Date.now },
  expiresAt: { type: Date, required: true, index: true },
});

/* An SMM provider. The admin can register several; credentials never
   leave the server. */
const PanelSchema = new mongoose.Schema({
  name:      { type: String, required: true },
  apiUrl:    { type: String, required: true },
  apiKey:    { type: String, required: true },
  isActive:  { type: Boolean, default: true },
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now },
});

/* Admin-managed service mapping (singleton, key: 'default').
   Each engagement label holds an ORDERED list of slots. When a label has
   more than one slot the scheduler rotates through them run by run, so
   consecutive runs hit different services (and possibly different panels).
   `serviceIds` is the legacy single-service shape, kept only so existing
   installs can be migrated on boot. */
const SlotSchema = new mongoose.Schema({
  panelId:   { type: String, required: true },
  serviceId: { type: String, required: true },
}, { _id: false });

const PanelConfigSchema = new mongoose.Schema({
  key:        { type: String, required: true, unique: true, default: 'default' },
  // Legacy fields (pre-multi-panel). Migrated then left untouched.
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
  serviceSlots: {
    views:    { type: [SlotSchema], default: [] },
    likes:    { type: [SlotSchema], default: [] },
    shares:   { type: [SlotSchema], default: [] },
    saves:    { type: [SlotSchema], default: [] },
    comments: { type: [SlotSchema], default: [] },
    reposts:  { type: [SlotSchema], default: [] },
  },
  migratedToSlots: { type: Boolean, default: false },
  updatedAt:  { type: Date, default: Date.now },
});

const PanelConfig = mongoose.model('PanelConfig', PanelConfigSchema);
const Panel       = mongoose.model('Panel', PanelSchema);

const Transaction     = mongoose.model('Transaction', TransactionSchema);
const Deposit         = mongoose.model('Deposit', DepositSchema);
const PaymentSettings = mongoose.model('PaymentSettings', PaymentSettingsSchema);

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
    balance: Math.round(Number(user.balancePaise) || 0) / 100,
    isOwner: user.isOwner === true,
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

/* ============================================================
   WALLET
   All amounts are integer paise. Balance changes go through
   findOneAndUpdate with a guard condition so two concurrent
   requests can never both spend the same money.
   ============================================================ */

function toPaise(rupees) {
  return Math.round(Number(rupees || 0) * 100);
}
function toRupees(paise) {
  return Math.round(Number(paise || 0)) / 100;
}

async function getPaymentSettings() {
  let doc = await PaymentSettings.findOne({ key: 'default' });
  if (!doc) doc = await PaymentSettings.create({ key: 'default' });
  return doc;
}

/** Credit a wallet and write a ledger row. Returns the new balance. */
async function creditWallet(userId, amountPaise, { type, note = '', reference = '' }) {
  const amount = Math.round(Number(amountPaise));
  if (!Number.isFinite(amount) || amount <= 0) throw new Error('Credit amount must be positive');

  const user = await User.findByIdAndUpdate(
    userId,
    { $inc: { balancePaise: amount } },
    { new: true }
  );
  if (!user) throw new Error('User not found');

  await Transaction.create({
    userId: String(userId),
    type,
    amountPaise: amount,
    balanceAfter: user.balancePaise,
    note,
    reference,
  });
  return user.balancePaise;
}

/**
 * Debit a wallet only if the funds are there. The `$gte` guard makes the
 * check-and-deduct a single atomic operation, so parallel orders cannot
 * push the balance negative.
 * Returns { ok, balance } — ok:false means insufficient funds.
 */
async function debitWallet(userId, amountPaise, { type, note = '', reference = '' }) {
  const amount = Math.round(Number(amountPaise));
  if (!Number.isFinite(amount) || amount <= 0) throw new Error('Debit amount must be positive');

  const user = await User.findOneAndUpdate(
    { _id: userId, balancePaise: { $gte: amount } },
    { $inc: { balancePaise: -amount } },
    { new: true }
  );
  if (!user) return { ok: false, balance: null };

  await Transaction.create({
    userId: String(userId),
    type,
    amountPaise: -amount,
    balanceAfter: user.balancePaise,
    note,
    reference,
  });
  return { ok: true, balance: user.balancePaise };
}

/* Accept only real image data URLs, and cap the size so the settings
   document can't be bloated by an unbounded upload. */
const MAX_QR_BYTES = 2 * 1024 * 1024;   // 2 MB decoded
function sanitizeQrImage(value, previous = '') {
  if (value === undefined) return previous;          // field omitted -> keep
  const raw = String(value || '').trim();
  if (!raw) return '';                                // explicit clear
  const match = /^data:image\/(png|jpe?g|webp|gif);base64,([A-Za-z0-9+/=]+)$/.exec(raw);
  if (!match) throw new Error('QR must be a PNG, JPG, WEBP or GIF image');
  const bytes = Math.floor(match[2].length * 3 / 4);
  if (bytes > MAX_QR_BYTES) {
    throw new Error(`QR image is too large (${(bytes / 1024 / 1024).toFixed(1)} MB, max 2 MB)`);
  }
  return raw;
}

/** Public view of the payment options a user can pay with. */
function publicPaymentSettings(doc) {
  return {
    minDeposit: toRupees(doc.minDepositPaise),
    upiEnabled: Boolean(doc.upiEnabled),
    cryptoEnabled: Boolean(doc.cryptoEnabled),
    upiMethods: (doc.upiMethods || [])
      .filter(m => m.isActive)
      .map(m => ({
        id: m.id, label: m.label, upiId: m.upiId,
        payeeName: m.payeeName, instructions: m.instructions,
        qrImage: m.qrImage || '',
      })),
    cryptoMethods: (doc.cryptoMethods || [])
      .filter(m => m.isActive)
      .map(m => ({
        id: m.id, label: m.label, network: m.network,
        address: m.address, instructions: m.instructions,
        qrImage: m.qrImage || '',
      })),
  };
}

/** Read the singleton panel config, creating an empty one on first call. */
async function getPanelConfig() {
  let doc = await PanelConfig.findOne({ key: 'default' });
  if (!doc) doc = await PanelConfig.create({ key: 'default' });
  return doc;
}

/* One-time upgrade from the single-panel shape to panels + slots.
   Existing installs keep working with no manual re-entry. */
async function migrateToMultiPanel() {
  const cfg = await getPanelConfig();
  if (cfg.migratedToSlots) return;

  // Nothing configured yet: just flag it and move on.
  if (!cfg.apiUrl || !cfg.apiKey) {
    cfg.migratedToSlots = true;
    await cfg.save();
    return;
  }

  let panel = await Panel.findOne({ apiUrl: cfg.apiUrl, apiKey: cfg.apiKey });
  if (!panel) {
    panel = await Panel.create({
      name: cfg.panelName || 'Panel 1',
      apiUrl: cfg.apiUrl,
      apiKey: cfg.apiKey,
    });
    log(`🔀 Migrated existing panel "${panel.name}" into the panels collection`);
  }

  for (const label of SERVICE_LABELS) {
    const legacyId = String(cfg.serviceIds?.[label] || '').trim();
    if (legacyId && (cfg.serviceSlots?.[label] || []).length === 0) {
      cfg.serviceSlots[label] = [{ panelId: String(panel._id), serviceId: legacyId }];
    }
  }

  cfg.migratedToSlots = true;
  cfg.updatedAt = new Date();
  await cfg.save();
  log('🔀 Service mapping migrated to rotating slots');
}

/** Slots for a label, dropping any that point at a missing/inactive panel. */
function usableSlots(cfg, label, panelsById) {
  const slots = (cfg?.serviceSlots?.[label] || []).map(s => ({
    panelId: String(s.panelId || ''),
    serviceId: String(s.serviceId || '').trim(),
  }));
  return slots.filter(s => {
    if (!s.serviceId) return false;
    const panel = panelsById.get(s.panelId);
    return Boolean(panel && panel.isActive !== false);
  });
}

async function loadPanelsById() {
  const panels = await Panel.find().lean();
  return new Map(panels.map(p => [String(p._id), p]));
}

/** Public view: service ids and panel names, never an API key. */
async function publicPanelConfig(doc) {
  const panelsById = await loadPanelsById();
  const services = {};
  for (const label of SERVICE_LABELS) {
    const slots = usableSlots(doc, label, panelsById);
    services[label] = {
      enabled: slots.length > 0,
      count: slots.length,
      rotating: slots.length > 1,
      slots: slots.map(s => ({
        serviceId: s.serviceId,
        panelId: s.panelId,
        panelName: panelsById.get(s.panelId)?.name || 'Unknown panel',
      })),
    };
  }
  const activePanels = [...panelsById.values()].filter(p => p.isActive !== false);
  return {
    panels: activePanels.map(p => ({ id: String(p._id), name: p.name })),
    services,
    configured: services.views.enabled,
    updatedAt: doc?.updatedAt || null,
  };
}

/** Admin view: adds per-panel detail plus masked keys. */
async function adminPanelConfig(doc) {
  const panels = await Panel.find().sort({ createdAt: 1 }).lean();
  const panelsById = new Map(panels.map(p => [String(p._id), p]));
  const serviceSlots = {};
  for (const label of SERVICE_LABELS) {
    serviceSlots[label] = (doc?.serviceSlots?.[label] || []).map(s => ({
      panelId: String(s.panelId || ''),
      serviceId: String(s.serviceId || ''),
      panelName: panelsById.get(String(s.panelId))?.name || 'Missing panel',
    }));
  }
  return {
    panels: panels.map(p => ({
      id: String(p._id),
      name: p.name,
      apiUrl: p.apiUrl,
      apiKeyMask: p.apiKey ? `••••••••${String(p.apiKey).slice(-4)}` : '',
      isActive: p.isActive !== false,
      createdAt: p.createdAt,
    })),
    serviceSlots,
    configured: usableSlots(doc, 'views', panelsById).length > 0,
    updatedAt: doc?.updatedAt || null,
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

    // Rotate across the admin's slots: run 0 -> slot 0, run 1 -> slot 1, …
    // wrapping around. With one slot this is identical to the old behaviour.
    const slots = Array.isArray(serviceConfig.slots) ? serviceConfig.slots : [];
    if (slots.length === 0) continue;
    let runIndex = -1;

    for (const run of (serviceConfig.runs || [])) {
      runIndex += 1;
      const slot = slots[runIndex % slots.length];
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
        apiUrl: slot.apiUrl,
        apiKey: slot.apiKey,
        service: slot.serviceId,
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
  await migrateToMultiPanel();

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

  /* Keep the free instance awake. Without this the process sleeps after
     ~15 min idle and scheduled runs stop firing until someone visits. */
  if (KEEP_ALIVE_URL) {
    const target = `${KEEP_ALIVE_URL.replace(/\/$/, '')}/api/health`;
    log(`💓 Keep-alive pinging ${target} every ${KEEP_ALIVE_INTERVAL_MS / 60000} min`);
    setInterval(async () => {
      try {
        const res = await fetch(target, { signal: AbortSignal.timeout(20_000) });
        if (!res.ok) warn(`Keep-alive ping returned HTTP ${res.status}`);
      } catch (e) {
        // A failed ping is not fatal; the next one may succeed.
        warn('Keep-alive ping failed:', e?.message || e);
      }
    }, KEEP_ALIVE_INTERVAL_MS);
  } else {
    warn('Keep-alive is OFF. On Render free tier the scheduler will stop when the instance sleeps.');
  }

  /* Warn loudly when delivery is drifting, so the logs show the problem
     rather than it being silent. */
  setInterval(async () => {
    try {
      const now = new Date();
      const oldest = await Run.findOne(
        { status: 'pending', time: { $lte: new Date(now - 10 * 60 * 1000) } },
        { time: 1 }
      ).sort({ time: 1 }).lean();
      if (oldest) {
        const lateMin = Math.round((now - new Date(oldest.time)) / 60000);
        const overdue = await Run.countDocuments({ status: 'pending', time: { $lte: now } });
        warn(`⏰ Delivery lag: ${overdue} run(s) overdue, oldest ${lateMin} min late.`);
      }
    } catch { /* monitoring must never crash the process */ }
  }, 5 * 60 * 1000);

  app.listen(PORT, '0.0.0.0', () => {
    log(`========================================`);
    log(`Server listening on port ${PORT}`);
    log(`MIN_VIEWS_PER_RUN = ${MIN_VIEWS_PER_RUN}`);
    log(`Keep-alive: ${KEEP_ALIVE_URL ? 'ON' : 'OFF'}`);
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
    const panelsById = await loadPanelsById();

    const resolved = {};
    for (const [label, value] of Object.entries(services)) {
      if (!value) continue;
      const normalized = String(label).toLowerCase();
      if (!SERVICE_LABELS.includes(normalized)) continue;

      // Attach each slot's own panel credentials so runs can rotate across
      // different providers.
      const slots = usableSlots(cfg, normalized, panelsById).map(slot => {
        const panel = panelsById.get(slot.panelId);
        return {
          serviceId: slot.serviceId,
          panelId: slot.panelId,
          apiUrl: panel.apiUrl,
          apiKey: panel.apiKey,
        };
      });

      if (slots.length === 0) {
        log(`[ORDER] Skipping "${normalized}" — no usable service slot configured`);
        continue;
      }
      resolved[normalized] = { slots, runs: value.runs || [] };
    }

    if (Object.keys(resolved).length === 0) {
      return res.status(503).json({
        error: 'No SMM services configured yet. Ask the admin to set one up.',
      });
    }

    /* ---- Price the order and charge the wallet BEFORE scheduling ---- */
    // Total the units the client asked for, per label. Quantities are
    // re-derived here rather than trusted from any client-sent price.
    const unitTotals = {};
    for (const [label, value] of Object.entries(resolved)) {
      unitTotals[label] = (value.runs || []).reduce((sum, run) => {
        if (label === 'comments') {
          const lines = String(run.comments || '').split('\n').filter(l => l.trim());
          return sum + Math.min(lines.length, 10);
        }
        return sum + Math.max(0, Math.floor(Number(run.quantity) || 0));
      }, 0);
    }

    const quote = await computeQuote(unitTotals);
    if (!quote.available) {
      return res.status(503).json({ error: quote.reason || 'Could not price this order.' });
    }

    const schedulerOrderId = makeOrderId();

    // Atomic: fails cleanly if the balance is short, and cannot be raced.
    const debit = await debitWallet(req.user._id, quote.totalPaise, {
      type: 'order_debit',
      note: `Order for ${link}`,
      reference: schedulerOrderId,
    });

    if (!debit.ok) {
      return res.status(402).json({
        error: 'Insufficient wallet balance',
        required: toRupees(quote.totalPaise),
        balance: toRupees(req.user.balancePaise),
        shortfall: toRupees(quote.totalPaise - req.user.balancePaise),
      });
    }

    let runs;
    try {
      runs = await addRuns(resolved, { link }, schedulerOrderId);
    } catch (e) {
      // Scheduling blew up after taking the money — hand it straight back.
      await creditWallet(req.user._id, quote.totalPaise, {
        type: 'refund',
        note: 'Automatic refund: order could not be scheduled',
        reference: schedulerOrderId,
      });
      throw e;
    }

    if (runs.length === 0) {
      await creditWallet(req.user._id, quote.totalPaise, {
        type: 'refund',
        note: 'Automatic refund: no runs were scheduled',
        reference: schedulerOrderId,
      });
      return res.status(400).json({
        error: 'No runs could be scheduled (check quantities and times).',
      });
    }

    const orderDoc = await Order.create({
      schedulerOrderId,
      userId: String(req.user._id),
      name: name || `Order ${schedulerOrderId}`,
      link,
      chargedPaise: quote.totalPaise,
      status: 'pending',
      totalRuns: runs.length,
      completedRuns: 0,
      runStatuses: runs.map(() => 'pending'),
      createdAt: new Date(),
      lastUpdatedAt: new Date(),
    });

    log(`📦 Order ${schedulerOrderId}: ${runs.length} run(s), charged ₹${toRupees(quote.totalPaise)}`);
    return res.json({
      success: true,
      message: 'Order scheduled',
      schedulerOrderId,
      status: orderDoc.status,
      completedRuns: 0,
      totalRuns: runs.length,
      charged: toRupees(quote.totalPaise),
      balance: toRupees(debit.balance),
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
      // Count what never ran, so we refund only the unused portion.
      const totalRuns = await Run.countDocuments({ schedulerOrderId });
      const cancelled = await Run.updateMany(
        { schedulerOrderId, status: { $in: ['pending', 'processing', 'paused'] } },
        { $set: { status: 'cancelled', error: 'Cancelled by user' } }
      );
      order.status = 'cancelled';

      const notRun = cancelled.modifiedCount || 0;
      const charged = Number(order.chargedPaise) || 0;
      const alreadyRefunded = Number(order.refundedPaise) || 0;

      if (charged > 0 && notRun > 0 && totalRuns > 0 && alreadyRefunded === 0) {
        const refund = Math.floor((charged * notRun) / totalRuns);
        if (refund > 0) {
          await creditWallet(order.userId, refund, {
            type: 'refund',
            note: `Refund for ${notRun} of ${totalRuns} unexecuted run(s)`,
            reference: schedulerOrderId,
          });
          order.refundedPaise = refund;
          log(`💸 Refunded ₹${toRupees(refund)} on cancel of ${schedulerOrderId}`);
        }
      }

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
/* Compute the price of a set of service volumes.
   Returns paise, including the admin's markup. Shared by /api/quote and
   /api/order so a user is always charged exactly what they were shown. */
async function computeQuote(requestedUnits) {
  const cfg = await getPanelConfig();
  const panelsById = await loadPanelsById();
  const settings = await getPaymentSettings();
  const markup = 1 + (Number(settings.markupPercent) || 0) / 100;

  const catalogueCache = new Map();
  const currencyCache = new Map();

  async function loadPanel(panelId) {
    if (catalogueCache.has(panelId)) return;
    const panel = panelsById.get(panelId);
    if (!panel) { catalogueCache.set(panelId, new Map()); return; }

    const rates = new Map();
    try {
      const params = new URLSearchParams({ key: panel.apiKey, action: 'services' });
      const response = await axios.post(panel.apiUrl, params.toString(), {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        timeout: PROVIDER_HTTP_TIMEOUT_MS,
        validateStatus: () => true,
      });
      const data = response.data;
      const list = Array.isArray(data) ? data
        : Array.isArray(data?.services) ? data.services
        : Array.isArray(data?.data) ? data.data
        : [];
      for (const row of list) {
        const id = String(row?.service ?? row?.id ?? '').trim();
        const rate = Number(String(row?.rate ?? row?.price ?? row?.cost ?? '')
          .replace(/[^\d.]/g, ''));
        if (id && Number.isFinite(rate)) rates.set(id, rate);
      }
    } catch (e) {
      warn(`Quote: catalogue fetch failed for ${panel.name}:`, e?.message || e);
    }
    catalogueCache.set(panelId, rates);

    try {
      const balanceParams = new URLSearchParams({ key: panel.apiKey, action: 'balance' });
      const balanceRes = await axios.post(panel.apiUrl, balanceParams.toString(), {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        timeout: PROVIDER_HTTP_TIMEOUT_MS,
        validateStatus: () => true,
      });
      currencyCache.set(panelId, extractPanelCurrency(balanceRes.data) || 'INR');
    } catch {
      currencyCache.set(panelId, 'INR');
    }
  }

  const fxCache = new Map();
  async function toInr(amount, currency) {
    if (!currency || currency === 'INR') return amount;
    if (!fxCache.has(currency)) {
      try {
        const fx = await getInrExchangeRate(currency);
        fxCache.set(currency, fx.rate);
      } catch { fxCache.set(currency, null); }
    }
    const rate = fxCache.get(currency);
    return rate == null ? null : amount * rate;
  }

  const breakdown = {};
  let costInr = 0;
  let missingRate = false;

  for (const label of SERVICE_LABELS) {
    const units = Math.max(0, Math.floor(Number(requestedUnits?.[label]) || 0));
    if (units <= 0) continue;

    const slots = usableSlots(cfg, label, panelsById);
    if (slots.length === 0) continue;

    const unitsPerSlot = units / slots.length;
    let labelTotal = 0;
    let pricedAny = false;

    for (const slot of slots) {
      await loadPanel(slot.panelId);
      const rate = catalogueCache.get(slot.panelId)?.get(slot.serviceId);
      if (!Number.isFinite(rate) || rate <= 0) { missingRate = true; continue; }

      const native = (unitsPerSlot / 1000) * rate;
      const inr = await toInr(native, currencyCache.get(slot.panelId));
      if (inr == null) { missingRate = true; continue; }
      labelTotal += inr;
      pricedAny = true;
    }

    if (pricedAny) {
      const withMarkup = labelTotal * markup;
      breakdown[label] = Math.round(withMarkup * 100);   // paise
      costInr += labelTotal;
    }
  }

  if (Object.keys(breakdown).length === 0) {
    return {
      available: false,
      reason: missingRate
        ? 'Panel did not return usable rates.'
        : 'No priced services in this order.',
    };
  }

  const totalPaise = Object.values(breakdown).reduce((sum, v) => sum + v, 0);
  const markupPercent = Number(settings.markupPercent) || 0;
  // What the panel charges us, before markup. Used to show your commission.
  const costPaise = Math.round((costInr || 0) * 100);
  return {
    available: true,
    totalPaise,
    breakdownPaise: breakdown,
    markupPercent,
    costPaise,
    profitPaise: Math.max(0, totalPaise - costPaise),
    partial: missingRate,
  };
}

app.post('/api/quote', requireUser, async (req, res) => {
  try {
    const requested = req.body?.services && typeof req.body.services === 'object'
      ? req.body.services
      : {};
    const quote = await computeQuote(requested);

    if (!quote.available) {
      return res.json({ available: false, reason: quote.reason });
    }

    const breakdown = {};
    for (const [label, paise] of Object.entries(quote.breakdownPaise)) {
      breakdown[label] = toRupees(paise);
    }

    const payload = {
      available: true,
      total: toRupees(quote.totalPaise),
      breakdown,
      currency: 'INR',
      partial: quote.partial,
      balance: toRupees(req.user.balancePaise),
      sufficient: req.user.balancePaise >= quote.totalPaise,
    };

    /* Owner accounts see the economics: panel cost, your markup, and the
       resulting commission in rupees. Never sent to normal users. */
    if (req.user.isOwner === true) {
      payload.owner = {
        panelCost: toRupees(quote.costPaise),
        commission: toRupees(quote.profitPaise),
        markupPercent: quote.markupPercent,
      };
    }

    return res.json(payload);
  } catch (e) {
    err('POST /api/quote:', e?.message || e);
    return res.status(500).json({ error: e?.message || 'Could not calculate price' });
  }
});

/* ============================================================
   WALLET & DEPOSIT ROUTES
   ============================================================ */

// ---- Public: which payment methods are available ----
app.get('/api/payment-methods', async (_req, res) => {
  try {
    const settings = await getPaymentSettings();
    res.json(publicPaymentSettings(settings));
  } catch (e) {
    err('GET /api/payment-methods:', e?.message || e);
    res.status(500).json({ error: 'Could not load payment methods' });
  }
});

// ---- Wallet balance + recent ledger ----
app.get('/api/wallet', requireUser, async (req, res) => {
  try {
    const transactions = await Transaction.find({ userId: String(req.user._id) })
      .sort({ createdAt: -1 })
      .limit(50)
      .lean();

    const deposits = await Deposit.find({ userId: String(req.user._id) })
      .sort({ createdAt: -1 })
      .limit(20)
      .lean();

    res.json({
      balance: toRupees(req.user.balancePaise),
      transactions: transactions.map(t => ({
        id: String(t._id),
        type: t.type,
        amount: toRupees(t.amountPaise),
        balanceAfter: toRupees(t.balanceAfter),
        note: t.note,
        reference: t.reference,
        createdAt: t.createdAt,
      })),
      deposits: deposits.map(d => ({
        id: String(d._id),
        amount: toRupees(d.amountPaise),
        method: d.method,
        reference: d.reference,
        status: d.status,
        adminNote: d.adminNote,
        createdAt: d.createdAt,
        reviewedAt: d.reviewedAt,
      })),
    });
  } catch (e) {
    err('GET /api/wallet:', e?.message || e);
    res.status(500).json({ error: 'Could not load wallet' });
  }
});

/* ---- Owner self top-up ----
   Owner accounts are yours, so they can credit their own wallet without a
   real payment. Guarded by isOwner, which only the admin panel can set. */
app.post('/api/wallet/self-topup', requireUser, async (req, res) => {
  try {
    if (req.user.isOwner !== true) {
      return res.status(403).json({ error: 'Not permitted on this account' });
    }
    const amountPaise = toPaise(req.body?.amount);
    if (!Number.isFinite(amountPaise) || amountPaise === 0) {
      return res.status(400).json({ error: 'Enter a non-zero amount' });
    }

    if (amountPaise > 0) {
      const balance = await creditWallet(req.user._id, amountPaise, {
        type: 'admin_credit',
        note: 'Owner self top-up',
      });
      log(`\uD83D\uDC51 Owner top-up +\u20B9${toRupees(amountPaise)} (${req.user.email})`);
      return res.json({ success: true, balance: toRupees(balance) });
    }

    const result = await debitWallet(req.user._id, Math.abs(amountPaise), {
      type: 'admin_debit',
      note: 'Owner self adjustment',
    });
    if (!result.ok) return res.status(400).json({ error: 'Balance is too low' });
    res.json({ success: true, balance: toRupees(result.balance) });
  } catch (e) {
    err('POST /api/wallet/self-topup:', e?.message || e);
    res.status(500).json({ error: 'Could not adjust wallet' });
  }
});

// ---- Submit a top-up for approval ----
app.post('/api/wallet/deposit', requireUser, async (req, res) => {
  try {
    const settings = await getPaymentSettings();
    const amountPaise = toPaise(req.body?.amount);
    const method = String(req.body?.method || '').toLowerCase();
    const reference = String(req.body?.reference || '').trim();
    const methodId = String(req.body?.methodId || '').trim();

    if (method !== 'upi' && method !== 'crypto') {
      return res.status(400).json({ error: 'Choose a valid payment method' });
    }
    if (method === 'upi' && !settings.upiEnabled) {
      return res.status(400).json({ error: 'UPI payments are currently disabled' });
    }
    if (method === 'crypto' && !settings.cryptoEnabled) {
      return res.status(400).json({ error: 'Crypto payments are currently disabled' });
    }
    if (!Number.isFinite(amountPaise) || amountPaise < settings.minDepositPaise) {
      return res.status(400).json({
        error: `Minimum deposit is ₹${toRupees(settings.minDepositPaise)}`,
      });
    }
    if (reference.length < 6) {
      return res.status(400).json({
        error: method === 'upi'
          ? 'Enter the 12-digit UTR / reference number from your payment app'
          : 'Enter the transaction hash',
      });
    }

    // A UTR is unique per payment; block re-use so one payment can't be
    // claimed twice (by the same user or a different one).
    const duplicate = await Deposit.findOne({ reference });
    if (duplicate) {
      return res.status(409).json({
        error: 'This reference number has already been submitted',
      });
    }

    const deposit = await Deposit.create({
      userId: String(req.user._id),
      userEmail: req.user.email,
      amountPaise,
      method,
      methodId,
      reference,
    });

    log(`💰 Deposit request ₹${toRupees(amountPaise)} from ${req.user.email} (${reference})`);
    res.status(201).json({
      success: true,
      deposit: {
        id: String(deposit._id),
        amount: toRupees(deposit.amountPaise),
        status: deposit.status,
        createdAt: deposit.createdAt,
      },
      message: 'Submitted. Your wallet will be credited once the payment is verified.',
    });
  } catch (e) {
    if (e?.code === 11000) {
      return res.status(409).json({ error: 'This reference number has already been submitted' });
    }
    err('POST /api/wallet/deposit:', e?.message || e);
    res.status(500).json({ error: 'Could not submit deposit' });
  }
});

/* ---- Admin: deposit queue ---- */
app.get('/api/admin/deposits', requireAdmin, async (req, res) => {
  try {
    const status = String(req.query?.status || 'pending');
    const filter = status === 'all' ? {} : { status };
    const deposits = await Deposit.find(filter).sort({ createdAt: -1 }).limit(200).lean();
    const pendingCount = await Deposit.countDocuments({ status: 'pending' });

    res.json({
      pendingCount,
      deposits: deposits.map(d => ({
        id: String(d._id),
        userId: d.userId,
        userEmail: d.userEmail,
        amount: toRupees(d.amountPaise),
        method: d.method,
        methodId: d.methodId,
        reference: d.reference,
        status: d.status,
        adminNote: d.adminNote,
        createdAt: d.createdAt,
        reviewedAt: d.reviewedAt,
      })),
    });
  } catch (e) {
    err('GET /api/admin/deposits:', e?.message || e);
    res.status(500).json({ error: 'Could not load deposits' });
  }
});

/* ---- Admin: approve or reject a deposit ---- */
app.post('/api/admin/deposits/:id/review', requireAdmin, async (req, res) => {
  try {
    const action = String(req.body?.action || '');
    const note = String(req.body?.note || '').trim();
    if (action !== 'approve' && action !== 'reject') {
      return res.status(400).json({ error: 'action must be approve or reject' });
    }

    // Only flip a deposit that is still pending. This guard makes a
    // double-click (or two admins) unable to credit the wallet twice.
    const deposit = await Deposit.findOneAndUpdate(
      { _id: req.params.id, status: 'pending' },
      {
        $set: {
          status: action === 'approve' ? 'approved' : 'rejected',
          adminNote: note,
          reviewedAt: new Date(),
        },
      },
      { new: true }
    );

    if (!deposit) {
      return res.status(409).json({ error: 'Deposit not found or already reviewed' });
    }

    let balance = null;
    if (action === 'approve') {
      balance = await creditWallet(deposit.userId, deposit.amountPaise, {
        type: 'deposit',
        note: `${deposit.method.toUpperCase()} deposit — ${deposit.reference}`,
        reference: String(deposit._id),
      });
      log(`✅ Deposit approved: ₹${toRupees(deposit.amountPaise)} → ${deposit.userEmail}`);
    } else {
      log(`❌ Deposit rejected: ${deposit.reference} (${deposit.userEmail})`);
    }

    res.json({
      success: true,
      status: deposit.status,
      newBalance: balance == null ? null : toRupees(balance),
    });
  } catch (e) {
    err('POST /api/admin/deposits/:id/review:', e?.message || e);
    res.status(500).json({ error: 'Could not review deposit' });
  }
});

/* ---- Admin: manual wallet adjustment ---- */
app.post('/api/admin/users/:id/wallet', requireAdmin, async (req, res) => {
  try {
    const amountPaise = toPaise(req.body?.amount);
    const note = String(req.body?.note || 'Manual adjustment').trim();
    if (!Number.isFinite(amountPaise) || amountPaise === 0) {
      return res.status(400).json({ error: 'Enter a non-zero amount' });
    }

    if (amountPaise > 0) {
      const balance = await creditWallet(req.params.id, amountPaise, {
        type: 'admin_credit', note,
      });
      return res.json({ success: true, balance: toRupees(balance) });
    }

    const result = await debitWallet(req.params.id, Math.abs(amountPaise), {
      type: 'admin_debit', note,
    });
    if (!result.ok) return res.status(400).json({ error: 'User balance is too low' });
    res.json({ success: true, balance: toRupees(result.balance) });
  } catch (e) {
    err('POST /api/admin/users/:id/wallet:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not adjust wallet' });
  }
});

/* ---- Admin: payment settings ---- */
app.get('/api/admin/payment-settings', requireAdmin, async (_req, res) => {
  try {
    const doc = await getPaymentSettings();
    res.json({
      minDeposit: toRupees(doc.minDepositPaise),
      markupPercent: doc.markupPercent,
      upiEnabled: doc.upiEnabled,
      cryptoEnabled: doc.cryptoEnabled,
      upiMethods: doc.upiMethods || [],
      cryptoMethods: doc.cryptoMethods || [],
      updatedAt: doc.updatedAt,
    });
  } catch (e) {
    err('GET /api/admin/payment-settings:', e?.message || e);
    res.status(500).json({ error: 'Could not load payment settings' });
  }
});

app.post('/api/admin/payment-settings', requireAdmin, async (req, res) => {
  try {
    const doc = await getPaymentSettings();
    const body = req.body || {};

    if (body.minDeposit !== undefined) {
      const paise = toPaise(body.minDeposit);
      if (!Number.isFinite(paise) || paise < 100) {
        return res.status(400).json({ error: 'Minimum deposit must be at least ₹1' });
      }
      doc.minDepositPaise = paise;
    }
    if (body.markupPercent !== undefined) {
      const pct = Number(body.markupPercent);
      if (!Number.isFinite(pct) || pct < 0 || pct > 1000) {
        return res.status(400).json({ error: 'Markup must be between 0 and 1000%' });
      }
      doc.markupPercent = pct;
    }
    if (typeof body.upiEnabled === 'boolean') doc.upiEnabled = body.upiEnabled;
    if (typeof body.cryptoEnabled === 'boolean') doc.cryptoEnabled = body.cryptoEnabled;

    if (Array.isArray(body.upiMethods)) {
      const existing = new Map((doc.upiMethods || []).map(m => [m.id, m]));
      doc.upiMethods = body.upiMethods.slice(0, 10).map((m, i) => {
        const id = String(m?.id || `upi-${Date.now()}-${i}`);
        return {
          id,
          label: String(m?.label || '').trim(),
          upiId: String(m?.upiId || '').trim(),
          payeeName: String(m?.payeeName || '').trim(),
          instructions: String(m?.instructions || '').trim(),
          qrImage: sanitizeQrImage(m?.qrImage, existing.get(id)?.qrImage || ''),
          isActive: m?.isActive !== false,
        };
      });
    }
    if (Array.isArray(body.cryptoMethods)) {
      const existing = new Map((doc.cryptoMethods || []).map(m => [m.id, m]));
      doc.cryptoMethods = body.cryptoMethods.slice(0, 10).map((m, i) => {
        const id = String(m?.id || `crypto-${Date.now()}-${i}`);
        return {
          id,
          label: String(m?.label || '').trim(),
          network: String(m?.network || '').trim(),
          address: String(m?.address || '').trim(),
          instructions: String(m?.instructions || '').trim(),
          qrImage: sanitizeQrImage(m?.qrImage, existing.get(id)?.qrImage || ''),
          isActive: m?.isActive !== false,
        };
      });
    }

    doc.updatedAt = new Date();
    await doc.save();
    log('⚙️  Payment settings updated');
    res.json({ success: true });
  } catch (e) {
    const msg = e?.message || 'Could not save payment settings';
    if (/QR (must|image)/i.test(msg)) return res.status(400).json({ error: msg });
    err('POST /api/admin/payment-settings:', msg);
    res.status(500).json({ error: msg });
  }
});

app.get('/api/panel-config', async (_req, res) => {
  try {
    const doc = await getPanelConfig();
    res.json(await publicPanelConfig(doc));
  } catch (e) {
    err('GET /api/panel-config:', e?.message || e);
    res.status(500).json({ error: 'Could not load panel configuration' });
  }
});

// ---- Admin: verify password (used by the admin login screen) ----
app.post('/api/admin/verify', requireAdmin, (_req, res) => {
  res.json({ success: true });
});

// ---- Admin: read panels + slot mapping ----
app.get('/api/admin/panel-config', requireAdmin, async (_req, res) => {
  try {
    const doc = await getPanelConfig();
    res.json(await adminPanelConfig(doc));
  } catch (e) {
    err('GET /api/admin/panel-config:', e?.message || e);
    res.status(500).json({ error: 'Could not load panel configuration' });
  }
});

/* ---- Admin: create a panel ---- */
app.post('/api/admin/panels', requireAdmin, async (req, res) => {
  try {
    const name = String(req.body?.name || '').trim();
    const apiUrl = String(req.body?.apiUrl || '').trim();
    const apiKey = String(req.body?.apiKey || '').trim();

    if (!name) return res.status(400).json({ error: 'Panel name is required' });
    if (!validateProviderUrl(apiUrl)) {
      return res.status(400).json({ error: 'API URL must be a valid http(s) URL' });
    }
    if (!apiKey) return res.status(400).json({ error: 'API key is required' });

    const panel = await Panel.create({ name, apiUrl, apiKey });
    log(`➕ Panel added: ${name}`);
    const doc = await getPanelConfig();
    res.status(201).json({ success: true, id: String(panel._id), ...(await adminPanelConfig(doc)) });
  } catch (e) {
    err('POST /api/admin/panels:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not add panel' });
  }
});

/* ---- Admin: update a panel ---- */
app.post('/api/admin/panels/:id', requireAdmin, async (req, res) => {
  try {
    const panel = await Panel.findById(req.params.id);
    if (!panel) return res.status(404).json({ error: 'Panel not found' });

    if (typeof req.body?.name === 'string' && req.body.name.trim()) {
      panel.name = req.body.name.trim();
    }
    if (typeof req.body?.apiUrl === 'string' && req.body.apiUrl.trim()) {
      if (!validateProviderUrl(req.body.apiUrl)) {
        return res.status(400).json({ error: 'API URL must be a valid http(s) URL' });
      }
      panel.apiUrl = req.body.apiUrl.trim();
    }
    // Blank key means "keep the stored one".
    if (typeof req.body?.apiKey === 'string' && req.body.apiKey.trim()) {
      panel.apiKey = req.body.apiKey.trim();
    }
    if (typeof req.body?.isActive === 'boolean') {
      panel.isActive = req.body.isActive;
    }

    panel.updatedAt = new Date();
    await panel.save();
    const doc = await getPanelConfig();
    res.json({ success: true, ...(await adminPanelConfig(doc)) });
  } catch (e) {
    err('POST /api/admin/panels/:id:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not update panel' });
  }
});

/* ---- Admin: delete a panel (and any slots pointing at it) ---- */
app.delete('/api/admin/panels/:id', requireAdmin, async (req, res) => {
  try {
    const panel = await Panel.findById(req.params.id);
    if (!panel) return res.status(404).json({ error: 'Panel not found' });

    await Panel.deleteOne({ _id: panel._id });

    // Drop orphaned slots so the mapping can never reference a dead panel.
    const doc = await getPanelConfig();
    let removed = 0;
    for (const label of SERVICE_LABELS) {
      const before = (doc.serviceSlots[label] || []).length;
      doc.serviceSlots[label] = (doc.serviceSlots[label] || [])
        .filter(slot => String(slot.panelId) !== String(panel._id));
      removed += before - doc.serviceSlots[label].length;
    }
    if (removed > 0) {
      doc.updatedAt = new Date();
      await doc.save();
    }
    log(`🗑️  Panel deleted: ${panel.name} (${removed} slot(s) removed)`);
    res.json({ success: true, removedSlots: removed, ...(await adminPanelConfig(doc)) });
  } catch (e) {
    err('DELETE /api/admin/panels/:id:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not delete panel' });
  }
});

/* ---- Admin: replace the slot list for one or more labels ----
   Body: { serviceSlots: { views: [{panelId, serviceId}, ...], ... } } */
app.post('/api/admin/service-slots', requireAdmin, async (req, res) => {
  try {
    const incoming = req.body?.serviceSlots;
    if (!incoming || typeof incoming !== 'object') {
      return res.status(400).json({ error: 'serviceSlots object is required' });
    }

    const doc = await getPanelConfig();
    const panelsById = await loadPanelsById();

    for (const label of SERVICE_LABELS) {
      if (!(label in incoming)) continue;
      const rows = Array.isArray(incoming[label]) ? incoming[label] : [];

      const cleaned = [];
      for (const row of rows.slice(0, 10)) { // hard cap, keeps rotation sane
        const panelId = String(row?.panelId || '').trim();
        const serviceId = String(row?.serviceId || '').trim();
        if (!panelId || !serviceId) continue;
        if (!panelsById.has(panelId)) {
          return res.status(400).json({ error: `Unknown panel for ${label}` });
        }
        cleaned.push({ panelId, serviceId });
      }
      doc.serviceSlots[label] = cleaned;
    }

    doc.migratedToSlots = true;
    doc.updatedAt = new Date();
    await doc.save();
    log('⚙️  Service slots updated');
    res.json({ success: true, ...(await adminPanelConfig(doc)) });
  } catch (e) {
    err('POST /api/admin/service-slots:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not save service slots' });
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
        isOwner: u.isOwner === true,
        balance: toRupees(u.balancePaise),
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

/* ---- Admin: create an owner account ----
   Creates a normal login that can self-fund and see commission figures. */
app.post('/api/admin/users/owner', requireAdmin, async (req, res) => {
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

    const existing = await User.findOne({ email });
    if (existing) {
      // Promote an existing account rather than failing.
      existing.isOwner = true;
      await existing.save();
      log(`\uD83D\uDC51 Existing account promoted to owner: ${email}`);
      return res.json({ success: true, promoted: true, id: String(existing._id) });
    }

    const salt = makeSalt();
    const user = await User.create({
      email, salt, passwordHash: hashPassword(password, salt),
      name, isOwner: true,
    });
    log(`\uD83D\uDC51 Owner account created: ${email}`);
    res.status(201).json({ success: true, id: String(user._id) });
  } catch (e) {
    if (e?.code === 11000) return res.status(409).json({ error: 'Email already exists' });
    err('POST /api/admin/users/owner:', e?.message || e);
    res.status(500).json({ error: 'Could not create owner account' });
  }
});

/* ---- Admin: toggle owner status on an existing account ---- */
app.post('/api/admin/users/:id/owner', requireAdmin, async (req, res) => {
  try {
    const isOwner = Boolean(req.body?.isOwner);
    const user = await User.findById(req.params.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    user.isOwner = isOwner;
    await user.save();
    res.json({ success: true, id: String(user._id), isOwner });
  } catch (e) {
    err('POST /api/admin/users/:id/owner:', e?.message || e);
    res.status(500).json({ error: 'Could not update user' });
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
    // Either target a saved panel by id, or test unsaved credentials.
    let apiUrl = String(req.body?.apiUrl || '').trim();
    let apiKey = String(req.body?.apiKey || '').trim();

    const panelId = String(req.body?.panelId || '').trim();
    if (panelId) {
      const panel = await Panel.findById(panelId).lean();
      if (!panel) return res.status(404).json({ error: 'Panel not found' });
      apiUrl = apiUrl || panel.apiUrl;
      apiKey = apiKey || panel.apiKey;
    }

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
  // How far behind is the scheduler? On a sleeping free instance this is the
  // number that tells you delivery is drifting.
  let overdue = 0;
  let oldestOverdueMinutes = 0;
  try {
    const now = new Date();
    overdue = await Run.countDocuments({ status: 'pending', time: { $lte: now } });
    if (overdue > 0) {
      const oldest = await Run.findOne(
        { status: 'pending', time: { $lte: now } },
        { time: 1 }
      ).sort({ time: 1 }).lean();
      if (oldest) {
        oldestOverdueMinutes = Math.round((now - new Date(oldest.time)) / 60000);
      }
    }
  } catch { /* health must never throw */ }

  res.json({
    status: 'ok',
    mongoConnected: mongoose.connection.readyState === 1,
    uptime: process.uptime(),
    inFlightTuples: inFlight.size,
    minViewsPerRun: MIN_VIEWS_PER_RUN,
    // Delivery lag indicators
    overdueRuns: overdue,
    oldestOverdueMinutes,
    schedulerHealthy: oldestOverdueMinutes < 10,
    keepAlive: KEEP_ALIVE_URL ? 'on' : 'off',
  });
});
