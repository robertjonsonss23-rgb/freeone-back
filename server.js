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
   TELEGRAM ALERTS
   Pings your phone the moment money needs your attention, so you
   don't have to sit refreshing the admin panel.

   Setup: message @BotFather -> /newbot -> copy the token into
   TELEGRAM_BOT_TOKEN. Then message your own bot once, open
   https://api.telegram.org/bot<TOKEN>/getUpdates and copy the
   chat id into TELEGRAM_CHAT_ID. Unset either one and alerts are
   silently skipped — the app behaves exactly as before.
   ============================================================ */
const TELEGRAM_BOT_TOKEN = (process.env.TELEGRAM_BOT_TOKEN || '').trim();
const TELEGRAM_CHAT_ID   = (process.env.TELEGRAM_CHAT_ID || '').trim();
const TELEGRAM_ENABLED   = Boolean(TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID);
// Overridable so the test suite can point at a local stub instead of Telegram.
const TELEGRAM_API_BASE  = (process.env.TELEGRAM_API_BASE || 'https://api.telegram.org').replace(/\/$/, '');

if (!TELEGRAM_ENABLED) {
  log('ℹ️  Telegram alerts are off (set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to enable).');
}

/** Telegram's HTML mode chokes on raw angle brackets and ampersands. */
function tgEscape(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/* Fire-and-forget: a dead bot token or a Telegram outage must never
   break a payment, so every failure is swallowed after a warning. */
async function notifyTelegram(text) {
  if (!TELEGRAM_ENABLED) return false;
  try {
    const response = await axios.post(
      `${TELEGRAM_API_BASE}/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        chat_id: TELEGRAM_CHAT_ID,
        text,
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      },
      { timeout: 8000, validateStatus: () => true }
    );
    if (response.status !== 200 || response.data?.ok !== true) {
      warn('Telegram alert rejected:', response.data?.description || `HTTP ${response.status}`);
      return false;
    }
    return true;
  } catch (e) {
    warn('Telegram alert failed:', e?.message || e);
    return false;
  }
}

/* ============================================================
   SCHEMAS
   ============================================================ */
const RunSchema = new mongoose.Schema({
  // Stable string id (no float collisions)
  id:                  { type: String, required: true, index: true, unique: true },
  // Which platform this run targets. Older runs predate this and read as ''.
  platform:            { type: String, default: '' },
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
  platform:         { type: String, default: '' },
  totalRuns:        { type: Number, required: true },
  completedRuns:    { type: Number, default: 0 },
  // What the user paid, in paise. Used for pro-rata refunds on cancel.
  chargedPaise:     { type: Number, default: 0 },
  refundedPaise:    { type: Number, default: 0 },
  /* What the SMM panel charges US for this order, in paise, captured at
     the moment of purchase. Stored rather than recomputed because provider
     rates change: re-pricing an old order later would quietly rewrite last
     month's profit. Orders placed before this existed read as 0 and are
     reported separately so the totals stay honest. */
  panelCostPaise:   { type: Number, default: 0 },
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
  /* One-time paywall for the New Order page. Once true it stays true —
     the unlock is for life. Owners bypass the check entirely. */
  hasOrderAccess:   { type: Boolean, default: false },
  orderAccessAt:    { type: Date, default: null },
  /* How the unlock happened: 'payment' (approved purchase) or 'admin'. */
  orderAccessSource:{ type: String, default: '' },
  /* Wallet balance in paise (integer) — never floats, so repeated
     debits can't drift. ₹50.00 is stored as 5000. */
  balancePaise: { type: Number, default: 0, min: 0 },
  /* Which currency this account prefers to SEE prices in. The wallet is
     still held in rupees; this only affects formatting. */
  displayCurrency:  { type: String, default: 'INR' },

  /* ---- Referrals ----
     Every account gets a short shareable code. `referredBy` records who
     invited them; the reward only pays out once their first deposit is
     approved, so fake signups earn nothing. */
  referralCode:     { type: String, index: true, sparse: true, unique: true },
  referredBy:       { type: String, default: '', index: true },  // referrer's userId
  referralRewarded: { type: Boolean, default: false },           // has this referral paid out?
  referralCount:    { type: Number, default: 0 },                // successful invites made
  referralEarnedPaise: { type: Number, default: 0 },
  createdAt:    { type: Date, default: Date.now },
  lastLoginAt:  { type: Date, default: null },
});

/* Immutable ledger. Every credit/debit writes one row, so a balance can
   always be explained and audited. */
const TransactionSchema = new mongoose.Schema({
  userId:       { type: String, required: true, index: true },
  type: {
    type: String,
    enum: ['deposit', 'order_debit', 'refund', 'admin_credit', 'admin_debit', 'referral'],
    required: true,
  },
  amountPaise:  { type: Number, required: true },   // positive = credit
  balanceAfter: { type: Number, required: true },
  note:         { type: String, default: '' },
  reference:    { type: String, default: '' },       // schedulerOrderId / depositId
  createdAt:    { type: Date, default: Date.now, index: true },
});

/* A user-submitted payment awaiting admin approval.
   `purpose` decides what approval does:
     'wallet' → credit the wallet (the original behaviour)
     'access' → unlock the New Order page for life (paywall) */
const DepositSchema = new mongoose.Schema({
  userId:      { type: String, required: true, index: true },
  userEmail:   { type: String, default: '' },
  amountPaise: { type: Number, required: true },
  purpose:     { type: String, enum: ['wallet', 'access'], default: 'wallet', index: true },
  method:      { type: String, enum: ['upi', 'crypto'], required: true },
  methodId:    { type: String, default: '' },        // which UPI id / wallet was used
  reference:   { type: String, required: true },      // UTR or tx hash
  /* For crypto payments: exactly what the user was told to send, captured at
     submission time so the admin verifies against the quoted figure even if
     the price is edited afterwards. */
  cryptoMicros:{ type: Number, default: 0 },
  cryptoCoin:  { type: String, default: '' },
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
  /* Global commission, used for any platform without its own value. Kept as
     the fallback so an existing install keeps pricing exactly as before. */
  markupPercent:    { type: Number, default: 30 },
  /* Per-platform commission overrides: { instagram: 200, youtube: 150 }.
     A platform missing from this map (or set to null) uses markupPercent.
     Mixed type so adding a platform later needs no migration. */
  platformMarkup:   { type: mongoose.Schema.Types.Mixed, default: () => ({}) },
  /* Commission for FOLLOWER growth, which is priced very differently from
     post engagement — followers cost far more per unit, so a markup that
     works for views can be wrong here. Per-platform, same shape and same
     fallback rules as platformMarkup. Empty = use the platform rate. */
  followerMarkup:   { type: mongoose.Schema.Types.Mixed, default: () => ({}) },
  upiEnabled:       { type: Boolean, default: true },
  cryptoEnabled:    { type: Boolean, default: false },
  /* ---- New Order paywall (admin switch) ----
     When OFF nobody is gated, no matter what hasOrderAccess says, so the
     admin can turn it off at any moment and every user is let straight in. */
  paywallEnabled:   { type: Boolean, default: false },
  paywallPricePaise:{ type: Number, default: 49900 },   // ₹499
  /* ---- Display currencies ----
     PRESENTATION ONLY. Every balance, price and ledger row stays in paise
     (INR); these rates just change what a user is shown. `inrPerUnit` is
     "how many rupees one unit is worth", so USD 83 means $1 = ₹83 and a
     ₹499 price displays as $6.01. Rates are typed by the admin. */
  currencies: [{
    _id:        false,
    code:       { type: String, required: true },   // USD, EUR, AED, PKR
    symbol:     { type: String, default: '' },
    inrPerUnit: { type: Number, required: true },
    isActive:   { type: Boolean, default: true },
  }],

  /* ---- Low-balance alerts ----
     Telegram ping when a panel's account drops below this, so you top up
     before deliveries start failing. 0 disables the alerts. */
  lowBalanceThreshold: { type: Number, default: 0 },

  /* ---- Presentation mask for the Orders page ----
     When on, normal users never see failures, errors or long-pending runs;
     those display as 'completed'. Nothing about delivery changes — the
     scheduler keeps retrying and the true state is kept in the database.
     Owner accounts always see the real thing. */
  hideRunProblems:        { type: Boolean, default: false },
  /* Show the bot score to ordinary customers too. Off by default so the
     behaviour is unchanged until the admin opts in — owners always see it
     regardless of this flag. */
  botScoreForUsers:       { type: Boolean, default: false },
  // A pending run older than this reads as 'completed' to normal users.
  pendingGraceMinutes:    { type: Number, default: 15 },

  /* ---- Referral programme ----
     Both sides get a flat reward, paid when the invited user's first
     deposit is approved. */
  referralEnabled:      { type: Boolean, default: false },
  referrerRewardPaise:  { type: Number, default: 5000 },   // ₹50 to the inviter
  refereeRewardPaise:   { type: Number, default: 5000 },   // ₹50 to the friend
  /* Guard against paying out on a token deposit: the friend must put in
     at least this much before any reward is released. */
  referralMinDepositPaise: { type: Number, default: 10000 }, // ₹100
  paywallTitle:     { type: String, default: 'Unlock New Order' },
  paywallBlurb:     { type: String, default: 'One-time payment. Unlocks the New Order page on this account for life.' },
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
    /* Ticker shown next to every crypto amount, e.g. "USDT". */
    coin:        { type: String, default: 'USDT' },
    /* Rupees one unit of `coin` is worth. Set by the admin, same as the
       display-currency rates. When > 0 the user may type any amount and we
       convert it; when 0 they must pick one of the fixed packs below. */
    inrPerUnit:  { type: Number, default: 0 },
  }],
  /* ---- Crypto pricing (no exchange rate; the admin types every figure) ----
     Amounts are integer micro-units (1 USDT = 1_000_000) so repeated maths
     can never drift the way a float would. */
  paywallCryptoMicros: { type: Number, default: 0 },   // 0 = crypto unlock not offered
  /* Top-up packs. A user buying with crypto picks one of these instead of
     typing a rupee amount, because without a rate there is nothing to
     convert an arbitrary amount into. */
  cryptoPacks: [{
    _id:         false,
    id:          { type: String, required: true },
    // What the wallet is credited, in paise.
    amountPaise: { type: Number, required: true },
    // What they must send, in micro-units of `coin`.
    cryptoMicros:{ type: Number, required: true },
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
  /* Legacy single-platform slots. Migrated into platformSlots.instagram on
     boot, then left alone so an older build could still read them. */
  serviceSlots: {
    views:    { type: [SlotSchema], default: [] },
    likes:    { type: [SlotSchema], default: [] },
    shares:   { type: [SlotSchema], default: [] },
    saves:    { type: [SlotSchema], default: [] },
    comments: { type: [SlotSchema], default: [] },
    reposts:  { type: [SlotSchema], default: [] },
  },
  /* Per-platform mapping: platformSlots.tiktok.views = [ {panelId, serviceId} ].
     Mixed type because the metric list differs per platform. */
  platformSlots:   { type: mongoose.Schema.Types.Mixed, default: () => ({}) },
  migratedToSlots: { type: Boolean, default: false },
  migratedToPlatforms: { type: Boolean, default: false },
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
/* Providers reject tiny drip-feeds, and a run under 100 views looks
   unnatural on the target platform. 100 is both the default and a hard
   floor — the UI can raise it, never lower it. */
const MIN_VIEWS_FLOOR = 100;

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
    hasOrderAccess: user.hasOrderAccess === true,
    displayCurrency: String(user.displayCurrency || 'INR').toUpperCase(),
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

/* ============================================================
   PLATFORMS
   Each platform has its own service ids on the provider side, and its own
   meaningful engagement types — YouTube has no "saves", TikTok has no
   "reposts" in the Instagram sense. Everything below is derived from this
   one table, so adding a platform later is a single edit.
   ============================================================ */
const PLATFORMS = ['instagram', 'tiktok', 'youtube'];
const DEFAULT_PLATFORM = 'instagram';

const PLATFORM_METRICS = {
  /* `followers` targets a PROFILE, not a post, so it never appears on the
     New Order page — it is sold through the dedicated Grow Followers page.
     It still lives here so it is mapped, priced and rotated like any other
     service with no special-casing anywhere else. */
  instagram: ['views', 'likes', 'shares', 'saves', 'comments', 'reposts', 'followers'],
  tiktok:    ['views', 'likes', 'shares', 'saves', 'comments', 'followers'],
  youtube:   ['views', 'likes', 'comments', 'subscribers'],
};

/* Metrics that buy profile growth rather than engagement on one post. */
const PROFILE_METRICS = ['followers', 'subscribers'];

/** Platforms the Grow Followers page can sell. */
const FOLLOWER_PLATFORMS = PLATFORMS.filter(p => PLATFORM_METRICS[p].includes('followers'));

/* A followers order must point at a PROFILE. Sending a post/reel/video URL to
   a followers service burns the customer's money for nothing, and the panel
   usually reports success anyway — so it is refused up front rather than
   discovered later. Deliberately permissive: anything that is not obviously
   a post is allowed, because URL shapes vary and a false refusal is worse
   than a rare miss. */
const POST_URL_PATTERNS = [
  /\/(p|reel|reels|tv|stories)\//i,   // instagram.com/p/... , /reel/...
  /\/video\//i,                       // tiktok.com/@user/video/...
  /\/(watch|shorts)\b/i,              // youtube.com/watch?v= , /shorts/
  /[?&]v=/i,
];

function looksLikePostUrl(link) {
  return POST_URL_PATTERNS.some(re => re.test(String(link || '')));
}

const PLATFORM_LABELS = {
  instagram: 'Instagram',
  tiktok: 'TikTok',
  youtube: 'YouTube',
};

/* Every metric used by any platform. The runs collection and the pricing
   code work off this union, so a metric only has to be listed once above. */
const SERVICE_LABELS = [...new Set(Object.values(PLATFORM_METRICS).flat())];

function normalizePlatform(value) {
  const v = String(value || '').trim().toLowerCase();
  return PLATFORMS.includes(v) ? v : DEFAULT_PLATFORM;
}

/** Is this metric meaningful on this platform? */
function metricAllowed(platform, metric) {
  return (PLATFORM_METRICS[normalizePlatform(platform)] || []).includes(String(metric).toLowerCase());
}

/** Commission % for one platform: its own override, else the global value.
    A blank/invalid override deliberately falls back rather than becoming 0,
    so a mistyped field can never silently sell at cost. */
function readPercent(raw) {
  if (raw === null || raw === undefined || raw === '') return null;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? n : null;
}

/**
 * Commission % to apply.
 *
 * `kind` is 'followers' for profile-growth orders and 'engagement' for
 * everything else. Resolution order, most specific first:
 *   followerMarkup[platform] -> platformMarkup[platform] -> markupPercent
 * so an unset field always inherits rather than silently becoming 0.
 */
function markupFor(settings, platform, kind = 'engagement') {
  const p = normalizePlatform(platform);
  if (kind === 'followers') {
    const own = readPercent(settings?.followerMarkup?.[p]);
    if (own !== null) return own;
  }
  const perPlatform = readPercent(settings?.platformMarkup?.[p]);
  if (perPlatform !== null) return perPlatform;
  return readPercent(settings?.markupPercent) ?? 0;
}

function emptyPlatformSlots() {
  const out = {};
  for (const p of PLATFORMS) {
    out[p] = {};
    for (const m of PLATFORM_METRICS[p]) out[p][m] = [];
  }
  return out;
}

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

/* Crypto amounts use 6 decimal places (the USDT standard) held as integers,
   so 5.67 USDT is stored as 5_670_000 and never suffers float drift. */
const CRYPTO_SCALE = 1_000_000;
function toMicros(amount) {
  return Math.round(Number(amount || 0) * CRYPTO_SCALE);
}
function fromMicros(micros) {
  return Math.round(Number(micros) || 0) / CRYPTO_SCALE;
}
/** Trim trailing zeros: 5.670000 -> "5.67", 6.000000 -> "6". */
function formatCrypto(micros) {
  const value = fromMicros(micros);
  return String(Number(value.toFixed(6)));
}

// Sensible starting points so the admin panel isn't an empty form.
const CURRENCY_DEFAULTS = [
  { code: 'USD', symbol: '$',   inrPerUnit: 83 },
  { code: 'EUR', symbol: '€',   inrPerUnit: 90 },
  { code: 'AED', symbol: 'د.إ', inrPerUnit: 23 },
  { code: 'PKR', symbol: '₨',   inrPerUnit: 0.3 },
];

async function getPaymentSettings() {
  let doc = await PaymentSettings.findOne({ key: 'default' });
  if (!doc) doc = await PaymentSettings.create({ key: 'default' });
  /* Seed starter currency rows once, so the admin edits real numbers rather
     than an empty table. They start INACTIVE — nothing appears to users
     until the admin has checked the rates and switched them on. */
  if (!Array.isArray(doc.currencies) || doc.currencies.length === 0) {
    doc.currencies = CURRENCY_DEFAULTS.map(c => ({ ...c, isActive: false }));
    await doc.save();
  }
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
    /* Drives whether the New Order page shows the bot score to this user. */
    botScoreForUsers: doc.botScoreForUsers === true,
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
        coin: m.coin || 'USDT',
        inrPerUnit: Number(m.inrPerUnit) || 0,
      })),
    /* Fixed top-up packs for crypto buyers. Sorted cheapest first so the
       list reads naturally. */
    cryptoPacks: (doc.cryptoPacks || [])
      .filter(p => p.isActive && p.amountPaise > 0 && p.cryptoMicros > 0)
      .sort((a, b) => a.amountPaise - b.amountPaise)
      .map(p => ({
        id: p.id,
        amount: toRupees(p.amountPaise),
        crypto: formatCrypto(p.cryptoMicros),
      })),
    currencies: activeCurrencies(doc),
    paywall: {
      enabled: Boolean(doc.paywallEnabled),
      price: toRupees(doc.paywallPricePaise),
      // "" when the admin hasn't priced the unlock in crypto yet.
      cryptoPrice: paywallCryptoPrice(doc),
      title: doc.paywallTitle || 'Unlock New Order',
      blurb: doc.paywallBlurb || '',
    },
  };
}

/* ============================================================
   ORDERS DISPLAY MASK
   A purely cosmetic layer over what a normal user is shown. The stored
   run documents are never modified, so:
     - the scheduler keeps retrying and can still deliver
     - refunds on cancel still use the real state
     - the admin panel and owner accounts see the truth
   ============================================================ */

/** Should this request see the real run state? */
function seesRawRuns(user, settings) {
  if (!settings || settings.hideRunProblems !== true) return true;  // mask off
  return user?.isOwner === true;                                     // owners always
}

/* Present one run to a normal user.
   - failed / retrying           -> completed, error stripped
   - pending past the grace window -> completed
   Everything else passes through untouched. */
function maskRun(run, graceMs, nowMs) {
  const status = String(run.status || '');
  let shown = status;

  if (status === 'failed' || status === 'retrying') {
    shown = 'completed';
  } else if (status === 'pending' || status === 'processing') {
    // `time` is when the run was due; overdue by more than the grace window
    // means the user has been staring at "pending" for too long.
    const due = new Date(run.time || run.createdAt || nowMs).getTime();
    if (Number.isFinite(due) && nowMs - due >= graceMs) shown = 'completed';
  }

  return {
    ...run,
    status: shown,
    // Never leak the reason, the retry count, or a missing provider id.
    error: shown === 'completed' && status !== 'completed' ? null : (run.error ?? null),
    attempts: shown === 'completed' && status !== 'completed' ? 0 : (run.attempts ?? 0),
  };
}

/** Recount an order's headline numbers from already-masked runs. */
function maskOrderTotals(order, maskedRuns) {
  const completed = maskedRuns.filter(r => r.status === 'completed').length;
  const cancelled = maskedRuns.filter(r => r.status === 'cancelled').length;
  const active = maskedRuns.length - cancelled;

  let status = order.status;
  // A cancelled order stays cancelled; otherwise hide 'failed' entirely.
  if (status !== 'cancelled') {
    if (active > 0 && completed === active) status = 'completed';
    else if (status === 'failed') status = 'running';
  }

  return {
    status,
    completedRuns: completed,
    runStatuses: maskedRuns.map(r => r.status),
  };
}

/* Convenience: fetch the mask settings once per request. */
async function runMaskContext(user) {
  const settings = await getPaymentSettings();
  const raw = seesRawRuns(user, settings);
  return {
    raw,
    graceMs: Math.max(0, Number(settings.pendingGraceMinutes) || 0) * 60 * 1000,
    now: Date.now(),
  };
}

/* ============================================================
   REFERRALS
   Reward is released only when the invited user's FIRST deposit is
   approved, so creating throwaway accounts earns nothing.
   ============================================================ */

/* Unambiguous alphabet: no O/0, I/1, so codes survive being read aloud
   or copied off a screenshot. */
const REF_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function makeReferralCode(length = 6) {
  let out = '';
  const bytes = crypto.randomBytes(length);
  for (let i = 0; i < length; i++) out += REF_ALPHABET[bytes[i] % REF_ALPHABET.length];
  return out;
}

function normalizeReferralCode(value) {
  return String(value || '').trim().toUpperCase().replace(/[^A-Z0-9]/g, '');
}

/** Give a user a unique code, retrying on the (unlikely) collision. */
async function ensureReferralCode(user) {
  if (user.referralCode) return user.referralCode;
  for (let attempt = 0; attempt < 6; attempt++) {
    const code = makeReferralCode();
    const clash = await User.findOne({ referralCode: code }).lean();
    if (clash) continue;
    user.referralCode = code;
    try {
      await user.save();
      return code;
    } catch (e) {
      if (e?.code !== 11000) throw e;   // duplicate key: loop and try again
    }
  }
  // Fall back to a longer code rather than leaving the user without one.
  const fallback = makeReferralCode(10);
  user.referralCode = fallback;
  await user.save();
  return fallback;
}

/* Pay out a referral, if this deposit qualifies.
   Called after a deposit is approved. Safe to call repeatedly: the
   `referralRewarded` flag is flipped with an atomic guarded update, so
   two concurrent approvals can never both pay. */
async function maybePayReferral(userId, depositPaise) {
  try {
    const settings = await getPaymentSettings();
    if (settings.referralEnabled !== true) return null;

    const referee = await User.findById(userId);
    if (!referee) return null;
    if (!referee.referredBy) return null;            // nobody invited them
    if (referee.referralRewarded) return null;       // already paid
    if (depositPaise < settings.referralMinDepositPaise) return null;

    const referrer = await User.findById(referee.referredBy);
    if (!referrer || referrer.isActive === false) return null;

    /* Claim the payout atomically. Whoever flips false->true wins; any
       parallel call sees matchedCount 0 and stops. */
    const claim = await User.updateOne(
      { _id: referee._id, referralRewarded: { $ne: true } },
      { $set: { referralRewarded: true } }
    );
    if (claim.matchedCount === 0 || claim.modifiedCount === 0) return null;

    const refereeReward = Math.max(0, Math.round(settings.refereeRewardPaise));
    const referrerReward = Math.max(0, Math.round(settings.referrerRewardPaise));

    if (refereeReward > 0) {
      await creditWallet(referee._id, refereeReward, {
        type: 'referral',
        note: 'Referral bonus — welcome!',
        reference: String(referrer._id),
      });
    }
    if (referrerReward > 0) {
      await creditWallet(referrer._id, referrerReward, {
        type: 'referral',
        note: `Referral bonus — ${referee.email} joined`,
        reference: String(referee._id),
      });
    }

    await User.updateOne(
      { _id: referrer._id },
      { $inc: { referralCount: 1, referralEarnedPaise: referrerReward } }
    );

    log(`🎁 Referral paid: ${referrer.email} +₹${toRupees(referrerReward)}, ${referee.email} +₹${toRupees(refereeReward)}`);
    notifyTelegram(
      `🎁 <b>Referral reward paid</b>\n\n` +
      `Inviter: ${tgEscape(referrer.email)} +₹${toRupees(referrerReward)}\n` +
      `Friend: ${tgEscape(referee.email)} +₹${toRupees(refereeReward)}`
    );
    return { referrerReward, refereeReward };
  } catch (e) {
    // A referral must never break the deposit that triggered it.
    err('maybePayReferral:', e?.message || e);
    return null;
  }
}

/* ============================================================
   DISPLAY CURRENCIES
   Nothing here touches stored money. Balances, charges, refunds and the
   ledger remain integer paise; these helpers only decide how a number is
   rendered for a given account.
   ============================================================ */

/** Currencies a user may pick: always INR, plus whatever the admin enabled. */
function activeCurrencies(settings) {
  const rows = (settings?.currencies || [])
    .filter(c => c.isActive !== false && Number(c.inrPerUnit) > 0)
    .map(c => ({
      code: String(c.code).toUpperCase(),
      symbol: c.symbol || '',
      inrPerUnit: Number(c.inrPerUnit),
    }));
  return [{ code: 'INR', symbol: '₹', inrPerUnit: 1 }, ...rows];
}

/* What the unlock costs in crypto, as a display string.
   Priority: the admin's explicit figure, else convert the rupee price using
   the first active wallet's rate. Empty when neither is available. */
function paywallCryptoPrice(settings) {
  if (settings?.paywallCryptoMicros > 0) return formatCrypto(settings.paywallCryptoMicros);
  const wallet = (settings?.cryptoMethods || []).find(
    m => m.isActive !== false && Number(m.inrPerUnit) > 0
  );
  const rate = Number(wallet?.inrPerUnit) || 0;
  if (rate <= 0) return '';
  const rupees = toRupees(settings?.paywallPricePaise);
  return formatCrypto(Math.round((rupees / rate) * CRYPTO_SCALE));
}

/** The display settings for one account, falling back to INR. */
function currencyFor(user, settings) {
  const wanted = String(user?.displayCurrency || 'INR').toUpperCase();
  const list = activeCurrencies(settings);
  return list.find(c => c.code === wanted) || list[0];
}

/* ============================================================
   NEW ORDER PAYWALL
   A single admin switch gates the New Order page. Three ways in:
     1. the switch is off        → everybody is allowed
     2. the account is an owner  → always allowed (it's yours)
     3. hasOrderAccess is true   → they paid once, allowed for life
   ============================================================ */

/** True when this user may use the New Order page right now. */
function userCanOrder(user, settings) {
  if (!settings || settings.paywallEnabled !== true) return true;
  if (!user) return false;
  if (user.isOwner === true) return true;
  return user.hasOrderAccess === true;
}

/** The paywall block returned to a signed-in user. */
function orderAccessPayload(user, settings) {
  const enabled = Boolean(settings?.paywallEnabled);
  return {
    paywallEnabled: enabled,
    // Reflects the reason, so the UI can say "included with your owner account".
    allowed: userCanOrder(user, settings),
    unlocked: user?.hasOrderAccess === true,
    isOwner: user?.isOwner === true,
    price: toRupees(settings?.paywallPricePaise),
    /* Crypto price for the unlock. An explicit figure wins; otherwise it is
       derived from the first crypto wallet's rate, so setting one rate covers
       both deposits and the paywall. "" means crypto isn't offered. */
    cryptoPrice: paywallCryptoPrice(settings),
    cryptoCoin: (settings?.cryptoMethods || []).find(m => m.isActive !== false)?.coin || 'USDT',
    title: settings?.paywallTitle || 'Unlock New Order',
    blurb: settings?.paywallBlurb || '',
    unlockedAt: user?.orderAccessAt || null,
  };
}

/** Flip a user's lifetime access on. Idempotent. */
async function grantOrderAccess(userId, source) {
  const user = await User.findByIdAndUpdate(
    userId,
    { $set: { hasOrderAccess: true, orderAccessAt: new Date(), orderAccessSource: source } },
    { new: true }
  );
  return user;
}

/** Route guard for anything that creates or changes an order. */
async function requireOrderAccess(req, res, next) {
  try {
    const settings = await getPaymentSettings();
    if (userCanOrder(req.user, settings)) return next();
    return res.status(402).json({
      error: 'Your account has not unlocked the New Order page yet.',
      paywall: orderAccessPayload(req.user, settings),
    });
  } catch (e) {
    err('requireOrderAccess:', e?.message || e);
    res.status(500).json({ error: 'Access check failed' });
  }
}

/** Read the singleton panel config, creating an empty one on first call. */
async function getPanelConfig() {
  let doc = await PanelConfig.findOne({ key: 'default' });
  if (!doc) doc = await PanelConfig.create({ key: 'default' });
  return doc;
}

/* One-time upgrade from the single-panel shape to panels + slots.
   Existing installs keep working with no manual re-entry. */
/* One-time upgrade to per-platform slots.
   Everything currently mapped is Instagram, so it moves there verbatim and
   the other platforms start empty. Runs idempotently and never deletes the
   old data, so a rollback is possible. */
async function migrateToPlatforms() {
  const cfg = await getPanelConfig();
  const existing = cfg.platformSlots && typeof cfg.platformSlots === 'object'
    ? cfg.platformSlots
    : {};

  const next = emptyPlatformSlots();
  // Preserve anything already stored per-platform.
  for (const platform of PLATFORMS) {
    for (const metric of PLATFORM_METRICS[platform]) {
      const rows = existing?.[platform]?.[metric];
      if (Array.isArray(rows) && rows.length > 0) {
        next[platform][metric] = rows.map(r => ({
          panelId: String(r.panelId || ''),
          serviceId: String(r.serviceId || ''),
        }));
      }
    }
  }

  if (!cfg.migratedToPlatforms) {
    let moved = 0;
    for (const metric of PLATFORM_METRICS[DEFAULT_PLATFORM]) {
      const legacy = cfg.serviceSlots?.[metric] || [];
      // Don't clobber a platform mapping that already exists.
      if (legacy.length > 0 && next[DEFAULT_PLATFORM][metric].length === 0) {
        next[DEFAULT_PLATFORM][metric] = legacy.map(r => ({
          panelId: String(r.panelId || ''),
          serviceId: String(r.serviceId || ''),
        }));
        moved += legacy.length;
      }
    }
    cfg.migratedToPlatforms = true;
    if (moved > 0) log(`🔀 Moved ${moved} existing service slot(s) to Instagram`);
  }

  cfg.platformSlots = next;
  cfg.markModified('platformSlots');
  cfg.updatedAt = new Date();
  await cfg.save();
}

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

/** Slots for one platform+metric, dropping any that point at a dead panel. */
function usableSlots(cfg, label, panelsById, platform = DEFAULT_PLATFORM) {
  const p = normalizePlatform(platform);
  /* Read the per-platform map; fall back to the legacy flat map for
     Instagram so an un-migrated install keeps working. */
  const raw = cfg?.platformSlots?.[p]?.[label]
    ?? (p === DEFAULT_PLATFORM ? cfg?.serviceSlots?.[label] : null)
    ?? [];
  const slots = raw.map(s => ({
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
/* What the New Order page needs to know: which services are available, and
   nothing else. Provider names and service ids are commercially sensitive —
   they tell a customer exactly where to buy the same thing cheaper — so they
   are only included for owner accounts. */
async function publicPanelConfig(doc, { includeProviders = false } = {}) {
  const panelsById = await loadPanelsById();

  const describe = (platform, label) => {
    const slots = usableSlots(doc, label, panelsById, platform);
    return {
      enabled: slots.length > 0,
      count: slots.length,
      rotating: slots.length > 1,
      ...(includeProviders
        ? {
            slots: slots.map(s => ({
              serviceId: s.serviceId,
              panelId: s.panelId,
              panelName: panelsById.get(s.panelId)?.name || 'Unknown panel',
            })),
          }
        : {}),
    };
  };

  /* Per-platform availability, so the New Order page can show only the
     engagement types that platform actually supports AND has mapped. */
  const platforms = {};
  for (const platform of PLATFORMS) {
    const services = {};
    for (const label of PLATFORM_METRICS[platform]) services[label] = describe(platform, label);
    platforms[platform] = {
      key: platform,
      label: PLATFORM_LABELS[platform],
      metrics: PLATFORM_METRICS[platform],
      services,
      // A platform is usable once its Views mapping exists.
      configured: services.views?.enabled === true,
      /* Separate gate for the Grow Followers page: it needs a followers
         service, which has nothing to do with the views mapping. */
      followersConfigured: services.followers?.enabled === true,
    };
  }

  // Flat view of the default platform, so older clients keep working.
  const services = {};
  for (const label of SERVICE_LABELS) services[label] = describe(DEFAULT_PLATFORM, label);

  const activePanels = [...panelsById.values()].filter(p => p.isActive !== false);
  return {
    // Normal users get a count only; owners get the real list.
    panels: includeProviders
      ? activePanels.map(p => ({ id: String(p._id), name: p.name }))
      : [],
    panelCount: activePanels.length,
    platforms,
    services,
    configured: Object.values(platforms).some(p => p.configured),
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
  /* The editable per-platform mapping. */
  const platformSlots = {};
  const platformConfigured = {};
  for (const platform of PLATFORMS) {
    platformSlots[platform] = {};
    for (const metric of PLATFORM_METRICS[platform]) {
      const raw = doc?.platformSlots?.[platform]?.[metric]
        ?? (platform === DEFAULT_PLATFORM ? doc?.serviceSlots?.[metric] : null)
        ?? [];
      platformSlots[platform][metric] = raw.map(s => ({
        panelId: String(s.panelId || ''),
        serviceId: String(s.serviceId || ''),
        panelName: panelsById.get(String(s.panelId))?.name || 'Missing panel',
      }));
    }
    platformConfigured[platform] = usableSlots(doc, 'views', panelsById, platform).length > 0;
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
    platformSlots,
    platformConfigured,
    platforms: PLATFORMS.map(k => ({ key: k, label: PLATFORM_LABELS[k], metrics: PLATFORM_METRICS[k] })),
    configured: Object.values(platformConfigured).some(Boolean),
    updatedAt: doc?.updatedAt || null,
  };
}

async function loadSettings() {
  /* Minimum views per run used to live here as a single shared value, which
     meant one customer changing it silently re-planned everyone else's
     orders. It is now chosen per order and validated against
     MIN_VIEWS_FLOOR, so there is no cross-account setting left to load.
     The old document is removed once, then this is a no-op. */
  try {
    const legacy = await Settings.findOneAndDelete({ key: 'minViewsPerRun' });
    if (legacy) {
      log(`🧹 Removed the old shared minViewsPerRun setting (was ${legacy.value}); it is per-order now`);
    }
  } catch (e) {
    warn('Could not tidy legacy settings:', e.message);
  }
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
        /* The floor is a hard platform rule; anything above it is this
           customer's own choice and must not depend on a shared setting. */
        if (!run.quantity || run.quantity < MIN_VIEWS_FLOOR) {
          log(`[ADD] SKIP VIEWS qty=${run.quantity} < FLOOR=${MIN_VIEWS_FLOOR}`);
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
        platform: baseConfig.platform || DEFAULT_PLATFORM,
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
  await migrateToPlatforms();

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

  /* Start scheduler. SCHEDULER=off exists so the test suite can inspect run
     states without the ticker racing it; never set this in production. */
  if (String(process.env.SCHEDULER || '').toLowerCase() === 'off') {
    warn('Scheduler is DISABLED (SCHEDULER=off). Runs will not execute.');
  } else {
    setInterval(schedulerTick, TICK_INTERVAL_MS);
    log(`🚀 Scheduler running every ${TICK_INTERVAL_MS / 1000}s`);
  }

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

  /* Low panel balance -> Telegram. Checked hourly; the first check is
     delayed a little so boot isn't slowed by provider calls. */
  setTimeout(() => { checkPanelBalances().catch(() => {}); }, 60_000);
  setInterval(() => { checkPanelBalances().catch(() => {}); }, LOW_BALANCE_CHECK_MS);

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
    log(`MIN_VIEWS_FLOOR = ${MIN_VIEWS_FLOOR} (per-order, not shared)`);
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
app.post('/api/order', requireUser, requireOrderAccess, async (req, res) => {
  try {
    const { link, services, name } = req.body || {};
    if (!link || !services) {
      return res.status(400).json({ error: 'Missing required fields' });
    }
    const platform = normalizePlatform(req.body?.platform);

    /* Profile-growth services need a profile URL. Checked before any money
       moves, so a mistyped link costs the customer nothing. */
    const wantsProfileMetric = Object.keys(services || {})
      .some(k => PROFILE_METRICS.includes(String(k).toLowerCase()));
    if (wantsProfileMetric && looksLikePostUrl(link)) {
      return res.status(400).json({
        error: 'Followers are delivered to a profile, not a single post. '
             + 'Use your profile link (for example instagram.com/yourname).',
      });
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
      // Silently drop metrics this platform doesn't support (YouTube saves).
      if (!metricAllowed(platform, normalized)) continue;

      // Attach each slot's own panel credentials so runs can rotate across
      // different providers.
      const slots = usableSlots(cfg, normalized, panelsById, platform).map(slot => {
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
    /* Per-run quantities, in the same order addRuns() will schedule them, so
       the price reflects the ACTUAL slot rotation rather than an even split.
       With panels on different rates those two answers can differ a lot. */
    const unitTotals = {};
    for (const [label, value] of Object.entries(resolved)) {
      unitTotals[label] = (value.runs || []).map(run => {
        if (label === 'comments') {
          const lines = String(run.comments || '').split('\n').filter(l => l.trim());
          return Math.min(lines.length, 10);
        }
        return Math.max(0, Math.floor(Number(run.quantity) || 0));
      });
    }

    const quote = await computeQuote(unitTotals, platform);
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
      runs = await addRuns(resolved, { link, platform }, schedulerOrderId);
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
      platform,
      chargedPaise: quote.totalPaise,
      panelCostPaise: Number(quote.costPaise) || 0,
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

    const rawRuns = await Run.find({ schedulerOrderId }).lean();
    const mask = await runMaskContext(req.user);
    const runs = mask.raw
      ? rawRuns
      : rawRuns.map(r => maskRun(r, mask.graceMs, mask.now));
    const totals = mask.raw
      ? { status: order.status, completedRuns: order.completedRuns, runStatuses: order.runStatuses }
      : maskOrderTotals(order, runs);

    return res.json({
      schedulerOrderId: order.schedulerOrderId,
      name: order.name,
      link: order.link,
      platform: order.platform || DEFAULT_PLATFORM,
      status: totals.status,
      totalRuns: order.totalRuns,
      completedRuns: totals.completedRuns,
      runStatuses: totals.runStatuses,
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
    const mask = await runMaskContext(req.user);
    const result = await Promise.all(orders.map(async (o) => {
      const rawRuns = await Run.find(
        { schedulerOrderId: o.schedulerOrderId },
        { id: 1, label: 1, quantity: 1, time: 1, status: 1, smmOrderId: 1, error: 1, attempts: 1 }
      ).lean();
      if (mask.raw) return { ...o, runs: rawRuns };

      const runs = rawRuns.map(r => maskRun(r, mask.graceMs, mask.now));
      return { ...o, ...maskOrderTotals(o, runs), runs };
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

    const rawRuns = await Run.find({ schedulerOrderId: req.params.schedulerOrderId }).lean();
    const mask = await runMaskContext(req.user);
    const runs = mask.raw
      ? rawRuns
      : rawRuns.map(r => maskRun(r, mask.graceMs, mask.now));

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
/* Minimum views per run is a PER-ORDER choice, not a site-wide setting:
   one customer raising it must never change anyone else's planning. This
   endpoint therefore only reports the platform floor. */
app.get('/api/settings/min-views', (_req, res) => {
  res.json({ minViewsPerRun: MIN_VIEWS_FLOOR, minimum: MIN_VIEWS_FLOOR });
});

/* Kept for backwards compatibility with older clients. It validates the
   value but deliberately stores nothing — the figure now travels with each
   order instead of being shared between accounts. */
app.post('/api/settings/min-views', async (req, res) => {
  const { minViewsPerRun } = req.body || {};
  if (typeof minViewsPerRun !== 'number' || !Number.isFinite(minViewsPerRun)) {
    return res.status(400).json({ error: 'Invalid minViewsPerRun value' });
  }
  if (minViewsPerRun < MIN_VIEWS_FLOOR) {
    return res.status(400).json({
      error: `Minimum views per run cannot be below ${MIN_VIEWS_FLOOR}`,
      minimum: MIN_VIEWS_FLOOR,
    });
  }
  res.json({
    success: true,
    minViewsPerRun: Math.floor(minViewsPerRun),
    minimum: MIN_VIEWS_FLOOR,
    perOrder: true,
  });
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
      minViewsPerRun: MIN_VIEWS_FLOOR,
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

    /* Resolve the referral code before creating the account. An unknown or
       self-referring code is ignored rather than blocking the signup —
       never lose a registration over a typo'd code. */
    let referredBy = '';
    const code = normalizeReferralCode(req.body?.referralCode);
    if (code) {
      const referrer = await User.findOne({ referralCode: code }).lean();
      if (referrer && referrer.isActive !== false) referredBy = String(referrer._id);
    }

    const salt = makeSalt();
    const user = await User.create({
      email,
      salt,
      passwordHash: hashPassword(password, salt),
      name,
      referredBy,
      referralCode: makeReferralCode(),
      lastLoginAt: new Date(),
    });

    const token = await createSession(user._id);
    log(`👤 New account: ${email}${referredBy ? ` (referred by ${code})` : ''}`);
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
app.get('/api/auth/me', requireUser, async (req, res) => {
  try {
    const settings = await getPaymentSettings();
    res.json({
      user: publicUser(req.user),
      orderAccess: orderAccessPayload(req.user, settings),
    });
  } catch (e) {
    // The identity still matters even if settings can't be read.
    res.json({ user: publicUser(req.user) });
  }
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
/* ============================================================
   PANEL CATALOGUE CACHE
   Pricing needs each panel's rate list. Fetching it on every quote meant
   6 panels x 2 calls = 12 round trips per keystroke-debounce, ~3s per
   quote, and no sharing between users. Rates change rarely, so they are
   cached process-wide for a few minutes.

   Two properties matter:
     - in-flight de-duplication: ten users pricing at once trigger ONE
       fetch per panel, not ten
     - stale-on-failure: if a panel is down we keep serving the last known
       rates rather than pricing the order at zero
   ============================================================ */
const PANEL_CATALOGUE_TTL_MS = 5 * 60 * 1000;
// How soon to retry a panel that just failed, while still serving its
// last-known rates in the meantime.
const PANEL_STALE_RETRY_MS = 30 * 1000;
const panelCatalogueCache = new Map();   // panelId -> { rates, currency, fetchedAt }
const panelCatalogueInflight = new Map(); // panelId -> Promise

/* Force the next quote to refetch this panel. The entry is expired rather
   than deleted, so if that refetch fails we can still fall back to the last
   known-good rates instead of pricing the order at zero. */
function invalidatePanelCatalogue(panelId) {
  const expire = (entry) => { if (entry) entry.fetchedAt = 0; };
  if (panelId) {
    expire(panelCatalogueCache.get(String(panelId)));
    panelCatalogueInflight.delete(String(panelId));
  } else {
    for (const entry of panelCatalogueCache.values()) expire(entry);
    panelCatalogueInflight.clear();
  }
}

async function fetchPanelCatalogue(panel) {
  const rates = new Map();
  const mins = new Map();
  let ok = false;
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
      /* The provider's own minimum order size. Follower services commonly
         sit at 100, and every drip batch is a SEPARATE order, so a batch
         below this is rejected by the panel. Read it rather than guess. */
      const min = Number(String(row?.min ?? row?.minimum ?? '').replace(/[^\d.]/g, ''));
      if (id && Number.isFinite(min) && min > 0) mins.set(id, Math.ceil(min));
    }
    ok = rates.size > 0;
  } catch (e) {
    warn(`Quote: catalogue fetch failed for ${panel.name}:`, e?.message || e);
  }

  let currency = 'INR';
  try {
    const balanceParams = new URLSearchParams({ key: panel.apiKey, action: 'balance' });
    const balanceRes = await axios.post(panel.apiUrl, balanceParams.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: PROVIDER_HTTP_TIMEOUT_MS,
      validateStatus: () => true,
    });
    currency = extractPanelCurrency(balanceRes.data) || 'INR';
  } catch { /* default to INR */ }

  return { rates, mins, currency, ok };
}

/* ============================================================
   PANEL BALANCE
   Read the credit left in each provider account, so the admin panel can
   show it and a Telegram alert can fire before deliveries start failing
   for lack of funds.
   ============================================================ */
const PANEL_BALANCE_TTL_MS = 5 * 60 * 1000;
const panelBalanceCache = new Map();     // panelId -> { balance, currency, inr, fetchedAt, ok, error }

function extractPanelBalance(body) {
  const candidates = [
    body?.balance, body?.funds, body?.amount,
    body?.data?.balance, body?.account?.balance, body?.user?.balance,
  ];
  for (const candidate of candidates) {
    if (candidate === undefined || candidate === null) continue;
    // Providers return "12.34", "$12.34" or 12.34 — strip anything else.
    const value = Number(String(candidate).replace(/[^\d.-]/g, ''));
    if (Number.isFinite(value)) return value;
  }
  return null;
}

/** Live balance for one panel, cached briefly. `force` skips the cache. */
async function getPanelBalance(panel, { force = false } = {}) {
  const id = String(panel._id);
  const cached = panelBalanceCache.get(id);
  if (!force && cached && Date.now() - cached.fetchedAt < PANEL_BALANCE_TTL_MS) return cached;

  let entry;
  try {
    const params = new URLSearchParams({ key: panel.apiKey, action: 'balance' });
    const response = await axios.post(panel.apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: PROVIDER_HTTP_TIMEOUT_MS,
      validateStatus: () => true,
    });
    const balance = extractPanelBalance(response.data);
    const currency = extractPanelCurrency(response.data) || 'INR';
    if (balance == null) throw new Error('Panel did not return a balance');

    /* Convert to rupees so balances across panels on different currencies
       can be compared, and one threshold covers them all. */
    let inr = balance;
    if (currency !== 'INR') {
      try {
        const fx = await getInrExchangeRate(currency);
        inr = balance * fx.rate;
      } catch { inr = null; }
    }
    entry = { balance, currency, inr, fetchedAt: Date.now(), ok: true, error: '' };
  } catch (e) {
    entry = {
      balance: null, currency: '', inr: null,
      fetchedAt: Date.now(), ok: false,
      error: e?.message || 'Could not read balance',
    };
  }
  panelBalanceCache.set(id, entry);
  return entry;
}

/** Balances for every panel, fetched in parallel. */
async function getAllPanelBalances({ force = false } = {}) {
  const panels = await Panel.find().sort({ createdAt: 1 }).lean();
  const rows = await Promise.all(panels.map(async (p) => {
    const b = await getPanelBalance(p, { force });
    return {
      id: String(p._id),
      name: p.name,
      isActive: p.isActive !== false,
      balance: b.balance,
      currency: b.currency,
      balanceInr: b.inr == null ? null : Math.round(b.inr * 100) / 100,
      ok: b.ok,
      error: b.error,
      checkedAt: new Date(b.fetchedAt).toISOString(),
    };
  }));
  return rows;
}

/* ---- Low-balance alerting ----
   Fires once when a panel crosses below the threshold, then stays quiet
   until it recovers. Without that latch a low panel would ping every
   check and you'd start ignoring the alerts. */
const lowBalanceAlerted = new Set();     // panelIds currently in the "warned" state
const LOW_BALANCE_CHECK_MS = 60 * 60 * 1000;

async function checkPanelBalances({ force = false } = {}) {
  try {
    const settings = await getPaymentSettings();
    const threshold = Number(settings.lowBalanceThreshold) || 0;
    if (threshold <= 0) return { checked: 0, alerted: 0 };

    const rows = await getAllPanelBalances({ force });
    let alerted = 0;

    for (const row of rows) {
      if (!row.isActive) continue;
      // An unreadable balance is a separate problem; don't guess it's low.
      if (!row.ok || row.balanceInr == null) continue;

      const isLow = row.balanceInr < threshold;
      const alreadyWarned = lowBalanceAlerted.has(row.id);

      if (isLow && !alreadyWarned) {
        lowBalanceAlerted.add(row.id);
        alerted += 1;
        const native = row.currency && row.currency !== 'INR'
          ? ` (${row.balance} ${row.currency})`
          : '';
        log(`⚠️  Low panel balance: ${row.name} ₹${row.balanceInr}`);
        notifyTelegram(
          `🔋 <b>Low panel balance</b>\n\n` +
          `Panel: <b>${tgEscape(row.name)}</b>\n` +
          `Left: <b>₹${row.balanceInr}</b>${tgEscape(native)}\n` +
          `Alert below: ₹${threshold}\n\n` +
          `Top this panel up, or orders using it will start failing.`
        );
      } else if (!isLow && alreadyWarned) {
        // Recovered — clear the latch and say so.
        lowBalanceAlerted.delete(row.id);
        log(`✅ Panel balance recovered: ${row.name} ₹${row.balanceInr}`);
        notifyTelegram(
          `✅ <b>Panel topped up</b>\n\n` +
          `Panel: <b>${tgEscape(row.name)}</b>\n` +
          `Balance: <b>₹${row.balanceInr}</b>`
        );
      }
    }
    return { checked: rows.length, alerted };
  } catch (e) {
    err('checkPanelBalances:', e?.message || e);
    return { checked: 0, alerted: 0 };
  }
}

/** Cached rate list + account currency for one panel. */
async function getPanelCatalogue(panel) {
  const id = String(panel._id);
  const cached = panelCatalogueCache.get(id);
  if (cached && Date.now() - cached.fetchedAt < PANEL_CATALOGUE_TTL_MS) return cached;

  // Someone else is already fetching this panel — wait for their result.
  const pending = panelCatalogueInflight.get(id);
  if (pending) return pending;

  const promise = (async () => {
    try {
      const fresh = await fetchPanelCatalogue(panel);
      if (!fresh.ok && cached && cached.rates.size > 0) {
        /* The panel failed. Serving the previous rates is far safer than
           pricing at zero. Back off for a short while so a dead panel is
           not retried on every single keystroke. */
        warn(`Quote: using stale cached rates for ${panel.name}`);
        cached.fetchedAt = Date.now() - PANEL_CATALOGUE_TTL_MS + PANEL_STALE_RETRY_MS;
        return cached;
      }
      const entry = {
        rates: fresh.rates,
        mins: fresh.mins || new Map(),
        currency: fresh.currency,
        fetchedAt: Date.now(),
      };
      panelCatalogueCache.set(id, entry);
      return entry;
    } finally {
      panelCatalogueInflight.delete(id);
    }
  })();

  panelCatalogueInflight.set(id, promise);
  return promise;
}

async function computeQuote(requestedUnits, platformArg = DEFAULT_PLATFORM) {
  const platform = normalizePlatform(platformArg);
  const cfg = await getPanelConfig();
  const panelsById = await loadPanelsById();
  const settings = await getPaymentSettings();
  /* Commission is per-platform, so a YouTube order can carry a different
     margin from an Instagram one. */
  /* Markup is resolved PER METRIC, not once per order: followers carry
     their own commission, so an order mixing followers with engagement
     must price each part at its own rate. */
  const markupPctFor = label => markupFor(
    settings, platform, PROFILE_METRICS.includes(label) ? 'followers' : 'engagement'
  );
  const appliedMarkups = new Set();

  const catalogueCache = new Map();
  const currencyCache = new Map();

  /** Pull this panel's rates into the per-request maps, via the shared cache. */
  async function loadPanel(panelId) {
    if (catalogueCache.has(panelId)) return;
    const panel = panelsById.get(panelId);
    if (!panel) { catalogueCache.set(panelId, new Map()); return; }

    const entry = await getPanelCatalogue(panel);
    catalogueCache.set(panelId, entry.rates);
    currencyCache.set(panelId, entry.currency);
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
  const slotBreakdown = [];      // per service-id detail, for owner accounts
  let costInr = 0;
  let missingRate = false;

  for (const label of SERVICE_LABELS) {
    if (!metricAllowed(platform, label)) continue;
    const requested = requestedUnits?.[label];
    /* Two shapes are accepted:
         - a number  -> a total, split evenly (used by the live estimate before
           the run list exists)
         - an array of per-run quantities -> priced against the SAME rotation
           the scheduler will use, which is exact. */
    const perRun = Array.isArray(requested)
      ? requested.map(n => Math.max(0, Math.floor(Number(n) || 0))).filter(n => n > 0)
      : null;
    const units = perRun
      ? perRun.reduce((a, b) => a + b, 0)
      : Math.max(0, Math.floor(Number(requested) || 0));
    if (units <= 0) continue;

    const slots = usableSlots(cfg, label, panelsById, platform);
    if (slots.length === 0) continue;

    /* Work out how many units each slot really receives.
       addRuns() assigns run i to slot (i % slots.length), so with uneven run
       sizes the split is NOT 50/50. Mirroring that here is what makes the
       quoted cost match the invoice. */
    const unitsPerSlot = slots.map(() => 0);
    const runsPerSlot = slots.map(() => 0);
    if (perRun) {
      perRun.forEach((qty, i) => {
        const idx = i % slots.length;
        unitsPerSlot[idx] += qty;
        runsPerSlot[idx] += 1;
      });
    } else {
      // No run list yet: fall back to an even split.
      const even = units / slots.length;
      for (let i = 0; i < slots.length; i++) unitsPerSlot[i] = even;
    }

    let labelTotal = 0;
    let pricedAny = false;

    for (let i = 0; i < slots.length; i++) {
      const slot = slots[i];
      const slotUnits = unitsPerSlot[i];
      await loadPanel(slot.panelId);
      const rate = catalogueCache.get(slot.panelId)?.get(slot.serviceId);
      const panel = panelsById.get(slot.panelId);
      const currency = currencyCache.get(slot.panelId) || 'INR';

      if (!Number.isFinite(rate) || rate <= 0) {
        missingRate = true;
        slotBreakdown.push({
          platform, label, serviceId: slot.serviceId,
          panelId: slot.panelId, panelName: panel?.name || 'Unknown panel',
          units: Math.round(slotUnits), runs: runsPerSlot[i],
          rate: null, currency, costPaise: 0, priced: false,
        });
        continue;
      }

      const native = (slotUnits / 1000) * rate;
      const inr = await toInr(native, currency);
      if (inr == null) {
        missingRate = true;
        slotBreakdown.push({
          platform, label, serviceId: slot.serviceId,
          panelId: slot.panelId, panelName: panel?.name || 'Unknown panel',
          units: Math.round(slotUnits), runs: runsPerSlot[i],
          rate, currency, costPaise: 0, priced: false,
        });
        continue;
      }

      labelTotal += inr;
      pricedAny = true;
      slotBreakdown.push({
        platform, label, serviceId: slot.serviceId,
        panelId: slot.panelId, panelName: panel?.name || 'Unknown panel',
        units: Math.round(slotUnits), runs: runsPerSlot[i],
        rate, currency,
        costPaise: Math.round(inr * 100),
        priced: true,
      });
    }

    if (pricedAny) {
      const labelPct = markupPctFor(label);
      appliedMarkups.add(labelPct);
      const withMarkup = labelTotal * (1 + labelPct / 100);
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
  // The rate actually applied to THIS order, so the owner view stays honest.
  /* One number for the owner readout. Orders are single-purpose in
     practice, so this is normally the only rate used; if an order ever
     mixes rates, report the highest rather than a misleading average. */
  const markupPercent = appliedMarkups.size
    ? Math.max(...appliedMarkups)
    : markupFor(settings, platform);
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
    slotBreakdown,
  };
}

app.post('/api/quote', requireUser, async (req, res) => {
  try {
    const requested = req.body?.services && typeof req.body.services === 'object'
      ? req.body.services
      : {};
    const quote = await computeQuote(requested, req.body?.platform);

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
        /* Per service-id detail so you can audit exactly which panel is
           being charged what, and spot a mis-mapped or overpriced slot. */
        slots: (quote.slotBreakdown || []).map(sb => ({
          label: sb.label,
          serviceId: sb.serviceId,
          panelName: sb.panelName,
          units: sb.units,
          runs: sb.runs,
          rate: sb.rate,
          currency: sb.currency,
          cost: toRupees(sb.costPaise),
          priced: sb.priced,
        })),
        // True when the caller supplied real run sizes (exact, not estimated).
        exact: Object.values(req.body?.services || {}).some(v => Array.isArray(v)),
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

/** The largest per-batch minimum across a platform's slots for one metric.
    Largest, not smallest: the scheduler rotates batches across every slot,
    so a batch has to satisfy whichever provider it lands on. */
async function metricMinimum(cfg, panelsById, platform, label) {
  const slots = usableSlots(cfg, label, panelsById, platform);
  let min = 0;
  for (const slot of slots) {
    const panel = panelsById.get(slot.panelId);
    if (!panel) continue;
    try {
      const cat = await getPanelCatalogue(panel);
      const m = cat?.mins?.get(slot.serviceId);
      if (Number.isFinite(m) && m > min) min = m;
    } catch { /* a panel that will not answer must not block the page */ }
  }
  return min;
}

/** Public: minimum order size per platform for the followers service. */
app.get('/api/followers/limits', async (_req, res) => {
  try {
    const cfg = await getPanelConfig();
    const panelsById = await loadPanelsById();
    const out = {};
    for (const key of FOLLOWER_PLATFORMS) {
      const configured = usableSlots(cfg, 'followers', panelsById, key).length > 0;
      out[key] = {
        configured,
        /* Falls back to 1 when the panel does not publish a minimum, so a
           silent provider never blocks ordering. */
        minPerBatch: configured ? (await metricMinimum(cfg, panelsById, key, 'followers')) || 1 : 0,
      };
    }
    res.json({ platforms: out });
  } catch (e) {
    err('GET /api/followers/limits:', e?.message || e);
    res.status(500).json({ error: 'Could not load follower limits' });
  }
});

/** Public: which platforms exist and what each supports. */
app.get('/api/platforms', async (_req, res) => {
  try {
    const doc = await getPanelConfig();
    const panelsById = await loadPanelsById();
    res.json({
      platforms: PLATFORMS.map(key => ({
        key,
        label: PLATFORM_LABELS[key],
        metrics: PLATFORM_METRICS[key],
        configured: usableSlots(doc, 'views', panelsById, key).length > 0,
        /* Independent of `configured`: selling followers needs a followers
           service, not a views one, so the Grow Followers page can be live
           for a platform whose post campaigns are not. */
        followersConfigured: usableSlots(doc, 'followers', panelsById, key).length > 0,
      })),
      defaultPlatform: DEFAULT_PLATFORM,
      followerPlatforms: FOLLOWER_PLATFORMS,
    });
  } catch (e) {
    err('GET /api/platforms:', e?.message || e);
    res.status(500).json({ error: 'Could not load platforms' });
  }
});

/* ============================================================
   DISPLAY CURRENCY ROUTES
   ============================================================ */

/** Public: which currencies may be displayed, and at what rate. */
app.get('/api/currencies', async (_req, res) => {
  try {
    const settings = await getPaymentSettings();
    res.json({ currencies: activeCurrencies(settings) });
  } catch (e) {
    err('GET /api/currencies:', e?.message || e);
    res.status(500).json({ error: 'Could not load currencies' });
  }
});

/** Change the signed-in account's display currency. */
app.post('/api/me/currency', requireUser, async (req, res) => {
  try {
    const code = String(req.body?.currency || '').trim().toUpperCase();
    const settings = await getPaymentSettings();
    const allowed = activeCurrencies(settings).some(c => c.code === code);
    if (!allowed) {
      return res.status(400).json({ error: 'That currency is not available' });
    }
    req.user.displayCurrency = code;
    await req.user.save();
    res.json({ success: true, displayCurrency: code, currency: currencyFor(req.user, settings) });
  } catch (e) {
    err('POST /api/me/currency:', e?.message || e);
    res.status(500).json({ error: 'Could not change currency' });
  }
});

/* ============================================================
   REFERRAL ROUTES
   ============================================================ */

/* ---- The signed-in user's referral dashboard ---- */
app.get('/api/referral', requireUser, async (req, res) => {
  try {
    const settings = await getPaymentSettings();
    // Accounts created before referrals existed have no code yet.
    const code = await ensureReferralCode(req.user);

    const invited = await User.find({ referredBy: String(req.user._id) })
      .sort({ createdAt: -1 })
      .limit(50)
      .lean();

    res.json({
      enabled: settings.referralEnabled === true,
      code,
      referrerReward: toRupees(settings.referrerRewardPaise),
      refereeReward: toRupees(settings.refereeRewardPaise),
      minDeposit: toRupees(settings.referralMinDepositPaise),
      totalInvited: invited.length,
      totalRewarded: invited.filter(u => u.referralRewarded).length,
      earned: toRupees(req.user.referralEarnedPaise),
      // Emails are partly masked: the inviter doesn't need the full address.
      invites: invited.map(u => {
        const [name = '', domain = ''] = String(u.email || '').split('@');
        const shown = name.length <= 2 ? `${name}***` : `${name.slice(0, 2)}***`;
        return {
          email: `${shown}@${domain}`,
          joinedAt: u.createdAt,
          rewarded: u.referralRewarded === true,
        };
      }),
    });
  } catch (e) {
    err('GET /api/referral:', e?.message || e);
    res.status(500).json({ error: 'Could not load your referral details' });
  }
});

/* ---- Public: is this code real? ----
   Lets the signup form confirm a code before the account is created.
   Returns only a boolean and a first name — never an email. */
app.get('/api/referral/check/:code', async (req, res) => {
  try {
    const settings = await getPaymentSettings();
    if (settings.referralEnabled !== true) {
      return res.json({ valid: false, enabled: false });
    }
    const code = normalizeReferralCode(req.params.code);
    if (!code) return res.json({ valid: false, enabled: true });

    const referrer = await User.findOne({ referralCode: code }).lean();
    const valid = Boolean(referrer && referrer.isActive !== false);
    res.json({
      valid,
      enabled: true,
      invitedBy: valid ? (referrer.name || '').split(' ')[0] || '' : '',
      refereeReward: toRupees(settings.refereeRewardPaise),
      minDeposit: toRupees(settings.referralMinDepositPaise),
    });
  } catch (e) {
    err('GET /api/referral/check:', e?.message || e);
    res.status(500).json({ error: 'Could not check that code' });
  }
});

/* ============================================================
   NEW ORDER PAYWALL ROUTES
   ============================================================ */

/* ---- Where the user stands, plus what they'd have to pay ----
   The payment methods are the SAME list the wallet deposit screen uses,
   so the admin only ever configures UPI / crypto in one place. */
app.get('/api/order-access', requireUser, async (req, res) => {
  try {
    const settings = await getPaymentSettings();
    const pending = await Deposit.findOne({
      userId: String(req.user._id),
      purpose: 'access',
      status: 'pending',
    }).sort({ createdAt: -1 }).lean();

    res.json({
      ...orderAccessPayload(req.user, settings),
      balance: toRupees(req.user.balancePaise),
      payment: publicPaymentSettings(settings),
      pending: pending
        ? {
            id: String(pending._id),
            amount: toRupees(pending.amountPaise),
            crypto: pending.cryptoMicros > 0 ? formatCrypto(pending.cryptoMicros) : '',
            coin: pending.cryptoCoin || '',
            method: pending.method,
            reference: pending.reference,
            createdAt: pending.createdAt,
          }
        : null,
    });
  } catch (e) {
    err('GET /api/order-access:', e?.message || e);
    res.status(500).json({ error: 'Could not load access status' });
  }
});

/* ---- Submit a paywall payment for approval ----
   Mirrors /api/wallet/deposit, but approving it unlocks the page instead
   of crediting the wallet. */
app.post('/api/order-access/purchase', requireUser, async (req, res) => {
  try {
    const settings = await getPaymentSettings();

    if (settings.paywallEnabled !== true) {
      return res.status(400).json({ error: 'The New Order page is currently open to everyone.' });
    }
    if (userCanOrder(req.user, settings)) {
      return res.status(409).json({ error: 'This account already has access.' });
    }

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
    /* Without a crypto price there is no amount to ask for, so refuse rather
       than let someone "pay" an unstated sum. */
    if (method === 'crypto' && !paywallCryptoPrice(settings)) {
      return res.status(400).json({
        error: 'Crypto is not available for this unlock yet. Please pay by UPI or contact the administrator.',
      });
    }
    if (reference.length < 6) {
      return res.status(400).json({
        error: method === 'upi'
          ? 'Enter the 12-digit UTR / reference number from your payment app'
          : 'Enter the transaction hash',
      });
    }

    // One open request at a time, so the queue can't be flooded.
    const open = await Deposit.findOne({
      userId: String(req.user._id), purpose: 'access', status: 'pending',
    });
    if (open) {
      return res.status(409).json({
        error: 'You already have an unlock request waiting for approval.',
      });
    }

    // Same UTR re-use guard as wallet deposits.
    const duplicate = await Deposit.findOne({ reference });
    if (duplicate) {
      return res.status(409).json({ error: 'This reference number has already been submitted' });
    }

    // Both prices are taken from settings, never from the request body.
    const chosenCoin = method === 'crypto'
      ? ((settings.cryptoMethods || []).find(m => m.id === methodId)?.coin || 'USDT')
      : '';

    const deposit = await Deposit.create({
      userId: String(req.user._id),
      userEmail: req.user.email,
      amountPaise: settings.paywallPricePaise,
      purpose: 'access',
      method, methodId, reference,
      cryptoMicros: method === 'crypto' ? toMicros(paywallCryptoPrice(settings)) : 0,
      cryptoCoin: chosenCoin,
    });

    const shown = method === 'crypto'
      ? `${formatCrypto(deposit.cryptoMicros)} ${chosenCoin}`
      : `₹${toRupees(deposit.amountPaise)}`;
    log(`🔓 Unlock request ${shown} from ${req.user.email} (${reference})`);
    // Not awaited: the user's response must not wait on Telegram.
    notifyTelegram(
      `🔓 <b>New Order unlock request</b>\n\n` +
      `Amount: <b>${tgEscape(shown)}</b>\n` +
      `From: ${tgEscape(req.user.email)}\n` +
      `Method: ${tgEscape(method.toUpperCase())}\n` +
      `${method === 'upi' ? 'UTR' : 'TX'}: <code>${tgEscape(reference)}</code>\n\n` +
      `Check the money arrived, then approve in the admin panel.`
    );
    res.status(201).json({
      success: true,
      request: {
        id: String(deposit._id),
        amount: toRupees(deposit.amountPaise),
        crypto: method === 'crypto' ? formatCrypto(deposit.cryptoMicros) : '',
        coin: chosenCoin,
        status: deposit.status,
        createdAt: deposit.createdAt,
      },
      message: 'Submitted. Your access is unlocked once the payment is verified.',
    });
  } catch (e) {
    if (e?.code === 11000) {
      return res.status(409).json({ error: 'This reference number has already been submitted' });
    }
    err('POST /api/order-access/purchase:', e?.message || e);
    res.status(500).json({ error: 'Could not submit unlock request' });
  }
});

/* ---- Unlock instantly using existing wallet balance ----
   No admin approval needed: the money is already verified. */
app.post('/api/order-access/pay-from-wallet', requireUser, async (req, res) => {
  try {
    const settings = await getPaymentSettings();

    if (settings.paywallEnabled !== true) {
      return res.status(400).json({ error: 'The New Order page is currently open to everyone.' });
    }
    if (userCanOrder(req.user, settings)) {
      return res.status(409).json({ error: 'This account already has access.' });
    }

    const price = Math.round(Number(settings.paywallPricePaise) || 0);
    if (price <= 0) {
      // A free paywall is just an unlock.
      const user = await grantOrderAccess(req.user._id, 'admin');
      return res.json({ success: true, balance: toRupees(user.balancePaise), unlocked: true });
    }

    const debit = await debitWallet(req.user._id, price, {
      type: 'order_debit',
      note: 'New Order page — lifetime unlock',
      reference: 'order-access',
    });
    if (!debit.ok) {
      return res.status(402).json({
        error: 'Wallet balance is too low',
        required: toRupees(price),
        balance: toRupees(req.user.balancePaise),
        shortfall: toRupees(price - req.user.balancePaise),
      });
    }

    const user = await grantOrderAccess(req.user._id, 'payment');
    log(`🔓 Access unlocked from wallet: ${req.user.email} (₹${toRupees(price)})`);
    res.json({ success: true, unlocked: true, balance: toRupees(user.balancePaise) });
  } catch (e) {
    err('POST /api/order-access/pay-from-wallet:', e?.message || e);
    res.status(500).json({ error: 'Could not unlock access' });
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
        crypto: d.cryptoMicros > 0 ? formatCrypto(d.cryptoMicros) : '',
        coin: d.cryptoCoin || '',
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
    const method = String(req.body?.method || '').toLowerCase();
    const reference = String(req.body?.reference || '').trim();
    const methodId = String(req.body?.methodId || '').trim();
    const packId = String(req.body?.packId || '').trim();

    if (method !== 'upi' && method !== 'crypto') {
      return res.status(400).json({ error: 'Choose a valid payment method' });
    }
    if (method === 'upi' && !settings.upiEnabled) {
      return res.status(400).json({ error: 'UPI payments are currently disabled' });
    }
    if (method === 'crypto' && !settings.cryptoEnabled) {
      return res.status(400).json({ error: 'Crypto payments are currently disabled' });
    }

    /* ---- How much is this worth? ----
       UPI: the user types a rupee amount, because they can send any amount.
       Crypto: there is no exchange rate, so they must pick one of the admin's
       fixed packs and BOTH figures come from that pack, never from the body. */
    let amountPaise;
    let cryptoMicros = 0;
    let cryptoCoin = '';

    if (method === 'crypto') {
      const wallet = (settings.cryptoMethods || []).find(m => m.id === methodId);
      cryptoCoin = wallet?.coin || 'USDT';
      const rate = Number(wallet?.inrPerUnit) || 0;

      if (rate > 0) {
        /* A rate is configured, so the user may send any amount they like.
           We convert THEIR figure to rupees here — the client never decides
           what the wallet is credited. */
        cryptoMicros = toMicros(req.body?.cryptoAmount);
        if (!Number.isFinite(cryptoMicros) || cryptoMicros <= 0) {
          return res.status(400).json({ error: `Enter how much ${cryptoCoin} you sent` });
        }
        amountPaise = Math.round(fromMicros(cryptoMicros) * rate * 100);
        if (amountPaise < settings.minDepositPaise) {
          const minUnits = formatCrypto(Math.ceil((settings.minDepositPaise / 100 / rate) * CRYPTO_SCALE));
          return res.status(400).json({
            error: `Minimum deposit is ₹${toRupees(settings.minDepositPaise)} (about ${minUnits} ${cryptoCoin})`,
          });
        }
      } else {
        // No rate set: fall back to the admin's fixed packs.
        const packs = (settings.cryptoPacks || []).filter(
          p => p.isActive && p.amountPaise > 0 && p.cryptoMicros > 0
        );
        if (packs.length === 0) {
          return res.status(400).json({
            error: 'No crypto top-up amounts are set up yet. Please use UPI or contact the administrator.',
          });
        }
        const pack = packs.find(p => p.id === packId);
        if (!pack) {
          return res.status(400).json({ error: 'Choose one of the available crypto amounts' });
        }
        amountPaise = pack.amountPaise;
        cryptoMicros = pack.cryptoMicros;
      }
    } else {
      amountPaise = toPaise(req.body?.amount);
      if (!Number.isFinite(amountPaise) || amountPaise < settings.minDepositPaise) {
        return res.status(400).json({
          error: `Minimum deposit is ₹${toRupees(settings.minDepositPaise)}`,
        });
      }
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
      cryptoMicros,
      cryptoCoin,
    });

    const shown = method === 'crypto'
      ? `${formatCrypto(cryptoMicros)} ${cryptoCoin} (₹${toRupees(amountPaise)})`
      : `₹${toRupees(amountPaise)}`;
    log(`💰 Deposit request ${shown} from ${req.user.email} (${reference})`);
    notifyTelegram(
      `💰 <b>New wallet deposit</b>\n\n` +
      `Amount: <b>${tgEscape(shown)}</b>\n` +
      `From: ${tgEscape(req.user.email)}\n` +
      `Method: ${tgEscape(method.toUpperCase())}\n` +
      `${method === 'upi' ? 'UTR' : 'TX'}: <code>${tgEscape(reference)}</code>\n\n` +
      `Check the money arrived, then approve in the admin panel.`
    );
    res.status(201).json({
      success: true,
      deposit: {
        id: String(deposit._id),
        amount: toRupees(deposit.amountPaise),
        crypto: method === 'crypto' ? formatCrypto(cryptoMicros) : '',
        coin: cryptoCoin,
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
    // 'wallet' | 'access' | 'all' — lets the admin UI show two queues.
    const purpose = String(req.query?.purpose || 'all');
    if (purpose === 'wallet') {
      // Rows written before the paywall existed have no `purpose` field.
      filter.$or = [{ purpose: 'wallet' }, { purpose: { $exists: false } }];
    } else if (purpose === 'access') {
      filter.purpose = 'access';
    }

    const deposits = await Deposit.find(filter).sort({ createdAt: -1 }).limit(200).lean();
    const pendingCount = await Deposit.countDocuments({ status: 'pending' });
    const pendingAccessCount = await Deposit.countDocuments({ status: 'pending', purpose: 'access' });

    res.json({
      pendingCount,
      pendingAccessCount,
      deposits: deposits.map(d => ({
        id: String(d._id),
        userId: d.userId,
        userEmail: d.userEmail,
        amount: toRupees(d.amountPaise),
        crypto: d.cryptoMicros > 0 ? formatCrypto(d.cryptoMicros) : '',
        coin: d.cryptoCoin || '',
        purpose: d.purpose || 'wallet',
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

    const purpose = deposit.purpose || 'wallet';
    let balance = null;
    let unlocked = false;
    let referral = null;

    if (action === 'approve' && purpose === 'access') {
      // Paywall payment: unlock the page instead of crediting the wallet.
      await grantOrderAccess(deposit.userId, 'payment');
      unlocked = true;
      log(`🔓 Access approved: ₹${toRupees(deposit.amountPaise)} → ${deposit.userEmail}`);
    } else if (action === 'approve') {
      balance = await creditWallet(deposit.userId, deposit.amountPaise, {
        type: 'deposit',
        note: `${deposit.method.toUpperCase()} deposit — ${deposit.reference}`,
        reference: String(deposit._id),
      });
      log(`✅ Deposit approved: ₹${toRupees(deposit.amountPaise)} → ${deposit.userEmail}`);
      // A first approved deposit is what releases any referral reward.
      referral = await maybePayReferral(deposit.userId, deposit.amountPaise);
      if (referral) balance = (await User.findById(deposit.userId).lean())?.balancePaise ?? balance;
    } else {
      log(`❌ ${purpose === 'access' ? 'Unlock' : 'Deposit'} rejected: ${deposit.reference} (${deposit.userEmail})`);
    }

    res.json({
      success: true,
      status: deposit.status,
      purpose,
      unlocked,
      newBalance: balance == null ? null : toRupees(balance),
      referralPaid: referral
        ? {
            referrer: toRupees(referral.referrerReward),
            referee: toRupees(referral.refereeReward),
          }
        : null,
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
      /* Effective commission per platform, already resolved: a platform with
         no override reports the global number, so the UI can show what will
         really be charged without repeating the fallback logic. */
      platformMarkup: Object.fromEntries(
        PLATFORMS.map(p => [p, markupFor(doc, p)])
      ),
      /* Which platforms have an explicit override, so the UI can show
         "using global" instead of a number the admin never typed. */
      platformMarkupSet: Object.fromEntries(
        PLATFORMS.map(p => {
          const raw = doc.platformMarkup?.[p];
          return [p, raw !== null && raw !== undefined && raw !== '' && Number.isFinite(Number(raw))];
        })
      ),
      /* Follower-growth commission, resolved the same way: a platform with
         no follower override reports whatever followers would actually be
         charged at (its platform rate, or the global one). */
      followerMarkup: Object.fromEntries(
        FOLLOWER_PLATFORMS.map(p => [p, markupFor(doc, p, 'followers')])
      ),
      followerMarkupSet: Object.fromEntries(
        FOLLOWER_PLATFORMS.map(p => [p, readPercent(doc.followerMarkup?.[p]) !== null])
      ),
      followerPlatforms: FOLLOWER_PLATFORMS,
      upiEnabled: doc.upiEnabled,
      cryptoEnabled: doc.cryptoEnabled,
      upiMethods: doc.upiMethods || [],
      cryptoMethods: (doc.cryptoMethods || []).map(m => ({
        id: m.id, label: m.label, network: m.network, address: m.address,
        instructions: m.instructions, qrImage: m.qrImage || '',
        isActive: m.isActive !== false, coin: m.coin || 'USDT',
        inrPerUnit: Number(m.inrPerUnit) || 0,
      })),
      cryptoPacks: (doc.cryptoPacks || []).map(p => ({
        id: p.id,
        amount: toRupees(p.amountPaise),
        crypto: formatCrypto(p.cryptoMicros),
        isActive: p.isActive !== false,
      })),
      currencies: (doc.currencies || []).map(c => ({
        code: c.code, symbol: c.symbol || '',
        inrPerUnit: Number(c.inrPerUnit), isActive: c.isActive !== false,
      })),
      lowBalanceThreshold: Number(doc.lowBalanceThreshold) || 0,
      hideRunProblems: Boolean(doc.hideRunProblems),
      botScoreForUsers: Boolean(doc.botScoreForUsers),
      pendingGraceMinutes: Number(doc.pendingGraceMinutes) || 15,
      referralEnabled: Boolean(doc.referralEnabled),
      referrerReward: toRupees(doc.referrerRewardPaise),
      refereeReward: toRupees(doc.refereeRewardPaise),
      referralMinDeposit: toRupees(doc.referralMinDepositPaise),
      paywallEnabled: Boolean(doc.paywallEnabled),
      paywallPrice: toRupees(doc.paywallPricePaise),
      paywallCryptoPrice: doc.paywallCryptoMicros > 0 ? formatCrypto(doc.paywallCryptoMicros) : '',
      paywallTitle: doc.paywallTitle || 'Unlock New Order',
      paywallBlurb: doc.paywallBlurb || '',
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

    /* ---- Per-platform commission ----
       Send null (or '') for a platform to clear its override and go back to
       the global rate. Unknown keys are ignored rather than stored. */
    if (body.platformMarkup && typeof body.platformMarkup === 'object') {
      const next = { ...(doc.platformMarkup || {}) };
      for (const key of Object.keys(body.platformMarkup)) {
        const platform = String(key).toLowerCase();
        if (!PLATFORMS.includes(platform)) continue;
        const raw = body.platformMarkup[key];
        if (raw === null || raw === undefined || raw === '') {
          delete next[platform];
          continue;
        }
        const pct = Number(raw);
        if (!Number.isFinite(pct) || pct < 0 || pct > 1000) {
          return res.status(400).json({
            error: `${PLATFORM_LABELS[platform]} commission must be between 0 and 1000%`,
          });
        }
        next[platform] = pct;
      }
      doc.platformMarkup = next;
      doc.markModified('platformMarkup');
    }

    /* ---- Follower-growth commission ----
       Same contract as platformMarkup: null clears the override so
       followers fall back to the platform (then global) rate. */
    if (body.followerMarkup && typeof body.followerMarkup === 'object') {
      const next = { ...(doc.followerMarkup || {}) };
      for (const key of Object.keys(body.followerMarkup)) {
        const platform = String(key).toLowerCase();
        if (!FOLLOWER_PLATFORMS.includes(platform)) continue;
        const raw = body.followerMarkup[key];
        if (raw === null || raw === undefined || raw === '') {
          delete next[platform];
          continue;
        }
        const pct = Number(raw);
        if (!Number.isFinite(pct) || pct < 0 || pct > 1000) {
          return res.status(400).json({
            error: `${PLATFORM_LABELS[platform]} follower commission must be between 0 and 1000%`,
          });
        }
        next[platform] = pct;
      }
      doc.followerMarkup = next;
      doc.markModified('followerMarkup');
    }
    if (typeof body.upiEnabled === 'boolean') doc.upiEnabled = body.upiEnabled;
    if (typeof body.cryptoEnabled === 'boolean') doc.cryptoEnabled = body.cryptoEnabled;

    /* ---- Display currency rates ----
       `inrPerUnit` is how many rupees one unit of the currency is worth.
       Rows without a positive rate are dropped rather than saved broken. */
    if (Array.isArray(body.currencies)) {
      const seen = new Set();
      doc.currencies = body.currencies
        .slice(0, 12)
        .map(c => ({
          code: String(c?.code || '').trim().toUpperCase().slice(0, 5),
          symbol: String(c?.symbol || '').trim().slice(0, 4),
          inrPerUnit: Number(c?.inrPerUnit),
          isActive: c?.isActive !== false,
        }))
        .filter(c => {
          if (!c.code || c.code === 'INR') return false;   // INR is implicit
          if (!Number.isFinite(c.inrPerUnit) || c.inrPerUnit <= 0) return false;
          if (seen.has(c.code)) return false;              // no duplicates
          seen.add(c.code);
          return true;
        });
    }

    /* ---- Low-balance alert threshold ---- */
    if (body.lowBalanceThreshold !== undefined) {
      const value = Number(body.lowBalanceThreshold);
      if (!Number.isFinite(value) || value < 0) {
        return res.status(400).json({ error: 'Low-balance threshold cannot be negative' });
      }
      doc.lowBalanceThreshold = value;
    }

    /* ---- Orders display mask ---- */
    if (typeof body.hideRunProblems === 'boolean') doc.hideRunProblems = body.hideRunProblems;
    if (typeof body.botScoreForUsers === 'boolean') doc.botScoreForUsers = body.botScoreForUsers;
    if (body.pendingGraceMinutes !== undefined) {
      const mins = Number(body.pendingGraceMinutes);
      if (!Number.isFinite(mins) || mins < 0 || mins > 1440) {
        return res.status(400).json({ error: 'Grace period must be between 0 and 1440 minutes' });
      }
      doc.pendingGraceMinutes = Math.round(mins);
    }

    /* ---- Referral programme ---- */
    if (typeof body.referralEnabled === 'boolean') doc.referralEnabled = body.referralEnabled;
    for (const [key, field] of [
      ['referrerReward', 'referrerRewardPaise'],
      ['refereeReward', 'refereeRewardPaise'],
      ['referralMinDeposit', 'referralMinDepositPaise'],
    ]) {
      if (body[key] === undefined) continue;
      const paise = toPaise(body[key]);
      if (!Number.isFinite(paise) || paise < 0) {
        return res.status(400).json({ error: 'Referral amounts cannot be negative' });
      }
      doc[field] = paise;
    }

    /* ---- Paywall switch ---- */
    if (typeof body.paywallEnabled === 'boolean') doc.paywallEnabled = body.paywallEnabled;
    if (body.paywallPrice !== undefined) {
      const paise = toPaise(body.paywallPrice);
      if (!Number.isFinite(paise) || paise < 0) {
        return res.status(400).json({ error: 'Unlock price cannot be negative' });
      }
      doc.paywallPricePaise = paise;
    }
    if (body.paywallTitle !== undefined) {
      doc.paywallTitle = String(body.paywallTitle).trim().slice(0, 80) || 'Unlock New Order';
    }
    if (body.paywallBlurb !== undefined) {
      doc.paywallBlurb = String(body.paywallBlurb).trim().slice(0, 400);
    }

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
          coin: (String(m?.coin || 'USDT').trim().toUpperCase().slice(0, 10)) || 'USDT',
          inrPerUnit: Math.max(0, Number(m?.inrPerUnit) || 0),
        };
      });
    }

    /* ---- Crypto top-up packs ----
       Each row pairs a rupee credit with the exact crypto amount to send.
       Rows missing either half are dropped rather than saved half-priced. */
    if (Array.isArray(body.cryptoPacks)) {
      doc.cryptoPacks = body.cryptoPacks
        .slice(0, 12)
        .map((p, i) => ({
          id: String(p?.id || `pack-${Date.now()}-${i}`),
          amountPaise: toPaise(p?.amount),
          cryptoMicros: toMicros(p?.crypto),
          isActive: p?.isActive !== false,
        }))
        .filter(p => p.amountPaise > 0 && p.cryptoMicros > 0);
    }

    /* ---- Paywall price in crypto (blank / 0 = don't offer crypto) ---- */
    if (body.paywallCryptoPrice !== undefined) {
      const raw = String(body.paywallCryptoPrice).trim();
      if (raw === '') {
        doc.paywallCryptoMicros = 0;
      } else {
        const micros = toMicros(raw);
        if (!Number.isFinite(micros) || micros < 0) {
          return res.status(400).json({ error: 'Crypto unlock price cannot be negative' });
        }
        doc.paywallCryptoMicros = micros;
      }
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

app.get('/api/panel-config', async (req, res) => {
  try {
    const doc = await getPanelConfig();
    // Owner accounts may audit which provider serves each slot.
    const user = await resolveUser(req).catch(() => null);
    res.json(await publicPanelConfig(doc, { includeProviders: user?.isOwner === true }));
  } catch (e) {
    err('GET /api/panel-config:', e?.message || e);
    res.status(500).json({ error: 'Could not load configuration' });
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
    // Credentials or URL may have changed: drop the cached rate list.
    invalidatePanelCatalogue(panel._id);
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
    invalidatePanelCatalogue(panel._id);

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
    /* Which platform is being edited. Older clients send no platform and
       mean Instagram, which keeps them working unchanged. */
    const platform = normalizePlatform(req.body?.platform);

    const clean = (rows, label) => {
      const cleaned = [];
      for (const row of (Array.isArray(rows) ? rows : []).slice(0, 10)) {
        const panelId = String(row?.panelId || '').trim();
        const serviceId = String(row?.serviceId || '').trim();
        if (!panelId || !serviceId) continue;
        if (!panelsById.has(panelId)) throw new Error(`Unknown panel for ${label}`);
        cleaned.push({ panelId, serviceId });
      }
      return cleaned;
    };

    // Start from what's stored so editing one platform can't wipe another.
    const next = (doc.platformSlots && typeof doc.platformSlots === 'object')
      ? JSON.parse(JSON.stringify(doc.platformSlots))
      : {};
    for (const pf of PLATFORMS) {
      if (!next[pf]) next[pf] = {};
      for (const m of PLATFORM_METRICS[pf]) if (!next[pf][m]) next[pf][m] = [];
    }

    for (const label of PLATFORM_METRICS[platform]) {
      if (!(label in incoming)) continue;
      const cleaned = clean(incoming[label], label);
      next[platform][label] = cleaned;
      // Mirror Instagram into the legacy field so a rollback still works.
      if (platform === DEFAULT_PLATFORM && doc.serviceSlots) {
        doc.serviceSlots[label] = cleaned;
      }
    }

    doc.platformSlots = next;
    doc.markModified('platformSlots');
    doc.migratedToPlatforms = true;
    doc.migratedToSlots = true;
    doc.updatedAt = new Date();
    await doc.save();
    log(`⚙️  Service slots updated (${platform})`);
    res.json({ success: true, ...(await adminPanelConfig(doc)) });
  } catch (e) {
    if (/Unknown panel/.test(e?.message || '')) {
      return res.status(400).json({ error: e.message });
    }
    err('POST /api/admin/service-slots:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not save service slots' });
  }
});

// ---- Admin: list registered users ----
/* ---- Admin: live balances of every connected panel ----
   `?refresh=1` bypasses the 5-minute cache. */
/* ---- Admin: profit report ----
   Revenue is what customers actually paid; cost is what the panels charge
   us. Both come from figures recorded AT PURCHASE TIME, so re-running this
   next month gives the same answer even if provider rates have moved.

   Refunds are handled proportionally: cancelling half an order's runs
   refunds half the money, so half the panel cost is deducted too —
   otherwise a cancelled order would look like a loss.

   `?days=N` limits the window; omit it for all time. */
app.get('/api/admin/profit', requireAdmin, async (req, res) => {
  try {
    const days = Math.max(0, Math.min(3650, Number(req.query.days) || 0));
    const since = days > 0 ? new Date(Date.now() - days * 86400_000) : null;
    const match = since ? { createdAt: { $gte: since } } : {};

    const orders = await Order.find(match, {
      chargedPaise: 1, refundedPaise: 1, panelCostPaise: 1,
      platform: 1, status: 1, createdAt: 1, userId: 1,
    }).lean();

    let revenue = 0, cost = 0, refunded = 0, unpriced = 0;
    const byPlatform = {};
    const byDay = new Map();

    for (const o of orders) {
      const charged = Number(o.chargedPaise) || 0;
      const refund = Number(o.refundedPaise) || 0;
      const fullCost = Number(o.panelCostPaise) || 0;

      /* Net of refunds. The same fraction of the cost is written off,
         because a refunded run is never sent to the provider. */
      const kept = Math.max(0, charged - refund);
      const costShare = charged > 0
        ? Math.round(fullCost * (kept / charged))
        : 0;

      revenue += kept;
      cost += costShare;
      refunded += refund;
      /* Orders placed before panelCostPaise existed. Counted separately so
         the margin is not silently overstated. */
      if (fullCost === 0 && kept > 0) unpriced += kept;

      const pf = normalizePlatform(o.platform);
      if (!byPlatform[pf]) byPlatform[pf] = { revenue: 0, cost: 0, orders: 0 };
      byPlatform[pf].revenue += kept;
      byPlatform[pf].cost += costShare;
      byPlatform[pf].orders += 1;

      const key = new Date(o.createdAt).toISOString().slice(0, 10);
      const row = byDay.get(key) || { revenue: 0, cost: 0, orders: 0 };
      row.revenue += kept; row.cost += costShare; row.orders += 1;
      byDay.set(key, row);
    }

    /* Money actually taken from customers, which is a different question
       from order revenue: deposits sit in wallets until they are spent. */
    const depAgg = await Transaction.aggregate([
      { $match: { type: 'deposit', ...(since ? { createdAt: { $gte: since } } : {}) } },
      { $group: { _id: null, total: { $sum: '$amountPaise' } } },
    ]);
    const deposited = Number(depAgg?.[0]?.total) || 0;

    /* Credit still sitting in customer wallets — a liability, not profit. */
    const balAgg = await User.aggregate([
      { $group: { _id: null, total: { $sum: '$balancePaise' } } },
    ]);
    const walletLiability = Number(balAgg?.[0]?.total) || 0;

    const profit = revenue - cost;
    res.json({
      windowDays: days || null,
      orders: orders.length,
      revenue: toRupees(revenue),
      cost: toRupees(cost),
      profit: toRupees(profit),
      margin: revenue > 0 ? +((profit / revenue) * 100).toFixed(1) : 0,
      refunded: toRupees(refunded),
      /* Revenue from orders with no recorded cost. Their margin is unknown,
         so profit above is understated by whatever they really cost. */
      unpricedRevenue: toRupees(unpriced),
      deposited: toRupees(deposited),
      walletLiability: toRupees(walletLiability),
      byPlatform: Object.fromEntries(
        Object.entries(byPlatform).map(([k, v]) => [k, {
          revenue: toRupees(v.revenue),
          cost: toRupees(v.cost),
          profit: toRupees(v.revenue - v.cost),
          orders: v.orders,
        }])
      ),
      daily: [...byDay.entries()]
        .sort((a, b) => (a[0] < b[0] ? -1 : 1))
        .slice(-60)
        .map(([date, v]) => ({
          date,
          revenue: toRupees(v.revenue),
          cost: toRupees(v.cost),
          profit: toRupees(v.revenue - v.cost),
          orders: v.orders,
        })),
    });
  } catch (e) {
    err('GET /api/admin/profit:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not build profit report' });
  }
});

/* ============================================================
   FAILURE DIAGNOSIS

   Providers return terse, inconsistent strings ("Not enough funds",
   "incorrect service id", "neworder.link invalid"). On their own they tell
   the admin nothing actionable. This maps the common shapes onto a cause
   and the one thing that actually fixes it.

   Ordered most specific first: "insufficient funds" must not be caught by
   the generic "invalid" rule. Anything unmatched falls through to a
   deliberately honest "unrecognised" bucket rather than a wrong guess.
   ============================================================ */
const FAILURE_RULES = [
  {
    key: 'panel_funds',
    test: /(not enough|insufficient|no) (funds|balance)|balance is too low|low balance/i,
    title: 'Your panel account is out of money',
    cause: 'The provider rejected the order because your balance with them is too low.',
    fix: 'Top up that panel, then use Retry. Set a low-balance alert on the Panels tab so this warns you first.',
    severity: 'critical',
    scope: 'panel',
  },
  {
    key: 'bad_service_id',
    test: /(incorrect|invalid|unknown|wrong|no such|not found).{0,20}service|service.{0,20}(not found|does not exist|invalid|incorrect)/i,
    title: 'Wrong service ID',
    cause: 'This service ID does not exist on that panel — usually a typo, or the provider retired the service.',
    fix: 'Admin → Services → find this metric → Browse and pick the service again.',
    severity: 'critical',
    scope: 'service',
  },
  {
    key: 'bad_link',
    test: /(invalid|incorrect|wrong|bad).{0,20}(link|url)|link.{0,20}(invalid|incorrect|not valid)|page not found|post not found/i,
    title: 'The provider rejected the link',
    cause: 'The post or profile URL was not accepted — often a private account, a deleted post, or the wrong link type for this service.',
    fix: 'Check the link opens publicly in a browser. Followers need a PROFILE link; views need a POST link.',
    severity: 'warning',
    scope: 'order',
  },
  {
    key: 'quantity',
    test: /(min|minimum|max|maximum).{0,20}(quantity|order|amount)|quantity.{0,20}(too|min|max|invalid)|out of range/i,
    title: 'Quantity outside the provider limits',
    cause: 'The batch size is below the service minimum or above its maximum.',
    fix: 'Check the min/max on the service (Admin → Services → Browse) and re-order within that range.',
    severity: 'warning',
    scope: 'service',
  },
  {
    key: 'auth',
    test: /(invalid|incorrect|wrong|bad).{0,20}(api )?key|unauthor|forbidden|authentication|403/i,
    title: 'The panel rejected your API key',
    cause: 'The stored key is wrong, expired, or the provider disabled it.',
    fix: 'Admin → Panels → edit that panel and paste a fresh API key from the provider dashboard.',
    severity: 'critical',
    scope: 'panel',
  },
  {
    key: 'duplicate',
    test: /duplicate|already (exists|ordered|in progress)|same order/i,
    title: 'The provider saw this as a duplicate',
    cause: 'They block repeat orders for the same link within a short window.',
    fix: 'Usually harmless — space the runs further apart, or wait and retry.',
    severity: 'warning',
    scope: 'order',
  },
  {
    key: 'unreachable',
    test: /ECONNREFUSED|ENOTFOUND|ETIMEDOUT|EAI_AGAIN|socket hang up|network|timeout|getaddrinfo/i,
    title: "Couldn't reach the panel",
    cause: 'The provider did not respond — their server was down, slow, or the API URL is wrong.',
    fix: 'Check the API URL on the Panels tab. If it is right, the provider is down — retry later.',
    severity: 'critical',
    scope: 'panel',
  },
  {
    key: 'rate_limit',
    test: /rate.?limit|too many requests|429|slow down|try again later|busy/i,
    title: 'The provider throttled you',
    cause: 'Too many orders were sent too quickly.',
    fix: 'Usually self-healing — the system retries automatically. Widen delivery windows if it keeps happening.',
    severity: 'info',
    scope: 'panel',
  },
  {
    key: 'server_error',
    test: /HTTP 5\d\d|internal server error|bad gateway|service unavailable/i,
    title: 'The panel returned a server error',
    cause: 'Something broke on the provider side, not yours.',
    fix: 'Nothing to fix here. Retry the run; if it persists, contact the provider.',
    severity: 'warning',
    scope: 'panel',
  },
];

/** Turn a raw provider error into a cause and a fix. */
function diagnoseFailure(raw) {
  const text = String(raw || '').trim();
  if (!text) {
    return {
      key: 'unknown', title: 'Failed with no message',
      cause: 'The provider gave no reason.',
      fix: 'Retry the run. If it fails again, check the service ID and your panel balance.',
      severity: 'warning', scope: 'order',
    };
  }
  for (const rule of FAILURE_RULES) {
    if (rule.test.test(text)) {
      const { test, ...rest } = rule;
      return rest;
    }
  }
  /* No guessing: an unmatched message is reported verbatim so the admin can
     read the provider's own words rather than a misleading translation. */
  return {
    key: 'unrecognised',
    title: 'Unrecognised provider error',
    cause: `The provider said: "${text.slice(0, 200)}"`,
    fix: 'Search that message in your provider dashboard or ask their support. Check the service ID and panel balance first.',
    severity: 'warning', scope: 'order',
  };
}

/* ---- Admin: what is failing, why, and what to do about it ---- */
app.get('/api/admin/failures', requireAdmin, async (req, res) => {
  try {
    const days = Math.max(1, Math.min(90, Number(req.query.days) || 7));
    const since = new Date(Date.now() - days * 86400_000);

    const failed = await Run.find(
      { status: 'failed', $or: [{ executedAt: { $gte: since } }, { time: { $gte: since } }] },
      { id: 1, label: 1, service: 1, error: 1, link: 1, platform: 1,
        schedulerOrderId: 1, executedAt: 1, time: 1, attempts: 1, apiUrl: 1 }
    ).sort({ executedAt: -1, time: -1 }).limit(2000).lean();

    const panels = await Panel.find().lean();
    const byUrl = new Map(panels.map(p => [String(p.apiUrl), p]));

    /* Group by (cause + service), because that is the unit the admin fixes:
       "service 1234 on Panel A keeps failing for X". One row per real
       problem beats 400 identical rows. */
    const groups = new Map();
    for (const r of failed) {
      const d = diagnoseFailure(r.error);
      const panel = byUrl.get(String(r.apiUrl));
      const key = `${d.key}|${r.service}|${panel?.name || r.apiUrl}`;
      const g = groups.get(key) || {
        key, diagnosis: d,
        serviceId: String(r.service || ''),
        panelName: panel?.name || 'Unknown panel',
        panelId: panel ? String(panel._id) : '',
        label: String(r.label || '').toLowerCase(),
        platform: normalizePlatform(r.platform),
        count: 0, orders: new Set(), sampleError: r.error || '',
        firstSeen: null, lastSeen: null, runIds: [],
      };
      g.count += 1;
      if (r.schedulerOrderId) g.orders.add(r.schedulerOrderId);
      if (g.runIds.length < 20) g.runIds.push(r.id);
      const when = r.executedAt || r.time;
      if (when) {
        const t = new Date(when);
        if (!g.firstSeen || t < g.firstSeen) g.firstSeen = t;
        if (!g.lastSeen || t > g.lastSeen) g.lastSeen = t;
      }
      groups.set(key, g);
    }

    const SEV = { critical: 0, warning: 1, info: 2 };
    const issues = [...groups.values()]
      .map(g => ({
        key: g.key,
        title: g.diagnosis.title,
        cause: g.diagnosis.cause,
        fix: g.diagnosis.fix,
        severity: g.diagnosis.severity,
        scope: g.diagnosis.scope,
        serviceId: g.serviceId,
        panelName: g.panelName,
        panelId: g.panelId,
        metric: g.label,
        platform: g.platform,
        failedRuns: g.count,
        affectedOrders: g.orders.size,
        sampleError: String(g.sampleError || '').slice(0, 300),
        firstSeen: g.firstSeen,
        lastSeen: g.lastSeen,
        runIds: g.runIds,
      }))
      .sort((a, b) =>
        (SEV[a.severity] - SEV[b.severity]) || (b.failedRuns - a.failedRuns));

    /* Health context so a spike is readable as a proportion, not a count. */
    const totalRecent = await Run.countDocuments({
      $or: [{ executedAt: { $gte: since } }, { time: { $gte: since } }],
    });
    const stuck = await Run.countDocuments({
      status: 'pending', time: { $lt: new Date(Date.now() - 30 * 60_000) },
    });

    res.json({
      windowDays: days,
      totalFailed: failed.length,
      totalRuns: totalRecent,
      failureRate: totalRecent > 0 ? +((failed.length / totalRecent) * 100).toFixed(1) : 0,
      stuckRuns: stuck,
      issues,
    });
  } catch (e) {
    err('GET /api/admin/failures:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not build the failure report' });
  }
});

/* ---- Admin: retry the runs behind one issue ---- */
app.post('/api/admin/failures/retry', requireAdmin, async (req, res) => {
  try {
    const ids = Array.isArray(req.body?.runIds) ? req.body.runIds.slice(0, 500) : [];
    if (ids.length === 0) return res.status(400).json({ error: 'No runs specified' });
    /* Re-queue a minute out so the scheduler picks them up on its next pass
       rather than all at once. attempts is reset so the retry budget starts
       fresh — the admin has presumably fixed the underlying cause. */
    const result = await Run.updateMany(
      { id: { $in: ids }, status: 'failed' },
      { $set: { status: 'pending', time: new Date(Date.now() + 60_000),
                error: null, attempts: 0, processingStartedAt: null } }
    );
    log(`🔁 Admin re-queued ${result.modifiedCount} failed run(s)`);
    res.json({ success: true, requeued: result.modifiedCount });
  } catch (e) {
    err('POST /api/admin/failures/retry:', e?.message || e);
    res.status(500).json({ error: e?.message || 'Could not retry those runs' });
  }
});

app.get('/api/admin/panel-balances', requireAdmin, async (req, res) => {
  try {
    const force = String(req.query?.refresh || '') === '1';
    const settings = await getPaymentSettings();
    const panels = await getAllPanelBalances({ force });
    const threshold = Number(settings.lowBalanceThreshold) || 0;
    res.json({
      threshold,
      panels: panels.map(p => ({
        ...p,
        isLow: threshold > 0 && p.ok && p.balanceInr != null && p.balanceInr < threshold,
      })),
    });
  } catch (e) {
    err('GET /api/admin/panel-balances:', e?.message || e);
    res.status(500).json({ error: 'Could not read panel balances' });
  }
});

/* ---- Admin: run the low-balance check now (also used to test alerts) ---- */
app.post('/api/admin/panel-balances/check', requireAdmin, async (_req, res) => {
  try {
    const result = await checkPanelBalances({ force: true });
    res.json({ success: true, ...result });
  } catch (e) {
    err('POST /api/admin/panel-balances/check:', e?.message || e);
    res.status(500).json({ error: 'Could not run the balance check' });
  }
});

/* ---- Admin: send a test Telegram alert ----
   Lets you confirm the bot token and chat id actually work without
   having to make a real payment. */
app.post('/api/admin/telegram/test', requireAdmin, async (_req, res) => {
  if (!TELEGRAM_ENABLED) {
    return res.status(400).json({
      error: 'Telegram is not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Render, then redeploy.',
      configured: false,
    });
  }
  const ok = await notifyTelegram(
    '✅ <b>TRUESMM test alert</b>\n\nIf you can read this, deposit alerts are working.'
  );
  if (!ok) {
    return res.status(502).json({
      error: 'Telegram rejected the message. Double-check the bot token and chat id.',
      configured: true,
    });
  }
  res.json({ success: true, configured: true });
});

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
        hasOrderAccess: u.hasOrderAccess === true,
        referralCode: u.referralCode || '',
        referralCount: u.referralCount || 0,
        orderAccessAt: u.orderAccessAt || null,
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

/* ---- Admin: grant or revoke New Order access by hand ----
   Useful for comping a friend, or undoing a mistaken approval. */
app.post('/api/admin/users/:id/order-access', requireAdmin, async (req, res) => {
  try {
    const grant = Boolean(req.body?.hasOrderAccess);
    const user = await User.findById(req.params.id);
    if (!user) return res.status(404).json({ error: 'User not found' });

    user.hasOrderAccess = grant;
    user.orderAccessAt = grant ? new Date() : null;
    user.orderAccessSource = grant ? 'admin' : '';
    await user.save();

    log(`${grant ? '🔓' : '🔒'} Order access ${grant ? 'granted to' : 'revoked from'} ${user.email}`);
    res.json({ success: true, id: String(user._id), hasOrderAccess: grant });
  } catch (e) {
    err('POST /api/admin/users/:id/order-access:', e?.message || e);
    res.status(500).json({ error: 'Could not update access' });
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
    minViewsPerRun: MIN_VIEWS_FLOOR,
    // Delivery lag indicators
    overdueRuns: overdue,
    oldestOverdueMinutes,
    telegram: TELEGRAM_ENABLED ? 'on' : 'off',
    schedulerHealthy: oldestOverdueMinutes < 10,
    keepAlive: KEEP_ALIVE_URL ? 'on' : 'off',
  });
});
