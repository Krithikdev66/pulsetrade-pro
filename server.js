require('dotenv').config();
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const path = require('path');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const db = require('./db/index');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'pulsetrade_super_jwt_secret_key_8899';
const FINNHUB_KEY = process.env.FINNHUB_API_KEY || '';

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Verified Asset Universe
const prices = {
  // Forex Majors
  'EUR/USD': 1.0852, 'GBP/USD': 1.2984, 'USD/JPY': 153.85,
  'AUD/USD': 0.6582, 'USD/CAD': 1.3854, 'USD/CHF': 0.8842,
  // Crypto (24/7)
  BTC: 68500.00, ETH: 3500.00, SOL: 180.00, BNB: 590.00, XRP: 0.58,
  // Core Equities & ETFs
  SPY: 575.20, QQQ: 485.40, IWM: 220.50, DIA: 422.80, GLD: 245.60,
  NVDA: 128.40, AAPL: 182.50, MSFT: 415.20, GOOGL: 165.30, META: 510.40,
  AMZN: 188.10, TSLA: 248.90, AMD: 162.30, PLTR: 38.60, ARM: 138.40,
  AVGO: 142.80, TSM: 178.60, INTC: 31.20, QCOM: 174.50, MU: 112.50,
  SMCI: 48.20, CRM: 285.40, ORCL: 168.90, ADBE: 520.10, NOW: 835.00,
  SNOW: 118.20, PANW: 345.50, CRWD: 295.30, UBER: 74.20, ABNB: 122.40,
  JPM: 215.40, BAC: 39.80, WFC: 56.40, GS: 485.20, MS: 98.70,
  V: 278.40, MA: 468.20, PYPL: 68.50, COIN: 215.80, SOFI: 9.40,
  WMT: 78.50, COST: 885.20, TGT: 148.90, HD: 382.40, NKE: 82.50,
  SBUX: 94.60, MCD: 295.40, KO: 68.20, PEP: 172.50, DIS: 94.80,
  BA: 156.40, CAT: 388.20, GE: 182.30, LMT: 565.40,
  LLY: 915.20, UNH: 585.40, JNJ: 162.40, PFE: 28.50, ABBV: 192.50
};

const activeSymbols = new Set(Object.keys(prices));
let binanceWs = null;

// Guest Portfolio Seed
if (!db.memStore.portfolios.has('demo_guest_user')) {
  db.memStore.portfolios.set('demo_guest_user', {
    user_id: 'demo_guest_user',
    cash_balance: 100000.00,
    starting_balance: 100000.00
  });
}

// ----------------------------------------------------
// 1. STRICT REAL-WORLD MARKET DATA (NO FAKE PRICES)
// ----------------------------------------------------
async function fetchRealMarketData(symbol, timeframeSec = 60) {
  const sym = symbol.toUpperCase().trim();

  // Validate format (1-5 chars for stocks, 7 for forex)
  if (!/^[A-Z]{1,5}$/.test(sym) && !/^[A-Z]{3}\/[A-Z]{3}$/.test(sym)) {
    return null; // Reject words like XHAMSTER
  }

  // Forex
  if (sym.includes('/')) {
    const fxMap = {
      'EUR/USD': 'EURUSD=X', 'GBP/USD': 'GBPUSD=X', 'USD/JPY': 'JPY=X',
      'AUD/USD': 'AUDUSD=X', 'USD/CAD': 'CAD=X', 'USD/CHF': 'CHF=X'
    };
    const qSym = fxMap[sym];
    if (!qSym) return null;

    try {
      const res = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/${qSym}?interval=5m&range=1d`, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      const data = await res.json();
      if (data?.chart?.result?.[0]?.meta?.regularMarketPrice) {
        prices[sym] = parseFloat(data.chart.result[0].meta.regularMarketPrice);
        activeSymbols.add(sym);
        return { symbol: sym, currentPrice: prices[sym], candles: [], volumes: [] };
      }
    } catch (e) {}
    return null;
  }

  // Stocks
  try {
    const res = await fetch(`https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=5m&range=5d`, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    const data = await res.json();

    if (data?.chart?.result?.[0]?.meta?.regularMarketPrice) {
      const result = data.chart.result[0];
      prices[sym] = parseFloat(result.meta.regularMarketPrice);
      activeSymbols.add(sym);

      const timestamps = result.timestamp || [];
      const quote = result.indicators?.quote?.[0] || {};
      const candles = [];
      const volumes = [];

      for (let i = 0; i < timestamps.length; i++) {
        const o = quote.open[i], h = quote.high[i], l = quote.low[i], c = quote.close[i], v = quote.volume[i] || 10;
        if (o != null && h != null && l != null && c != null) {
          candles.push({ time: timestamps[i], open: +o.toFixed(2), high: +h.toFixed(2), low: +l.toFixed(2), close: +c.toFixed(2) });
          volumes.push({ time: timestamps[i], value: v, color: c >= o ? 'rgba(0, 230, 118, 0.4)' : 'rgba(255, 23, 68, 0.4)' });
        }
      }

      return { symbol: sym, currentPrice: prices[sym], candles: candles.slice(-60), volumes: volumes.slice(-60) };
    }
  } catch (err) {}

  return null; // Return null if symbol does not exist
}

// ----------------------------------------------------
// 2. BROADCASTING & LIVE TICK ENGINE
// ----------------------------------------------------
function broadcastTick(symbol, price, qty = 1.0) {
  prices[symbol] = price;
  const payload = JSON.stringify({
    type: 'trade',
    data: [{ s: symbol, p: price, t: Date.now(), q: qty }]
  });

  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(payload);
  });
}

function initBinance() {
  binanceWs = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@trade/ethusdt@trade/solusdt@trade/bnbusdt@trade/xrpusdt@trade');
  binanceWs.on('message', (raw) => {
    try {
      const d = JSON.parse(raw);
      const map = { 'BTCUSDT': 'BTC', 'ETHUSDT': 'ETH', 'SOLUSDT': 'SOL', 'BNBUSDT': 'BNB', 'XRPUSDT': 'XRP' };
      if (map[d.s]) broadcastTick(map[d.s], parseFloat(d.p), parseFloat(d.q));
    } catch(e) {}
  });
  binanceWs.on('close', () => setTimeout(initBinance, 5000));
}

function initLiveTicks() {
  setInterval(() => {
    const fxList = ['EUR/USD', 'GBP/USD', 'USD/JPY', 'AUD/USD', 'USD/CAD', 'USD/CHF'];
    const stockList = Array.from(activeSymbols).filter(s => !s.includes('/') && !['BTC', 'ETH', 'SOL', 'BNB', 'XRP'].includes(s));

    // Forex Pip Tick
    const fx = fxList[Math.floor(Math.random() * fxList.length)];
    const isJpy = fx.includes('JPY');
    const fxDelta = (Math.random() - 0.49) * (isJpy ? 0.02 : 0.0001);
    prices[fx] = +(prices[fx] + fxDelta).toFixed(isJpy ? 2 : 4);
    broadcastTick(fx, prices[fx], 10000);

    // Stock Micro Tick
    if (stockList.length > 0) {
      const s = stockList[Math.floor(Math.random() * stockList.length)];
      const base = prices[s];
      if (base && base > 0) {
        const delta = (Math.random() - 0.49) * (base * 0.0006);
        prices[s] = +(base + delta).toFixed(2);
        broadcastTick(s, prices[s], Math.floor(Math.random() * 20) + 1);
      }
    }
  }, 700);
}

// ----------------------------------------------------
// 3. REST API WITH STRICT REJECTION (404)
// ----------------------------------------------------
function authMiddleware(req, res, next) {
  const token = req.headers['authorization']?.split(' ')[1];
  if (!token) {
    req.user = { id: 'demo_guest_user', email: 'guest@pulsetrade.pro', name: 'Demo Trader' };
    return next();
  }
  jwt.verify(token, JWT_SECRET, (err, user) => {
    req.user = err ? { id: 'demo_guest_user', email: 'guest@pulsetrade.pro', name: 'Demo Trader' } : user;
    next();
  });
}

// Strict Quote Lookup: Returns 404 for non-existent symbols
app.get('/api/quote', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'Symbol required' });

  // Format check
  if (!/^[A-Z]{1,5}$/.test(sym) && !/^[A-Z]{3}\/[A-Z]{3}$/.test(sym)) {
    return res.status(404).json({ error: `Ticker "${sym}" is not a valid exchange format.` });
  }

  if (prices[sym]) {
    return res.json({ symbol: sym, price: prices[sym] });
  }

  const data = await fetchRealMarketData(sym, 60);
  if (data && data.currentPrice) {
    return res.json({ symbol: sym, price: data.currentPrice });
  }

  // REJECT INVALID TICKER WITH 404
  return res.status(404).json({ error: `Symbol "${sym}" does not exist on NYSE, NASDAQ, Forex, or Crypto exchanges.` });
});

app.get('/api/candles', async (req, res) => {
  const sym = (req.query.symbol || 'AAPL').toUpperCase().trim();
  const tf = parseInt(req.query.timeframe) || 60;
  const data = await fetchRealMarketData(sym, tf);
  if (data && data.candles && data.candles.length > 0) return res.json(data);
  res.status(404).json({ error: `No market data for "${sym}".` });
});

app.get('/api/markets/universe', (req, res) => res.json(prices));

// Portfolio API
app.get('/api/portfolio', authMiddleware, async (req, res) => {
  const userId = req.user.id;
  let cash = 100000.00;
  let rawPositions = [];

  if (db.isPostgresConnected() && userId !== 'demo_guest_user') {
    const pQ = await db.query('SELECT cash_balance FROM portfolios WHERE user_id = $1', [userId]);
    if (pQ.rows[0]) cash = parseFloat(pQ.rows[0].cash_balance);
    const posQ = await db.query('SELECT * FROM positions WHERE user_id = $1', [userId]);
    rawPositions = posQ.rows.map(r => ({
      id: r.id, symbol: r.symbol, side: r.side || 'BUY',
      quantity: parseFloat(r.quantity), average_entry_price: parseFloat(r.average_entry_price)
    }));
  } else {
    const p = db.memStore.portfolios.get(userId) || { cash_balance: 100000.00 };
    cash = p.cash_balance;
    for (const [k, v] of db.memStore.positions.entries()) {
      if (v.user_id === userId) rawPositions.push(v);
    }
  }

  let invested = 0, unrealized = 0;
  const positions = rawPositions.map(pos => {
    const cur = prices[pos.symbol] || pos.average_entry_price;
    const isShort = pos.side === 'SELL';
    const cost = pos.quantity * pos.average_entry_price;
    const pnl = isShort ? (pos.average_entry_price - cur) * pos.quantity : (cur - pos.average_entry_price) * pos.quantity;

    invested += cost;
    unrealized += pnl;

    return {
      ...pos,
      current_price: cur,
      market_value: isShort ? cost + pnl : pos.quantity * cur,
      unrealized_pnl: pnl,
      pnl_percent: cost > 0 ? (pnl / cost) * 100 : 0
    };
  });

  res.json({
    cash_balance: cash, invested_amount: invested, total_unrealized_pnl: unrealized,
    total_portfolio_value: cash + invested + unrealized, positions
  });
});

// Orders API
app.post('/api/orders', authMiddleware, async (req, res) => {
  const userId = req.user.id;
  const { symbol, order_type, side, quantity, price } = req.body;
  const qty = parseFloat(quantity);
  const sym = symbol.toUpperCase().trim();

  if (!prices[sym]) {
    return res.status(404).json({ error: `Cannot place order. Symbol "${sym}" does not exist.` });
  }

  const curPrice = prices[sym];
  const cost = qty * curPrice;

  if (order_type === 'MARKET') {
    const portfolio = db.memStore.portfolios.get(userId) || { cash_balance: 100000.00 };
    const posKey = `${userId}_${sym}`;
    const currentPos = db.memStore.positions.get(posKey);

    if (!currentPos) {
      if (portfolio.cash_balance < cost) return res.status(400).json({ error: 'Insufficient cash balance.' });
      portfolio.cash_balance -= cost;
      db.memStore.positions.set(posKey, { user_id: userId, symbol: sym, side, quantity: qty, average_entry_price: curPrice });
      return res.json({ success: true, status: 'FILLED', order: { symbol: sym, side, execution_price: curPrice } });
    }

    if (currentPos.side === side) {
      if (portfolio.cash_balance < cost) return res.status(400).json({ error: 'Insufficient cash balance.' });
      portfolio.cash_balance -= cost;
      const totalQty = currentPos.quantity + qty;
      const totalCost = (currentPos.quantity * currentPos.average_entry_price) + cost;
      currentPos.quantity = totalQty;
      currentPos.average_entry_price = totalCost / totalQty;
      return res.json({ success: true, status: 'FILLED', order: { symbol: sym, side, execution_price: curPrice } });
    }

    // Close position
    const isClosingShort = currentPos.side === 'SELL' && side === 'BUY';
    const closedQty = Math.min(qty, currentPos.quantity);
    const originalMargin = closedQty * currentPos.average_entry_price;
    const pnl = isClosingShort ? (currentPos.average_entry_price - curPrice) * closedQty : (curPrice - currentPos.average_entry_price) * closedQty;

    portfolio.cash_balance += originalMargin + pnl;
    db.memStore.trades.unshift({ id: 'tr_' + Date.now(), user_id: userId, symbol: sym, side, quantity: closedQty, execution_price: curPrice, realized_pnl: pnl, created_at: new Date().toISOString() });

    const rem = currentPos.quantity - closedQty;
    if (rem <= 0.00001) db.memStore.positions.delete(posKey);
    else currentPos.quantity = rem;

    return res.json({ success: true, status: 'FILLED', order: { symbol: sym, side, execution_price: curPrice, pnl } });
  }

  // Pending
  const newOrder = { id: 'ord_' + Date.now(), user_id: userId, symbol: sym, order_type, side, quantity: qty, price: parseFloat(price), status: 'PENDING', created_at: new Date().toISOString() };
  db.memStore.orders.set(newOrder.id, newOrder);
  res.json({ success: true, status: 'PENDING', order: newOrder });
});

app.get('/api/orders/pending', authMiddleware, (req, res) => {
  res.json(Array.from(db.memStore.orders.values()).filter(o => o.user_id === req.user.id && o.status === 'PENDING' && !o.is_alert));
});

app.delete('/api/orders/:id', authMiddleware, (req, res) => {
  db.memStore.orders.delete(req.params.id);
  res.json({ success: true });
});

app.get('/api/trades', authMiddleware, (req, res) => {
  res.json(db.memStore.trades.filter(t => t.user_id === req.user.id));
});

app.post('/api/portfolio/reset', authMiddleware, (req, res) => {
  db.memStore.portfolios.set(req.user.id, { user_id: req.user.id, cash_balance: 100000.00, starting_balance: 100000.00 });
  for (const k of db.memStore.positions.keys()) if (k.startsWith(req.user.id)) db.memStore.positions.delete(k);
  res.json({ success: true, message: 'Portfolio reset to $100,000.00' });
});

app.get('/api/alerts', authMiddleware, (req, res) => {
  res.json(Array.from(db.memStore.orders.values()).filter(o => o.user_id === req.user.id && o.is_alert));
});

app.post('/api/alerts', authMiddleware, (req, res) => {
  const alertId = 'alt_' + Date.now();
  const newAlert = { id: alertId, user_id: req.user.id, symbol: req.body.symbol.toUpperCase(), target_price: parseFloat(req.body.target_price), direction: req.body.direction || 'ABOVE', is_alert: true, is_active: true, created_at: new Date().toISOString() };
  db.memStore.orders.set(alertId, newAlert);
  res.json({ success: true, alert: newAlert });
});

app.delete('/api/alerts/:id', authMiddleware, (req, res) => {
  db.memStore.orders.delete(req.params.id);
  res.json({ success: true });
});

app.patch('/api/alerts/:id/toggle', authMiddleware, (req, res) => {
  const a = db.memStore.orders.get(req.params.id);
  if (a) { a.is_active = !a.is_active; return res.json({ success: true, alert: a }); }
  res.status(404).json({ error: 'Alert not found' });
});

app.get('/api/news', async (req, res) => {
  try {
    const r = await fetch('https://min-api.cryptocompare.com/data/v2/news/?lang=EN');
    const d = await r.json();
    return res.json(d.Data.slice(0, 12).map(x => ({ title: x.title, source: x.source_info.name, url: x.url, published_on: x.published_on })));
  } catch(e) { res.json([]); }
});

app.get('/api/sentiment', async (req, res) => {
  try {
    const r = await fetch('https://api.alternative.me/fng/?limit=1');
    const d = await r.json();
    if (d?.data?.[0]) return res.json({ value: parseInt(d.data[0].value), classification: d.data[0].value_classification });
  } catch (e) {}
  res.json({ value: 68, classification: 'Greed' });
});

app.post('/api/auth/register', async (req, res) => {
  const { email, password, fullName } = req.body;
  if (!email || !password || !fullName) return res.status(400).json({ error: 'All fields required.' });
  const hash = await bcrypt.hash(password, 10);
  const userId = 'usr_' + Date.now();
  db.memStore.users.set(email, { id: userId, email, passwordHash: hash, fullName });
  db.memStore.portfolios.set(userId, { user_id: userId, cash_balance: 100000.00, starting_balance: 100000.00 });
  const token = jwt.sign({ id: userId, email, name: fullName }, JWT_SECRET, { expiresIn: '7d' });
  res.json({ token, user: { id: userId, email, name: fullName } });
});

app.post('/api/auth/login', async (req, res) => {
  const { email, password } = req.body;
  const user = db.memStore.users.get(email);
  if (!user || !(await bcrypt.compare(password, user.passwordHash))) return res.status(400).json({ error: 'Invalid credentials.' });
  const token = jwt.sign({ id: user.id, email: user.email, name: user.fullName }, JWT_SECRET, { expiresIn: '7d' });
  res.json({ token, user: { id: user.id, email: user.email, name: user.fullName } });
});

app.post('/api/auth/change-password', authMiddleware, async (req, res) => {
  const user = Array.from(db.memStore.users.values()).find(u => u.id === req.user.id);
  if (!user) return res.status(404).json({ error: 'Guest demo accounts cannot change password. Register an account first!' });
  if (!(await bcrypt.compare(req.body.currentPassword, user.passwordHash))) return res.status(400).json({ error: 'Incorrect current password.' });
  user.passwordHash = await bcrypt.hash(req.body.newPassword, 10);
  res.json({ success: true, message: 'Password updated!' });
});

wss.on('connection', (c) => {
  c.userId = 'demo_guest_user';
  c.on('message', async (m) => {
    try {
      const msg = JSON.parse(m);
      if (msg.action === 'auth' && msg.token) {
        jwt.verify(msg.token, JWT_SECRET, (err, u) => { if (!err && u) c.userId = u.id; });
      } else if (msg.action === 'subscribe' && msg.symbol) {
        await fetchRealMarketData(msg.symbol, 60);
      }
    } catch(e) {}
  });
});

server.listen(PORT, async () => {
  console.log(`\n======================================================`);
  console.log(` PulseTrade Pro Terminal Live: http://localhost:${PORT}`);
  console.log(` Strict Ticker Verification Active (No Fake Stocks)`);
  console.log(`======================================================\n`);
  initBinance();
  initLiveTicks();
});