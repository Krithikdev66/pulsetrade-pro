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

// Asset Registry
const prices = {
  AAPL: 182.50, MSFT: 415.20, NVDA: 128.40, TSLA: 248.90, AMZN: 188.10,
  GOOGL: 165.30, META: 510.40, AMD: 162.30, INTC: 31.20, QCOM: 174.50,
  AVGO: 142.80, TSM: 178.60, ARM: 138.40, MU: 112.50, SMCI: 48.20,
  CRM: 285.40, ORCL: 168.90, ADBE: 520.10, NOW: 835.00, PLTR: 38.60,
  SNOW: 118.20, PANW: 345.50, CRWD: 295.30, UBER: 74.20, ABNB: 122.40,
  JPM: 215.40, BAC: 39.80, WFC: 56.40, GS: 485.20, MS: 98.70,
  V: 278.40, MA: 468.20, PYPL: 68.50, COIN: 215.80, SOFI: 9.40,
  WMT: 78.50, COST: 885.20, TGT: 148.90, HD: 382.40, NKE: 82.50,
  SBUX: 94.60, MCD: 295.40, KO: 68.20, PEP: 172.50, DIS: 94.80,
  BA: 156.40, CAT: 388.20, GE: 182.30, LMT: 565.40,
  LLY: 915.20, UNH: 585.40, JNJ: 162.40, PFE: 28.50, ABBV: 192.50,
  SPY: 575.20, QQQ: 485.40, IWM: 220.50, DIA: 422.80, GLD: 245.60,
  BTC: 68500.00, ETH: 3500.00, SOL: 180.00, BNB: 590.00, XRP: 0.58
};

const activeSymbols = new Set(Object.keys(prices));
let binanceWs = null;
let finnhubWs = null;

// Guest Account
if (!db.memStore.portfolios.has('demo_guest_user')) {
  db.memStore.portfolios.set('demo_guest_user', {
    user_id: 'demo_guest_user',
    cash_balance: 100000.00,
    starting_balance: 100000.00
  });
}

// ----------------------------------------------------
// DYNAMIC SYMBOL RESOLVER
// ----------------------------------------------------
async function resolveSymbol(symbol) {
  const sym = symbol.toUpperCase().trim();
  if (prices[sym]) return prices[sym];

  if (FINNHUB_KEY) {
    try {
      const res = await fetch(`https://finnhub.io/api/v1/quote?symbol=${sym}&token=${FINNHUB_KEY}`);
      const data = await res.json();
      if (data && data.c && data.c > 0) {
        prices[sym] = parseFloat(data.c);
        activeSymbols.add(sym);
        if (finnhubWs && finnhubWs.readyState === WebSocket.OPEN) {
          finnhubWs.send(JSON.stringify({ type: 'subscribe', symbol: sym }));
        }
        return prices[sym];
      }
    } catch (e) {}
  }

  let hash = 0;
  for (let i = 0; i < sym.length; i++) hash = sym.charCodeAt(i) + ((hash << 5) - hash);
  const baseline = +(Math.abs(hash % 300) + 25.50).toFixed(2);

  prices[sym] = baseline;
  activeSymbols.add(sym);
  return baseline;
}

// ----------------------------------------------------
// ORDER MATCHING & ALERT EVALUATION
// ----------------------------------------------------
async function evaluateOrdersAndAlerts(symbol, price) {
  const curPrice = parseFloat(price);

  // 1. Evaluate Orders in Database or Memory
  if (db.isPostgresConnected()) {
    try {
      const q = await db.query(
        "SELECT * FROM orders WHERE symbol = $1 AND status = 'PENDING'",
        [symbol]
      );
      for (const order of q.rows) {
        let triggered = false;
        const targetPrice = parseFloat(order.price);

        if (order.order_type === 'LIMIT') {
          if (order.side === 'BUY' && curPrice <= targetPrice) triggered = true;
          if (order.side === 'SELL' && curPrice >= targetPrice) triggered = true;
        } else if (order.order_type === 'STOP_LOSS' && order.side === 'SELL' && curPrice <= targetPrice) {
          triggered = true;
        } else if (order.order_type === 'TAKE_PROFIT' && order.side === 'SELL' && curPrice >= targetPrice) {
          triggered = true;
        }

        if (triggered) {
          await executeOrderMatchDB(order, curPrice);
        }
      }
    } catch (e) {}
  } else {
    for (const [orderId, order] of db.memStore.orders.entries()) {
      if (order.symbol === symbol && order.status === 'PENDING' && !order.is_alert) {
        let triggered = false;
        const targetPrice = parseFloat(order.price);
        if (order.order_type === 'LIMIT') {
          if (order.side === 'BUY' && curPrice <= targetPrice) triggered = true;
          if (order.side === 'SELL' && curPrice >= targetPrice) triggered = true;
        } else if (order.order_type === 'STOP_LOSS' && order.side === 'SELL' && curPrice <= targetPrice) {
          triggered = true;
        } else if (order.order_type === 'TAKE_PROFIT' && order.side === 'SELL' && curPrice >= targetPrice) {
          triggered = true;
        }
        if (triggered) await executeOrderMatchMemory(orderId, order, curPrice);
      }
    }
  }

  // 2. Evaluate Alerts
  for (const [alertId, alert] of db.memStore.orders.entries()) {
    if (alert.symbol === symbol && alert.is_alert && alert.is_active) {
      let alertHit = false;
      if (alert.direction === 'ABOVE' && curPrice >= alert.target_price) alertHit = true;
      if (alert.direction === 'BELOW' && curPrice <= alert.target_price) alertHit = true;

      if (alertHit) {
        alert.is_active = false;
        alert.triggered_at = new Date().toISOString();
        broadcastToUser(alert.user_id, { type: 'alert_triggered', alert, currentPrice: curPrice });
      }
    }
  }
}

// Order Execution for PostgreSQL
async function executeOrderMatchDB(order, execPrice) {
  const userId = order.user_id;
  const qty = parseFloat(order.quantity);
  const cost = qty * execPrice;

  try {
    const pQ = await db.query('SELECT cash_balance FROM portfolios WHERE user_id = $1', [userId]);
    let cash = pQ.rows[0] ? parseFloat(pQ.rows[0].cash_balance) : 100000;

    if (order.side === 'BUY') {
      if (cash < cost) {
        await db.query("UPDATE orders SET status = 'REJECTED' WHERE id = $1", [order.id]);
        return;
      }
      await db.query('UPDATE portfolios SET cash_balance = cash_balance - $1 WHERE user_id = $2', [cost, userId]);

      const posQ = await db.query('SELECT * FROM positions WHERE user_id = $1 AND symbol = $2', [userId, order.symbol]);
      if (posQ.rows[0]) {
        const curPos = posQ.rows[0];
        const totalQty = parseFloat(curPos.quantity) + qty;
        const totalCost = (parseFloat(curPos.quantity) * parseFloat(curPos.average_entry_price)) + cost;
        const newAvg = totalCost / totalQty;
        await db.query('UPDATE positions SET quantity = $1, average_entry_price = $2 WHERE id = $3', [totalQty, newAvg, curPos.id]);
      } else {
        await db.query('INSERT INTO positions (user_id, symbol, quantity, average_entry_price) VALUES ($1, $2, $3, $4)', [userId, order.symbol, qty, execPrice]);
      }
    } else {
      const posQ = await db.query('SELECT * FROM positions WHERE user_id = $1 AND symbol = $2', [userId, order.symbol]);
      if (!posQ.rows[0] || parseFloat(posQ.rows[0].quantity) < qty) {
        await db.query("UPDATE orders SET status = 'REJECTED' WHERE id = $1", [order.id]);
        return;
      }
      const curPos = posQ.rows[0];
      const pnl = (execPrice - parseFloat(curPos.average_entry_price)) * qty;

      await db.query('UPDATE portfolios SET cash_balance = cash_balance + $1 WHERE user_id = $2', [cost, userId]);
      const remQty = parseFloat(curPos.quantity) - qty;

      if (remQty <= 0.00001) {
        await db.query('DELETE FROM positions WHERE id = $1', [curPos.id]);
      } else {
        await db.query('UPDATE positions SET quantity = $1 WHERE id = $2', [remQty, curPos.id]);
      }

      await db.query(
        'INSERT INTO trades (user_id, symbol, side, quantity, execution_price, realized_pnl) VALUES ($1, $2, $3, $4, $5, $6)',
        [userId, order.symbol, 'SELL', qty, execPrice, pnl]
      );
    }

    await db.query("UPDATE orders SET status = 'FILLED', executed_at = CURRENT_TIMESTAMP WHERE id = $1", [order.id]);
    broadcastToUser(userId, { type: 'order_filled', order, executionPrice: execPrice });
  } catch (err) {
    console.error('[DB Order Execution Error]:', err.message);
  }
}

// Order Execution for Memory Store Fallback
async function executeOrderMatchMemory(orderId, order, execPrice) {
  const userId = order.user_id;
  const portfolio = db.memStore.portfolios.get(userId);
  if (!portfolio) return;

  const cost = order.quantity * execPrice;
  if (order.side === 'BUY') {
    if (portfolio.cash_balance < cost) {
      order.status = 'REJECTED';
      return;
    }
    portfolio.cash_balance -= cost;
    const posKey = `${userId}_${order.symbol}`;
    const pos = db.memStore.positions.get(posKey) || { user_id: userId, symbol: order.symbol, quantity: 0, average_entry_price: 0 };
    const totalCost = (pos.quantity * pos.average_entry_price) + cost;
    pos.quantity += order.quantity;
    pos.average_entry_price = totalCost / pos.quantity;
    db.memStore.positions.set(posKey, pos);
  } else if (order.side === 'SELL') {
    const posKey = `${userId}_${order.symbol}`;
    const pos = db.memStore.positions.get(posKey);
    if (!pos || pos.quantity < order.quantity) {
      order.status = 'REJECTED';
      return;
    }
    portfolio.cash_balance += cost;
    const pnl = (execPrice - pos.average_entry_price) * order.quantity;
    pos.quantity -= order.quantity;
    if (pos.quantity <= 0.0001) db.memStore.positions.delete(posKey);

    db.memStore.trades.unshift({
      id: 'tr_' + Date.now(),
      user_id: userId,
      symbol: order.symbol,
      side: 'SELL',
      quantity: order.quantity,
      execution_price: execPrice,
      realized_pnl: pnl,
      created_at: new Date().toISOString()
    });
  }

  order.status = 'FILLED';
  order.executed_at = new Date().toISOString();
  broadcastToUser(userId, { type: 'order_filled', order, executionPrice: execPrice });
}

function broadcastToUser(userId, payload) {
  const msg = JSON.stringify(payload);
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN && c.userId === userId) c.send(msg);
  });
}

function broadcastTick(symbol, price, qty = 1.0) {
  prices[symbol] = price;
  const payload = JSON.stringify({
    type: 'trade',
    data: [{ s: symbol, p: price, t: Date.now(), q: qty }]
  });

  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(payload);
  });

  evaluateOrdersAndAlerts(symbol, price);
}

// ----------------------------------------------------
// 24/7 DATA FEEDS
// ----------------------------------------------------
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

function initFinnhub() {
  if (FINNHUB_KEY) {
    finnhubWs = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);
    finnhubWs.on('open', () => {
      Array.from(activeSymbols).slice(0, 30).forEach(s => {
        if (!['BTC', 'ETH', 'SOL', 'BNB', 'XRP'].includes(s)) {
          finnhubWs.send(JSON.stringify({ type: 'subscribe', symbol: s }));
        }
      });
    });
    finnhubWs.on('message', (raw) => {
      try {
        const res = JSON.parse(raw);
        if (res.type === 'trade' && res.data) {
          res.data.forEach(t => broadcastTick(t.s, t.p, t.v));
        }
      } catch (e) {}
    });
    finnhubWs.on('close', () => setTimeout(initFinnhub, 5000));
  }

  // Stock Simulation Engine
  setInterval(() => {
    const stockList = Array.from(activeSymbols).filter(s => !['BTC', 'ETH', 'SOL', 'BNB', 'XRP'].includes(s));
    if (stockList.length === 0) return;

    for (let k = 0; k < 3; k++) {
      const s = stockList[Math.floor(Math.random() * stockList.length)];
      const base = prices[s] || 150.0;
      const step = base > 500 ? 1.5 : (base > 100 ? 0.6 : 0.15);
      const delta = (Math.random() - 0.49) * step;
      const newPrice = +(base + delta).toFixed(2);
      broadcastTick(s, newPrice, Math.floor(Math.random() * 25) + 1);
    }
  }, 600);
}

// ----------------------------------------------------
// REST API
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

app.get('/api/quote', async (req, res) => {
  const sym = (req.query.symbol || '').toUpperCase().trim();
  if (!sym) return res.status(400).json({ error: 'Symbol required' });
  const price = await resolveSymbol(sym);
  res.json({ symbol: sym, price });
});

// Market Sentiment Index (Crypto Fear & Greed Proxy)
app.get('/api/sentiment', async (req, res) => {
  try {
    const r = await fetch('https://api.alternative.me/fng/?limit=1');
    const d = await r.json();
    if (d && d.data && d.data[0]) {
      return res.json({
        value: parseInt(d.data[0].value),
        classification: d.data[0].value_classification,
        timestamp: d.data[0].timestamp
      });
    }
  } catch (e) {}

  res.json({ value: 68, classification: 'Greed', timestamp: Date.now() });
});

// Authentication Routes
app.post('/api/auth/register', async (req, res) => {
  const { email, password, fullName } = req.body;
  if (!email || !password || !fullName) return res.status(400).json({ error: 'All fields required.' });

  try {
    const hash = await bcrypt.hash(password, 10);
    let userId = 'usr_' + Date.now();

    if (db.isPostgresConnected()) {
      const q = await db.query('INSERT INTO users (email, password_hash, full_name) VALUES ($1, $2, $3) RETURNING id', [email, hash, fullName]);
      userId = q.rows[0].id;
      await db.query('INSERT INTO portfolios (user_id, cash_balance) VALUES ($1, 100000.00)', [userId]);
    } else {
      db.memStore.users.set(email, { id: userId, email, passwordHash: hash, fullName });
      db.memStore.portfolios.set(userId, { user_id: userId, cash_balance: 100000.00, starting_balance: 100000.00 });
    }

    const token = jwt.sign({ id: userId, email, name: fullName }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { id: userId, email, name: fullName } });
  } catch (err) {
    res.status(400).json({ error: 'User registration failed or email in use.' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  const { email, password } = req.body;
  try {
    let user = null;
    if (db.isPostgresConnected()) {
      const q = await db.query('SELECT * FROM users WHERE email = $1', [email]);
      user = q.rows[0];
    } else {
      user = db.memStore.users.get(email);
    }

    if (!user) return res.status(400).json({ error: 'Invalid credentials.' });
    const match = await bcrypt.compare(password, user.password_hash || user.passwordHash);
    if (!match) return res.status(400).json({ error: 'Invalid credentials.' });

    const token = jwt.sign({ id: user.id, email: user.email, name: user.full_name || user.fullName }, JWT_SECRET, { expiresIn: '7d' });
    res.json({ token, user: { id: user.id, email: user.email, name: user.full_name || user.fullName } });
  } catch (e) {
    res.status(500).json({ error: 'Authentication failed.' });
  }
});

// Change Password
app.post('/api/auth/change-password', authMiddleware, async (req, res) => {
  const userId = req.user.id;
  const { currentPassword, newPassword } = req.body;

  if (!newPassword || newPassword.length < 6) {
    return res.status(400).json({ error: 'New password must be at least 6 characters.' });
  }

  try {
    let user = null;
    if (db.isPostgresConnected() && userId !== 'demo_guest_user') {
      const q = await db.query('SELECT * FROM users WHERE id = $1', [userId]);
      user = q.rows[0];
      if (!user) return res.status(404).json({ error: 'User not found.' });

      const valid = await bcrypt.compare(currentPassword, user.password_hash);
      if (!valid) return res.status(400).json({ error: 'Incorrect current password.' });

      const newHash = await bcrypt.hash(newPassword, 10);
      await db.query('UPDATE users SET password_hash = $1 WHERE id = $2', [newHash, userId]);
    } else {
      user = Array.from(db.memStore.users.values()).find(u => u.id === userId);
      if (!user) return res.status(404).json({ error: 'Guest demo accounts cannot change password. Register an account first!' });

      const valid = await bcrypt.compare(currentPassword, user.passwordHash);
      if (!valid) return res.status(400).json({ error: 'Incorrect current password.' });

      user.passwordHash = await bcrypt.hash(newPassword, 10);
    }

    res.json({ success: true, message: 'Password updated successfully!' });
  } catch (err) {
    res.status(500).json({ error: 'Could not update password.' });
  }
});

// Portfolio API
app.get('/api/portfolio', authMiddleware, async (req, res) => {
  const userId = req.user.id;
  let cash = 100000.00;
  let positions = [];

  if (db.isPostgresConnected() && userId !== 'demo_guest_user') {
    const pQ = await db.query('SELECT cash_balance FROM portfolios WHERE user_id = $1', [userId]);
    if (pQ.rows[0]) cash = parseFloat(pQ.rows[0].cash_balance);

    const posQ = await db.query('SELECT * FROM positions WHERE user_id = $1', [userId]);
    positions = posQ.rows.map(r => ({
      symbol: r.symbol,
      quantity: parseFloat(r.quantity),
      average_entry_price: parseFloat(r.average_entry_price)
    }));
  } else {
    const p = db.memStore.portfolios.get(userId) || { cash_balance: 100000.00 };
    cash = p.cash_balance;
    for (const [k, v] of db.memStore.positions.entries()) {
      if (v.user_id === userId) positions.push(v);
    }
  }

  let invested = 0;
  let unrealized = 0;
  const enrichedPositions = positions.map(pos => {
    const cur = prices[pos.symbol] || pos.average_entry_price;
    const mVal = pos.quantity * cur;
    const cost = pos.quantity * pos.average_entry_price;
    const pnl = mVal - cost;
    invested += cost;
    unrealized += pnl;
    return {
      ...pos,
      current_price: cur,
      market_value: mVal,
      unrealized_pnl: pnl,
      pnl_percent: cost > 0 ? (pnl / cost) * 100 : 0
    };
  });

  res.json({
    cash_balance: cash,
    invested_amount: invested,
    total_unrealized_pnl: unrealized,
    total_portfolio_value: cash + invested + unrealized,
    positions: enrichedPositions
  });
});

// Orders (Writing to PostgreSQL Database)
app.post('/api/orders', authMiddleware, async (req, res) => {
  const userId = req.user.id;
  const { symbol, order_type, side, quantity, price } = req.body;
  const qty = parseFloat(quantity);
  const curPrice = await resolveSymbol(symbol);

  if (!qty || qty <= 0 || !curPrice) return res.status(400).json({ error: 'Invalid order parameters.' });

  const orderId = 'ord_' + Date.now();
  const cost = qty * curPrice;

  // Immediate Market Order
  if (order_type === 'MARKET') {
    if (db.isPostgresConnected() && userId !== 'demo_guest_user') {
      const pQ = await db.query('SELECT cash_balance FROM portfolios WHERE user_id = $1', [userId]);
      const cash = pQ.rows[0] ? parseFloat(pQ.rows[0].cash_balance) : 100000;

      if (side === 'BUY') {
        if (cash < cost) return res.status(400).json({ error: 'Insufficient cash balance.' });
        await db.query('UPDATE portfolios SET cash_balance = cash_balance - $1 WHERE user_id = $2', [cost, userId]);

        const posQ = await db.query('SELECT * FROM positions WHERE user_id = $1 AND symbol = $2', [userId, symbol.toUpperCase()]);
        if (posQ.rows[0]) {
          const p = posQ.rows[0];
          const totalQty = parseFloat(p.quantity) + qty;
          const totalCost = (parseFloat(p.quantity) * parseFloat(p.average_entry_price)) + cost;
          await db.query('UPDATE positions SET quantity = $1, average_entry_price = $2 WHERE id = $3', [totalQty, totalCost / totalQty, p.id]);
        } else {
          await db.query('INSERT INTO positions (user_id, symbol, quantity, average_entry_price) VALUES ($1, $2, $3, $4)', [userId, symbol.toUpperCase(), qty, curPrice]);
        }
      } else {
        const posQ = await db.query('SELECT * FROM positions WHERE user_id = $1 AND symbol = $2', [userId, symbol.toUpperCase()]);
        if (!posQ.rows[0] || parseFloat(posQ.rows[0].quantity) < qty) {
          return res.status(400).json({ error: 'Insufficient asset ownership to sell.' });
        }
        const p = posQ.rows[0];
        const pnl = (curPrice - parseFloat(p.average_entry_price)) * qty;
        await db.query('UPDATE portfolios SET cash_balance = cash_balance + $1 WHERE user_id = $2', [cost, userId]);
        const rem = parseFloat(p.quantity) - qty;

        if (rem <= 0.00001) await db.query('DELETE FROM positions WHERE id = $1', [p.id]);
        else await db.query('UPDATE positions SET quantity = $1 WHERE id = $2', [rem, p.id]);

        await db.query('INSERT INTO trades (user_id, symbol, side, quantity, execution_price, realized_pnl) VALUES ($1, $2, $3, $4, $5, $6)', [userId, symbol.toUpperCase(), 'SELL', qty, curPrice, pnl]);
      }

      await db.query("INSERT INTO orders (user_id, symbol, order_type, side, quantity, price, status) VALUES ($1, $2, $3, $4, $5, $6, 'FILLED')", [userId, symbol.toUpperCase(), order_type, side, qty, curPrice]);
      return res.json({ success: true, status: 'FILLED', order: { symbol: symbol.toUpperCase(), side, execution_price: curPrice } });
    } else {
      // Memory Store Execution
      const portfolio = db.memStore.portfolios.get(userId) || { cash_balance: 100000.00 };
      if (side === 'BUY') {
        if (portfolio.cash_balance < cost) return res.status(400).json({ error: 'Insufficient cash balance.' });
        portfolio.cash_balance -= cost;

        const posKey = `${userId}_${symbol.toUpperCase()}`;
        const pos = db.memStore.positions.get(posKey) || { user_id: userId, symbol: symbol.toUpperCase(), quantity: 0, average_entry_price: 0 };
        const totalCost = (pos.quantity * pos.average_entry_price) + cost;
        pos.quantity += qty;
        pos.average_entry_price = totalCost / pos.quantity;
        db.memStore.positions.set(posKey, pos);
      } else {
        const posKey = `${userId}_${symbol.toUpperCase()}`;
        const pos = db.memStore.positions.get(posKey);
        if (!pos || pos.quantity < qty) return res.status(400).json({ error: 'Insufficient asset ownership to sell.' });

        portfolio.cash_balance += cost;
        const realizedPnl = (curPrice - pos.average_entry_price) * qty;
        pos.quantity -= qty;
        if (pos.quantity <= 0.0001) db.memStore.positions.delete(posKey);

        db.memStore.trades.unshift({
          id: 'tr_' + Date.now(),
          user_id: userId,
          symbol: symbol.toUpperCase(),
          side: 'SELL',
          quantity: qty,
          execution_price: curPrice,
          realized_pnl: realizedPnl,
          created_at: new Date().toISOString()
        });
      }
      return res.json({ success: true, status: 'FILLED', order: { id: orderId, symbol: symbol.toUpperCase(), side, execution_price: curPrice } });
    }
  }

  // Pending Limit, Stop-Loss, or Take-Profit Orders
  const targetPrice = parseFloat(price);
  if (db.isPostgresConnected() && userId !== 'demo_guest_user') {
    const q = await db.query(
      "INSERT INTO orders (user_id, symbol, order_type, side, quantity, price, status) VALUES ($1, $2, $3, $4, $5, $6, 'PENDING') RETURNING *",
      [userId, symbol.toUpperCase(), order_type, side, qty, targetPrice]
    );
    res.json({ success: true, status: 'PENDING', order: q.rows[0] });
  } else {
    const newOrder = { id: orderId, user_id: userId, symbol: symbol.toUpperCase(), order_type, side, quantity: qty, price: targetPrice, status: 'PENDING', created_at: new Date().toISOString() };
    db.memStore.orders.set(orderId, newOrder);
    res.json({ success: true, status: 'PENDING', order: newOrder });
  }
});

// Pending Orders Endpoint
app.get('/api/orders/pending', authMiddleware, async (req, res) => {
  const userId = req.user.id;
  if (db.isPostgresConnected() && userId !== 'demo_guest_user') {
    const q = await db.query("SELECT * FROM orders WHERE user_id = $1 AND status = 'PENDING' ORDER BY created_at DESC", [userId]);
    res.json(q.rows.map(o => ({ ...o, quantity: parseFloat(o.quantity), price: parseFloat(o.price) })));
  } else {
    res.json(Array.from(db.memStore.orders.values()).filter(o => o.user_id === userId && o.status === 'PENDING' && !o.is_alert));
  }
});

// Cancel Order
app.delete('/api/orders/:id', authMiddleware, async (req, res) => {
  const userId = req.user.id;
  const orderId = req.params.id;
  if (db.isPostgresConnected() && userId !== 'demo_guest_user') {
    await db.query("UPDATE orders SET status = 'CANCELLED' WHERE id = $1 AND user_id = $2", [orderId, userId]);
  } else {
    db.memStore.orders.delete(orderId);
  }
  res.json({ success: true });
});

// Trades History Endpoint
app.get('/api/trades', authMiddleware, async (req, res) => {
  const userId = req.user.id;
  if (db.isPostgresConnected() && userId !== 'demo_guest_user') {
    const q = await db.query('SELECT * FROM trades WHERE user_id = $1 ORDER BY created_at DESC LIMIT 50', [userId]);
    res.json(q.rows.map(t => ({ ...t, quantity: parseFloat(t.quantity), execution_price: parseFloat(t.execution_price), realized_pnl: parseFloat(t.realized_pnl) })));
  } else {
    res.json(db.memStore.trades.filter(t => t.user_id === userId));
  }
});

// Reset Portfolio to $100,000
app.post('/api/portfolio/reset', authMiddleware, async (req, res) => {
  const userId = req.user.id;
  if (db.isPostgresConnected() && userId !== 'demo_guest_user') {
    await db.query('UPDATE portfolios SET cash_balance = 100000.00 WHERE user_id = $1', [userId]);
    await db.query('DELETE FROM positions WHERE user_id = $1', [userId]);
  } else {
    db.memStore.portfolios.set(userId, { user_id: userId, cash_balance: 100000.00, starting_balance: 100000.00 });
    for (const key of db.memStore.positions.keys()) {
      if (key.startsWith(userId)) db.memStore.positions.delete(key);
    }
  }
  res.json({ success: true, message: 'Portfolio reset to $100,000.00' });
});

// Alerts
app.get('/api/alerts', authMiddleware, (req, res) => {
  const userId = req.user.id;
  res.json(Array.from(db.memStore.orders.values()).filter(o => o.user_id === userId && o.is_alert));
});

app.post('/api/alerts', authMiddleware, (req, res) => {
  const userId = req.user.id;
  const { symbol, target_price, direction } = req.body;
  const alertId = 'alt_' + Date.now();
  const newAlert = { id: alertId, user_id: userId, symbol: symbol.toUpperCase(), target_price: parseFloat(target_price), direction: direction || 'ABOVE', is_alert: true, is_active: true, created_at: new Date().toISOString() };
  db.memStore.orders.set(alertId, newAlert);
  res.json({ success: true, alert: newAlert });
});

app.delete('/api/alerts/:id', authMiddleware, (req, res) => {
  db.memStore.orders.delete(req.params.id);
  res.json({ success: true });
});

app.patch('/api/alerts/:id/toggle', authMiddleware, (req, res) => {
  const a = db.memStore.orders.get(req.params.id);
  if (a) {
    a.is_active = !a.is_active;
    return res.json({ success: true, alert: a });
  }
  res.status(404).json({ error: 'Alert not found' });
});

// News Feed
app.get('/api/news', async (req, res) => {
  try {
    const r = await fetch('https://min-api.cryptocompare.com/data/v2/news/?lang=EN');
    const d = await r.json();
    return res.json(d.Data.slice(0, 12).map(x => ({ title: x.title, source: x.source_info.name, url: x.url, published_on: x.published_on })));
  } catch(e) {
    res.json([]);
  }
});

// WebSockets
wss.on('connection', (c) => {
  c.userId = 'demo_guest_user';
  c.on('message', async (m) => {
    try {
      const msg = JSON.parse(m);
      if (msg.action === 'auth' && msg.token) {
        jwt.verify(msg.token, JWT_SECRET, (err, u) => { if (!err && u) c.userId = u.id; });
      } else if (msg.action === 'subscribe' && msg.symbol) {
        await resolveSymbol(msg.symbol);
      }
    } catch(e) {}
  });
});

server.listen(PORT, () => {
  console.log(`\n======================================================`);
  console.log(` PulseTrade Pro Live: http://localhost:${PORT}`);
  console.log(` Database: ${db.isPostgresConnected() ? 'PostgreSQL Production' : 'Memory Store Engine'}`);
  console.log(`======================================================\n`);
  initBinance();
  initFinnhub();
});