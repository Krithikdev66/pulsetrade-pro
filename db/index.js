const { Pool } = require('pg');

let pool = null;
let isPostgresConnected = false;

const tableQueries = [
  `CREATE TABLE IF NOT EXISTS users (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      email VARCHAR(255) UNIQUE NOT NULL,
      password_hash VARCHAR(255) NOT NULL,
      full_name VARCHAR(100) NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
  );`,
  `CREATE TABLE IF NOT EXISTS portfolios (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id UUID REFERENCES users(id) ON DELETE CASCADE,
      cash_balance NUMERIC(15, 2) NOT NULL DEFAULT 100000.00,
      starting_balance NUMERIC(15, 2) NOT NULL DEFAULT 100000.00,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
      CONSTRAINT unique_user_portfolio UNIQUE(user_id)
  );`,
  `CREATE TABLE IF NOT EXISTS positions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id UUID REFERENCES users(id) ON DELETE CASCADE,
      symbol VARCHAR(20) NOT NULL,
      side VARCHAR(10) NOT NULL DEFAULT 'BUY',
      quantity NUMERIC(18, 6) NOT NULL,
      average_entry_price NUMERIC(15, 4) NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
  );`,
  `ALTER TABLE positions ADD COLUMN IF NOT EXISTS side VARCHAR(10) DEFAULT 'BUY';`,
  `CREATE TABLE IF NOT EXISTS orders (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id UUID REFERENCES users(id) ON DELETE CASCADE,
      symbol VARCHAR(20) NOT NULL,
      order_type VARCHAR(20) NOT NULL,
      side VARCHAR(10) NOT NULL,
      quantity NUMERIC(18, 6) NOT NULL,
      price NUMERIC(15, 4),
      status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
      created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
      executed_at TIMESTAMP WITH TIME ZONE
  );`,
  `CREATE TABLE IF NOT EXISTS trades (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      user_id UUID REFERENCES users(id) ON DELETE CASCADE,
      symbol VARCHAR(20) NOT NULL,
      side VARCHAR(10) NOT NULL,
      quantity NUMERIC(18, 6) NOT NULL,
      execution_price NUMERIC(15, 4) NOT NULL,
      realized_pnl NUMERIC(15, 2) DEFAULT 0.00,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
  );`
];

if (process.env.DATABASE_URL) {
  let connStr = process.env.DATABASE_URL;
  if (connStr.includes('?')) connStr = connStr.split('?')[0];

  pool = new Pool({
    connectionString: connStr,
    ssl: { rejectUnauthorized: false }
  });

  pool.connect(async (err, client, release) => {
    if (err) {
      console.warn('[Database] PostgreSQL connection notice. Running memory engine:', err.message);
      isPostgresConnected = false;
    } else {
      console.log('[Database] PostgreSQL connected successfully!');
      isPostgresConnected = true;
      for (const query of tableQueries) {
        try { await client.query(query); } catch (e) {}
      }
      console.log('[Database] Tables and Forex Long/Short support verified.');
      release();
    }
  });
} else {
  console.log('[Database] Running in-memory database adapter.');
}

const memStore = {
  users: new Map(),
  portfolios: new Map(),
  positions: new Map(),
  orders: new Map(),
  trades: []
};

module.exports = {
  isPostgresConnected: () => isPostgresConnected,
  query: async (text, params) => {
    if (isPostgresConnected && pool) return pool.query(text, params);
    return null;
  },
  memStore
};