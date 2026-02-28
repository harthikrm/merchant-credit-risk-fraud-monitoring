DROP TABLE IF EXISTS outcomes_monthly CASCADE;
DROP TABLE IF EXISTS sla_events CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;

CREATE TABLE merchants (
  merchant_id UUID PRIMARY KEY,
  industry TEXT NOT NULL,
  state CHAR(2) NOT NULL,
  onboard_date DATE NOT NULL,
  risk_tier_true TEXT NOT NULL  -- LOW/MED/HIGH (hidden driver)
);

CREATE TABLE transactions (
  txn_id UUID PRIMARY KEY,
  merchant_id UUID NOT NULL REFERENCES merchants(merchant_id),
  txn_ts TIMESTAMP NOT NULL,
  amount NUMERIC(12,2) NOT NULL CHECK (amount > 0),
  channel TEXT NOT NULL,        -- CARD/ACH
  status TEXT NOT NULL,         -- APPROVED/DECLINED
  is_fraud INT NOT NULL CHECK (is_fraud IN (0,1)),
  is_chargeback INT NOT NULL CHECK (is_chargeback IN (0,1))
);

CREATE INDEX idx_txn_merchant_ts ON transactions(merchant_id, txn_ts);
CREATE INDEX idx_txn_ts ON transactions(txn_ts);

CREATE TABLE sla_events (
  event_id UUID PRIMARY KEY,
  merchant_id UUID NOT NULL REFERENCES merchants(merchant_id),
  event_ts TIMESTAMP NOT NULL,
  event_type TEXT NOT NULL,     -- DISPUTE_RESPONSE_DELAY/SETTLEMENT_DELAY/DOC_REQUEST_MISSED
  breached INT NOT NULL CHECK (breached IN (0,1))
);

CREATE INDEX idx_sla_merchant_ts ON sla_events(merchant_id, event_ts);

CREATE TABLE outcomes_monthly (
  merchant_id UUID NOT NULL REFERENCES merchants(merchant_id),
  month_end DATE NOT NULL,
  default_next_60d INT NOT NULL CHECK (default_next_60d IN (0,1)),
  PRIMARY KEY (merchant_id, month_end)
);

CREATE INDEX idx_outcomes_month ON outcomes_monthly(month_end);
