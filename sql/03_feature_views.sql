-- 1. Month spine
CREATE OR REPLACE VIEW v_merchant_months AS
SELECT
  m.merchant_id,
  d::date AS month_start,
  (d + interval '1 month - 1 day')::date AS month_end
FROM merchants m
CROSS JOIN generate_series(
  date '2025-01-01',
  date '2025-12-01',
  interval '1 month'
) AS d;

-- 2. Past-window features (NO leakage)
-- 2a) Transactions past 30 days (ending at month_end)
CREATE OR REPLACE VIEW v_txn_features_past_30d AS
SELECT
  mm.merchant_id,
  mm.month_end,

  COUNT(*) AS txn_count_30d,
  COALESCE(SUM(CASE WHEN t.status='APPROVED' THEN t.amount ELSE 0 END), 0) AS gmv_30d,
  COALESCE(AVG(CASE WHEN t.status='APPROVED' THEN t.amount END), 0) AS avg_amount_30d,
  COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY CASE WHEN t.status='APPROVED' THEN t.amount END), 0) AS p95_amount_30d,
  COALESCE(STDDEV_SAMP(CASE WHEN t.status='APPROVED' THEN t.amount END), 0) AS amount_stddev_30d,

  COALESCE(AVG((t.status='DECLINED')::int)::float, 0) AS decline_rate_30d,
  COALESCE(AVG(t.is_fraud)::float, 0) AS fraud_rate_30d,
  COALESCE(AVG(t.is_chargeback)::float, 0) AS chargeback_rate_30d,

  COALESCE(AVG((t.channel='ACH')::int)::float, 0) AS ach_share_30d,
  COUNT(DISTINCT t.txn_ts::date) AS active_days_30d

FROM v_merchant_months mm
LEFT JOIN transactions t
  ON t.merchant_id = mm.merchant_id
 AND t.txn_ts::date >  (mm.month_end - 30)
 AND t.txn_ts::date <= mm.month_end
GROUP BY 1,2;

-- 2b) SLA past 60 days
CREATE OR REPLACE VIEW v_sla_features_past_60d AS
SELECT
  mm.merchant_id,
  mm.month_end,
  COUNT(s.event_id) AS sla_events_60d,
  COALESCE(SUM(s.breached), 0) AS sla_breaches_60d,
  COALESCE(AVG(s.breached)::float, 0) AS sla_breach_rate_60d
FROM v_merchant_months mm
LEFT JOIN sla_events s
  ON s.merchant_id = mm.merchant_id
 AND s.event_ts::date >  (mm.month_end - 60)
 AND s.event_ts::date <= mm.month_end
GROUP BY 1,2;

-- 2c) Transactions past 60 days 
CREATE OR REPLACE VIEW v_txn_features_past_60d AS
SELECT
  mm.merchant_id,
  mm.month_end,
  COALESCE(AVG(t.is_fraud)::float, 0) AS fraud_rate_60d,
  COALESCE(AVG(t.is_chargeback)::float, 0) AS chargeback_rate_60d
FROM v_merchant_months mm
LEFT JOIN transactions t
  ON t.merchant_id = mm.merchant_id
 AND t.txn_ts::date >  (mm.month_end - 60)
 AND t.txn_ts::date <= mm.month_end
GROUP BY 1,2;

-- 3) Trend features (MoM deltas)
CREATE OR REPLACE VIEW v_txn_trends_mom AS
SELECT
  cur.merchant_id,
  cur.month_end,
  (cur.gmv_30d - COALESCE(prev.gmv_30d, 0)) AS gmv_mom_change,
  (cur.fraud_rate_30d - COALESCE(prev.fraud_rate_30d, 0)) AS fraud_rate_mom_change,
  (cur.chargeback_rate_30d - COALESCE(prev.chargeback_rate_30d, 0)) AS chargeback_rate_mom_change,
  (cur.decline_rate_30d - COALESCE(prev.decline_rate_30d, 0)) AS decline_rate_mom_change
FROM v_txn_features_past_30d cur
LEFT JOIN v_txn_features_past_30d prev
  ON prev.merchant_id = cur.merchant_id
 AND prev.month_end = (cur.month_end - interval '1 month')::date;

-- 4) Advanced features (90d and historical all-time)
CREATE OR REPLACE VIEW v_daily_declines AS
SELECT 
  merchant_id, 
  txn_ts::date as txn_date,
  AVG((status='DECLINED')::int)::float as daily_dec,
  COUNT(*) as daily_txns
FROM transactions
GROUP BY 1, 2;

CREATE OR REPLACE VIEW v_txn_features_past_90d AS
SELECT
  mm.merchant_id,
  mm.month_end,
  COALESCE(SUM(t.is_fraud), 0) AS cumulative_fraud_90d,
  COALESCE(STDDEV_SAMP(CASE WHEN t.status='APPROVED' THEN t.amount END), 0) AS rolling_3m_volatility_90d,
  COALESCE(AVG((t.status='DECLINED')::int)::float, 0) AS decline_rate_90d,
  COALESCE(MAX(CASE WHEN d.daily_txns >= 3 THEN d.daily_dec END), 0) AS rolling_max_decline_spike
FROM v_merchant_months mm
LEFT JOIN transactions t 
  ON t.merchant_id = mm.merchant_id 
 AND t.txn_ts::date > (mm.month_end - 90) AND t.txn_ts::date <= mm.month_end
LEFT JOIN v_daily_declines d 
  ON d.merchant_id = mm.merchant_id 
 AND d.txn_date > (mm.month_end - 90) AND d.txn_date <= mm.month_end
GROUP BY mm.merchant_id, mm.month_end;

CREATE OR REPLACE VIEW v_merchant_historical_fraud AS
SELECT
  mm.merchant_id,
  mm.month_end,
  COALESCE(AVG(t.is_fraud)::float, 0) AS historical_fraud_rate,
  COALESCE(STDDEV_SAMP(t.is_fraud::int)::float, 0) AS historical_fraud_stddev
FROM v_merchant_months mm
LEFT JOIN transactions t
  ON t.merchant_id = mm.merchant_id
 AND t.txn_ts::date <= mm.month_end
GROUP BY mm.merchant_id, mm.month_end;

-- 5) Final feature table (merchant-month)
CREATE OR REPLACE VIEW v_merchant_month_features AS
SELECT
  mm.merchant_id,
  mm.month_end,
  m.industry,
  m.state,

  COALESCE(t.txn_count_30d, 0) AS txn_count_30d,
  COALESCE(t.gmv_30d, 0) AS gmv_30d,
  COALESCE(t.avg_amount_30d, 0) AS avg_amount_30d,
  COALESCE(t.p95_amount_30d, 0) AS p95_amount_30d,
  COALESCE(t.amount_stddev_30d, 0) AS amount_stddev_30d,
  COALESCE(t.decline_rate_30d, 0) AS decline_rate_30d,
  COALESCE(t.fraud_rate_30d, 0) AS fraud_rate_30d,
  COALESCE(t.chargeback_rate_30d, 0) AS chargeback_rate_30d,
  COALESCE(t.ach_share_30d, 0) AS ach_share_30d,
  COALESCE(t.active_days_30d, 0) AS active_days_30d,

  COALESCE(tr.gmv_mom_change, 0) AS gmv_mom_change,
  COALESCE(tr.fraud_rate_mom_change, 0) AS fraud_rate_mom_change,
  COALESCE(tr.chargeback_rate_mom_change, 0) AS chargeback_rate_mom_change,
  COALESCE(tr.decline_rate_mom_change, 0) AS decline_rate_mom_change,

  COALESCE(s.sla_events_60d, 0) AS sla_events_60d,
  COALESCE(s.sla_breaches_60d, 0) AS sla_breaches_60d,
  COALESCE(s.sla_breach_rate_60d, 0) AS sla_breach_rate_60d,
  
  -- Extra requested 60d Features
  COALESCE(f60.fraud_rate_60d, 0) AS fraud_rate_60d,
  COALESCE(f60.chargeback_rate_60d, 0) AS chargeback_rate_60d,
  
  -- Advanced Features
  COALESCE(f90.cumulative_fraud_90d, 0) AS cumulative_fraud_90d,
  COALESCE(f90.rolling_3m_volatility_90d, 0) AS amount_volatility_90d,
  COALESCE(f90.rolling_max_decline_spike, 0) AS rolling_max_decline_spike,
  COALESCE(f90.decline_rate_90d, 0) AS decline_rate_90d,
  
  COALESCE(tr.fraud_rate_mom_change, 0) AS fraud_acceleration,
  COALESCE(tr.chargeback_rate_mom_change, 0) AS chargeback_acceleration,
  
  CASE WHEN COALESCE(h.historical_fraud_stddev, 0) > 0 THEN 
    (COALESCE(t.fraud_rate_30d, 0) - COALESCE(h.historical_fraud_rate, 0)) / h.historical_fraud_stddev 
  ELSE 0 END AS z_score_fraud,
  
  -- Interaction Terms
  (COALESCE(t.fraud_rate_30d, 0) * COALESCE(t.chargeback_rate_30d, 0)) AS risk_interaction,
  (COALESCE(t.decline_rate_30d, 0) * COALESCE(t.fraud_rate_30d, 0)) AS decline_x_fraud

FROM v_merchant_months mm
JOIN merchants m ON m.merchant_id = mm.merchant_id
LEFT JOIN v_txn_features_past_30d t ON t.merchant_id = mm.merchant_id AND t.month_end = mm.month_end
LEFT JOIN v_txn_trends_mom tr ON tr.merchant_id = mm.merchant_id AND tr.month_end = mm.month_end
LEFT JOIN v_sla_features_past_60d s ON s.merchant_id = mm.merchant_id AND s.month_end = mm.month_end
LEFT JOIN v_txn_features_past_60d f60 ON f60.merchant_id = mm.merchant_id AND f60.month_end = mm.month_end
LEFT JOIN v_txn_features_past_90d f90 ON f90.merchant_id = mm.merchant_id AND f90.month_end = mm.month_end
LEFT JOIN v_merchant_historical_fraud h ON h.merchant_id = mm.merchant_id AND h.month_end = mm.month_end;
