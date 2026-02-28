-- Data anomaly tests
CREATE OR REPLACE VIEW v_data_exceptions AS
SELECT 'ORPHAN_TXN' AS exception_type, txn_id::text AS reference_id
FROM transactions
WHERE merchant_id NOT IN (SELECT merchant_id FROM merchants)

UNION ALL

SELECT 'NEG_AMOUNT' AS exception_type, txn_id::text AS reference_id
FROM transactions
WHERE amount <= 0

UNION ALL

SELECT 'FUTURE_TS' AS exception_type, txn_id::text AS reference_id
FROM transactions
WHERE txn_ts > timezone('UTC', now())

UNION ALL

SELECT 'DUP_TXN_ID' AS exception_type, txn_id::text AS reference_id
FROM transactions
GROUP BY txn_id
HAVING COUNT(*) > 1

UNION ALL

-- Leakage check: no feature uses txns after month_end
SELECT 'LEAKAGE_GUARD' AS exception_type, f.merchant_id::text || '_' || f.month_end::text AS reference_id
FROM v_merchant_month_features f
JOIN transactions t
  ON t.merchant_id = f.merchant_id
WHERE t.txn_ts::date > f.month_end
  AND f.txn_count_30d > 0;
  
-- Check label monotonicity:
-- High tier > Med tier > Low tier default rate
CREATE OR REPLACE VIEW v_label_sanity AS
SELECT 
  m.risk_tier_true,
  COUNT(*) as total_months,
  SUM(o.default_next_60d) as default_count,
  AVG(o.default_next_60d) as default_rate
FROM outcomes_monthly o
JOIN merchants m ON m.merchant_id = o.merchant_id
GROUP BY m.risk_tier_true
ORDER BY default_rate DESC;
