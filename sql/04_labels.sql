CREATE OR REPLACE VIEW v_default_label_next_60d AS
SELECT
  mm.merchant_id,
  mm.month_end,

  -- future window: (month_end, month_end + 60]
  COALESCE(AVG(t.is_chargeback)::float, 0) AS future_chargeback_rate,
  COALESCE(AVG(t.is_fraud)::float, 0) AS future_fraud_rate,
  COALESCE(AVG((t.status='DECLINED')::int)::float, 0) AS future_decline_rate,
  COALESCE(SUM(t.is_fraud), 0) AS future_fraud_count,
  COUNT(t.txn_id) AS future_txn_count,

  COALESCE(AVG(s.breached)::float, 0) AS future_sla_breach_rate,

  CASE WHEN
    COALESCE(AVG(t.is_chargeback)::float, 0) >= 0.06
    OR COALESCE(AVG(t.is_fraud)::float, 0) >= 0.06
    OR (COALESCE(AVG((t.status='DECLINED')::int)::float, 0) >= 0.25 AND COALESCE(SUM(t.is_fraud), 0) >= 10)
    OR (COALESCE(AVG(s.breached)::float, 0) >= 0.20 AND COUNT(t.txn_id) >= 200)
  THEN 1 ELSE 0 END AS default_next_60d

FROM v_merchant_months mm
LEFT JOIN transactions t
  ON t.merchant_id = mm.merchant_id
 AND t.txn_ts::date >  mm.month_end
 AND t.txn_ts::date <= (mm.month_end + 60)

LEFT JOIN sla_events s
  ON s.merchant_id = mm.merchant_id
 AND s.event_ts::date >  mm.month_end
 AND s.event_ts::date <= (mm.month_end + 60)

GROUP BY 1,2;
