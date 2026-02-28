-- Additional views for dashboard (Power BI)
CREATE OR REPLACE VIEW v_mart_portfolio_overview AS
SELECT
  mm.month_end,
  COUNT(DISTINCT mm.merchant_id) AS total_merchants,
  SUM(f.gmv_30d) AS total_gmv,
  AVG(f.fraud_rate_30d) AS portfolio_fraud_rate,
  AVG(f.chargeback_rate_30d) AS portfolio_chargeback_rate,
  AVG(f.sla_breach_rate_60d) AS portfolio_sla_breach_rate,
  AVG(o.default_next_60d) AS portfolio_default_risk
FROM v_merchant_months mm
LEFT JOIN v_merchant_month_features f ON f.merchant_id = mm.merchant_id AND f.month_end = mm.month_end
LEFT JOIN outcomes_monthly o ON o.merchant_id = mm.merchant_id AND o.month_end = mm.month_end
GROUP BY mm.month_end
ORDER BY mm.month_end;

CREATE OR REPLACE VIEW v_mart_merchant_risk_queue AS
SELECT
  f.merchant_id,
  f.month_end,
  f.industry,
  f.state,
  f.gmv_30d,
  f.fraud_rate_30d,
  f.chargeback_rate_30d,
  f.decline_rate_30d,
  f.sla_breach_rate_60d,
  o.default_next_60d
FROM v_merchant_month_features f
LEFT JOIN outcomes_monthly o ON o.merchant_id = f.merchant_id AND o.month_end = f.month_end
WHERE f.month_end = (SELECT MAX(month_end) FROM v_merchant_months);
