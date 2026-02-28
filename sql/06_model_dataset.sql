CREATE OR REPLACE VIEW v_model_dataset AS
SELECT 
  f.*,
  o.default_next_60d
FROM v_merchant_month_features f
JOIN outcomes_monthly o 
  ON f.merchant_id = o.merchant_id 
 AND f.month_end = o.month_end;
