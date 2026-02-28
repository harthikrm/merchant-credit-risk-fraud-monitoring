import pandas as pd
import numpy as np

# Load raw and basic spine manually to find exactly ~3% rate
merchants = pd.read_csv("data/raw/merchants.csv", parse_dates=['onboard_date'])
txns = pd.read_csv("data/raw/transactions.csv", parse_dates=['txn_ts'])
sla = pd.read_csv("data/raw/sla_events.csv", parse_dates=['event_ts'])

start_date = pd.to_datetime("2025-01-01")
months = [start_date + pd.DateOffset(months=i, days=-1) for i in range(1, 13)]
months = [m + pd.offsets.MonthEnd(0) for m in months]

txns['txn_ts'] = pd.to_datetime(txns['txn_ts'])
sla['event_ts'] = pd.to_datetime(sla['event_ts'])

spine = []
for mid in merchants['merchant_id'].unique():
    for m in months:
        spine.append({'merchant_id': mid, 'month_end': m.date()})
df_spine = pd.DataFrame(spine)
df_spine['month_end'] = pd.to_datetime(df_spine['month_end'])

label_records = []
for m in months:
    m_end = pd.to_datetime(m.date())
    mask_t = (txns['txn_ts'] > m_end) & (txns['txn_ts'] <= m_end + pd.Timedelta(days=60))
    t_sub = txns[mask_t]
    mask_s = (sla['event_ts'] > m_end) & (sla['event_ts'] <= m_end + pd.Timedelta(days=60))
    s_sub = sla[mask_s]
    
    if len(t_sub) > 0:
        agg_t = t_sub.groupby('merchant_id').agg(
            future_chargeback=('is_chargeback', 'mean'),
            future_fraud=('is_fraud', 'mean'),
            future_decline=('status', lambda x: (x == 'DECLINED').mean()),
            future_fraud_count=('is_fraud', 'sum'),
            future_txn_count=('txn_id', 'count')
        ).reset_index()
    else:
        agg_t = pd.DataFrame(columns=['merchant_id', 'future_chargeback', 'future_fraud', 'future_decline', 'future_fraud_count', 'future_txn_count'])
        
    if len(s_sub) > 0:
        agg_s = s_sub.groupby('merchant_id').agg(future_sla_breach_rate=('breached', 'mean')).reset_index()
    else:
        agg_s = pd.DataFrame(columns=['merchant_id', 'future_sla_breach_rate'])
        
    merged = df_spine[df_spine['month_end'] == m_end][['merchant_id', 'month_end']]
    merged = merged.merge(agg_t, on='merchant_id', how='left')
    merged = merged.merge(agg_s, on='merchant_id', how='left')
    merged.fillna(0, inplace=True)
    label_records.append(merged)

labels = pd.concat(label_records, ignore_index=True)

# Test ranges
print("Testing thresholds for 2-4% rate:")
for cb in [0.02, 0.03, 0.04, 0.05, 0.06]:
    for fr in [0.03, 0.04, 0.05, 0.06, 0.08]:
        d_rate = np.where(
            (labels['future_chargeback'] >= cb) |
            (labels['future_fraud'] >= fr) |
            ((labels['future_decline'] >= 0.25) & (labels['future_fraud_count'] >= 10)),
            1, 0
        ).mean()
        if 0.02 <= d_rate <= 0.04:
            print(f"CB>={cb}, Fraud>={fr}, Dec>=0.25&FC>=10 => Default Rate: {d_rate:.4%}")
