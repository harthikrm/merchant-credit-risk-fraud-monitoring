import pandas as pd
import numpy as np
import datetime
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import precision_recall_curve, auc, roc_auc_score, average_precision_score
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
import xgboost as xgb
import os

# Create processed directory
os.makedirs("data/processed", exist_ok=True)

def load_data():
    """Load from CSV (Mocking the SQL output for Python Training)"""
    print("Loading data... (Normally from SQL v_model_dataset)")
    # Since we can't easily hook up to a local Postgres right now, we will mock the v_model_dataset features
    # but the SQL layer exists in the project. For the sake of this notebook, we'll recreate the feature logic 
    # directly using the raw files so the script works standalone.
    
    # Alternatively, in a real setup we run:
    # df = pd.read_sql("SELECT * FROM v_model_dataset", con)
    
    # **Standalone Feature Generation for Python Model Run**
    merchants = pd.read_csv("data/raw/merchants.csv", parse_dates=['onboard_date'])
    txns = pd.read_csv("data/raw/transactions.csv", parse_dates=['txn_ts'])
    sla = pd.read_csv("data/raw/sla_events.csv", parse_dates=['event_ts'])
    
    # Build a simple spine
    start_date = pd.to_datetime("2025-01-01")
    months = [start_date + pd.DateOffset(months=i, days=-1) for i in range(1, 13)]
    months = [m + pd.offsets.MonthEnd(0) for m in months]
    
    spine = []
    for mid in merchants['merchant_id'].unique():
        for m in months:
            spine.append({'merchant_id': mid, 'month_end': m.date()})
    df_spine = pd.DataFrame(spine)
    df_spine['month_end'] = pd.to_datetime(df_spine['month_end'])
    
    features = []
    
    # We will approximate the dataset here to run the ML portion standalone
    print("Building features for modeling...")
    txns['txn_ts'] = pd.to_datetime(txns['txn_ts'])
    txns['month_end'] = txns['txn_ts'] + pd.offsets.MonthEnd(0)
    
    sla['event_ts'] = pd.to_datetime(sla['event_ts'])
    sla['month_end'] = sla['event_ts'] + pd.offsets.MonthEnd(0)
    
    # 30d txns
    f_txns = txns.groupby(['merchant_id', 'month_end']).agg(
        txn_count_30d=('txn_id', 'count'),
        gmv_30d=('amount', lambda x: x[txns.loc[x.index, 'status'] == 'APPROVED'].sum()),
        decline_rate_30d=('status', lambda x: (x == 'DECLINED').mean()),
        fraud_rate_30d=('is_fraud', 'mean'),
        chargeback_rate_30d=('is_chargeback', 'mean')
    ).reset_index()
    
    # 60d SLA
    f_sla = sla.groupby(['merchant_id', 'month_end']).agg(
        sla_events_60d=('event_id', 'count'),
        sla_breach_rate_60d=('breached', 'mean')
    ).reset_index()
    
    # Future labels correctly mapped for each month
    label_records = []
    
    for m in months:
        m_end = pd.to_datetime(m.date())
        # txns in (m_end, m_end + 60d]
        mask_t = (txns['txn_ts'] > m_end) & (txns['txn_ts'] <= m_end + pd.Timedelta(days=60))
        t_sub = txns[mask_t]
        
        # sla in (m_end, m_end + 60d]
        mask_s = (sla['event_ts'] > m_end) & (sla['event_ts'] <= m_end + pd.Timedelta(days=60))
        s_sub = sla[mask_s]
        
        # Aggregate txns
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
            
        # Aggregate SLA
        if len(s_sub) > 0:
            agg_s = s_sub.groupby('merchant_id').agg(
                future_sla_breach_rate=('breached', 'mean')
            ).reset_index()
        else:
            agg_s = pd.DataFrame(columns=['merchant_id', 'future_sla_breach_rate'])
            
        # Merge
        merged = df_spine[df_spine['month_end'] == m_end][['merchant_id', 'month_end']]
        merged = merged.merge(agg_t, on='merchant_id', how='left')
        merged = merged.merge(agg_s, on='merchant_id', how='left')
        
        merged.fillna(0, inplace=True)
        
        merged['default_next_60d'] = np.where(
            (merged['future_chargeback'] >= 0.06) |
            (merged['future_fraud'] >= 0.06) |
            ((merged['future_decline'] >= 0.25) & (merged['future_fraud_count'] >= 10)) |
            ((merged['future_sla_breach_rate'] >= 0.20) & (merged['future_txn_count'] >= 200)),
            1, 0
        )
        
        label_records.append(merged[['merchant_id', 'month_end', 'default_next_60d', 'future_chargeback', 'future_fraud', 'future_decline', 'future_fraud_count', 'future_sla_breach_rate', 'future_txn_count']])
        
    labels = pd.concat(label_records, ignore_index=True)
    
    print("Label condition rates:")
    print("CB>=0.012:", (labels['future_chargeback'] >= 0.012).mean())
    print("Fraud>=0.018:", (labels['future_fraud'] >= 0.018).mean())
    print("Decline>=0.2 & Fraud>=5:", ((labels['future_decline'] >= 0.20) & (labels['future_fraud_count'] >= 5)).mean())
    print("SLA>=0.1 & txns>=200:", ((labels['future_sla_breach_rate'] >= 0.10) & (labels['future_txn_count'] >= 200)).mean())
    
    # Merge
    master = df_spine.merge(merchants[['merchant_id', 'industry']], on='merchant_id', how='left')
    master = master.merge(f_txns, on=['merchant_id', 'month_end'], how='left')
    master = master.merge(f_sla, on=['merchant_id', 'month_end'], how='left')
    master = master.merge(labels[['merchant_id', 'month_end', 'default_next_60d']], on=['merchant_id', 'month_end'], how='left')
    
    master.fillna({
        'txn_count_30d': 0, 'gmv_30d': 0, 'decline_rate_30d': 0,
        'fraud_rate_30d': 0, 'chargeback_rate_30d': 0, 'sla_events_60d': 0,
        'sla_breach_rate_60d': 0, 'default_next_60d': 0
    }, inplace=True)
    
    master['default_next_60d'] = master['default_next_60d'].astype(int)
    print(f"Dataset shape: {master.shape}. Default rate: {master['default_next_60d'].mean():.4f}")
    
    # Advanced Features
    print("Building advanced features...")
    advanced_records = []
    
    for m in months:
        m_end = pd.to_datetime(m.date())
        # Past 60d mask for features
        mask_60 = (txns['txn_ts'] > (m_end - pd.Timedelta(days=60))) & (txns['txn_ts'] <= m_end)
        t_60 = txns[mask_60].copy()
        if len(t_60) > 0:
            agg_60 = t_60.groupby('merchant_id').agg(
                fraud_rate_60d=('is_fraud', 'mean'),
                chargeback_rate_60d=('is_chargeback', 'mean')
            ).reset_index()
        else:
            agg_60 = pd.DataFrame(columns=['merchant_id', 'fraud_rate_60d', 'chargeback_rate_60d'])
            
        # Past 90d mask
        mask_90 = (txns['txn_ts'] > (m_end - pd.Timedelta(days=90))) & (txns['txn_ts'] <= m_end)
        t_90 = txns[mask_90].copy()
        
        if len(t_90) > 0:
            agg_90 = t_90.groupby('merchant_id').agg(
                cumulative_fraud_90d=('is_fraud', 'sum'),
                amount_volatility_90d=('amount', 'std'),
                decline_rate_90d=('status', lambda x: (x == 'DECLINED').mean())
            ).reset_index()
            
            # Max daily decline spike over 90d
            t_90['date'] = t_90['txn_ts'].dt.date
            daily = t_90.groupby(['merchant_id', 'date']).agg(
                daily_dec=('status', lambda x: (x == 'DECLINED').mean()),
                daily_txns=('txn_id', 'count')
            ).reset_index()
            # only consider days with >= 3 txns to avoid noise
            daily = daily[daily['daily_txns'] >= 3]
            if len(daily) > 0:
                max_dec = daily.groupby('merchant_id').agg(
                    rolling_max_decline_spike=('daily_dec', 'max')
                ).reset_index()
            else:
                max_dec = pd.DataFrame(columns=['merchant_id', 'rolling_max_decline_spike'])
                
            agg_90 = agg_90.merge(max_dec, on='merchant_id', how='left')
        else:
            agg_90 = pd.DataFrame(columns=['merchant_id', 'cumulative_fraud_90d', 'amount_volatility_90d', 'decline_rate_90d', 'rolling_max_decline_spike'])
            
        # Historical All-Time
        mask_hist = (txns['txn_ts'] <= m_end)
        t_hist = txns[mask_hist]
        if len(t_hist) > 0:
            agg_hist = t_hist.groupby('merchant_id').agg(
                historical_fraud_rate=('is_fraud', 'mean'),
                historical_fraud_std=('is_fraud', 'std')
            ).reset_index()
        else:
            agg_hist = pd.DataFrame(columns=['merchant_id', 'historical_fraud_rate', 'historical_fraud_std'])
            
        merged_90 = df_spine[df_spine['month_end'] == m_end][['merchant_id', 'month_end']]
        merged_90 = merged_90.merge(agg_60, on='merchant_id', how='left')
        merged_90 = merged_90.merge(agg_90, on='merchant_id', how='left')
        merged_90 = merged_90.merge(agg_hist, on='merchant_id', how='left')
        
        advanced_records.append(merged_90)
        
    adv_features = pd.concat(advanced_records, ignore_index=True)
    
    # Merge into master
    master = master.merge(adv_features, on=['merchant_id', 'month_end'], how='left')
    
    # Calculate accelerations (requires sorting)
    master = master.sort_values(['merchant_id', 'month_end'])
    master['prev_fraud_rate'] = master.groupby('merchant_id')['fraud_rate_30d'].shift(1).fillna(0)
    master['prev_cb_rate'] = master.groupby('merchant_id')['chargeback_rate_30d'].shift(1).fillna(0)
    
    master['fraud_acceleration'] = master['fraud_rate_30d'] - master['prev_fraud_rate']
    master['chargeback_acceleration'] = master['chargeback_rate_30d'] - master['prev_cb_rate']
    
    # Z-score
    master['z_score_fraud'] = np.where(master['historical_fraud_std'] > 0, 
                                      (master['fraud_rate_30d'] - master['historical_fraud_rate']) / master['historical_fraud_std'], 
                                      0)
                                      
    # Risk Interactions
    master['risk_interaction'] = master['fraud_rate_30d'] * master['chargeback_rate_30d']
    master['decline_x_fraud'] = master['decline_rate_30d'] * master['fraud_rate_30d']
    
    master.fillna({
        'cumulative_fraud_90d': 0, 'amount_volatility_90d': 0, 'rolling_max_decline_spike': 0,
        'decline_rate_90d': 0, 'fraud_rate_60d': 0, 'chargeback_rate_60d': 0,
        'fraud_acceleration': 0, 'chargeback_acceleration': 0, 'z_score_fraud': 0,
        'risk_interaction': 0, 'decline_x_fraud': 0
    }, inplace=True)
    
    # OOT Split (Out of Time)
    master['month'] = pd.to_datetime(master['month_end']).dt.month
    
    train = master[master['month'] <= 9].copy()
    test = master[master['month'] > 9].copy()
    
    return train, test, master

def evaluate_model(y_true, y_pred_prob, name):
    precision, recall, _ = precision_recall_curve(y_true, y_pred_prob)
    pr_auc = auc(recall, precision)
    roc = roc_auc_score(y_true, y_pred_prob)
    
    # Precision@Top K%
    k_pct = 0.05
    k = int(len(y_pred_prob) * k_pct)
    top_k_indices = np.argsort(y_pred_prob)[::-1][:k]
    top_k_true = y_true.iloc[top_k_indices] if isinstance(y_true, pd.Series) else y_true[top_k_indices]
    prec_at_k = top_k_true.mean()
    
    print(f"=== {name} ===")
    print(f"PR-AUC: {pr_auc:.4f}")
    print(f"ROC-AUC: {roc:.4f}")
    print(f"Precision@5%: {prec_at_k:.4f}")
    print("------------------")
    return pr_auc, prec_at_k

def train_champion_challenger():
    train, test, master = load_data()
    
    features = ['txn_count_30d', 'gmv_30d', 'decline_rate_30d', 
                'fraud_rate_30d', 'chargeback_rate_30d', 'sla_events_60d', 'sla_breach_rate_60d',
                'fraud_rate_60d', 'chargeback_rate_60d',
                'cumulative_fraud_90d', 'fraud_acceleration', 'chargeback_acceleration',
                'amount_volatility_90d', 'rolling_max_decline_spike', 'z_score_fraud',
                'decline_rate_90d', 'risk_interaction', 'decline_x_fraud']
    
    X_train = train[features]
    y_train = train['default_next_60d']
    X_test = test[features]
    y_test = test['default_next_60d']
    
    # Champion: Logistic Regression
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)
    
    champion = LogisticRegression(class_weight='balanced', random_state=42)
    champion.fit(X_train_s, y_train)
    
    champ_preds = champion.predict_proba(X_test_s)[:, 1]
    evaluate_model(y_test, champ_preds, "Champion (Logistic Regression)")
    
    # Challenger: XGBoost
    challenger = xgb.XGBClassifier(
        n_estimators=100, 
        learning_rate=0.1,
        max_depth=4,
        scale_pos_weight=sum(y_train==0)/sum(y_train==1),
        random_state=42
    )
    challenger.fit(X_train, y_train)
    
    chall_preds = challenger.predict_proba(X_test)[:, 1]
    evaluate_model(y_test, chall_preds, "Challenger (XGBoost)")
    
    # Feature Imp
    imp = pd.DataFrame({'feature': features, 'importance': challenger.feature_importances_})
    imp = imp.sort_values('importance', ascending=False)
    print("XGB Feature Importance:")
    print(imp)
    
    # Save the processed predictions for Thresholding / Rules evaluation
    test['champ_score'] = champ_preds
    test['challenger_score'] = chall_preds
    test.to_csv("data/processed/scored_test_set.csv", index=False)
    print("Exported data/processed/scored_test_set.csv")

if __name__ == "__main__":
    train_champion_challenger()
