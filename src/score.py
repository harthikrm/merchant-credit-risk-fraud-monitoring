import pandas as pd
import numpy as np

def run_scoring_system():
    print("Loading test set scores...")
    df = pd.read_csv("data/processed/scored_test_set.csv")
    
    # 1. Rules Component
    # If chargeback_rate > X and gmv > Y -> high risk
    # If sla_breach_rate > X -> medium/high risk
    # If fraud_rate spikes MoM > X -> review (simulated here as just high fraud rate for simplicity in isolated dataset)
    
    def apply_rules(row):
        score = 0
        reasons = []
        
        # Rule 1: High Chargeback & Volume
        if row['chargeback_rate_30d'] > 0.01 and row['gmv_30d'] > 1000:
            score += 0.4
            reasons.append("High CB & Vol")
            
        # Rule 2: Sustained SLA breaches
        if row['sla_breach_rate_60d'] > 0.08:
            score += 0.3
            reasons.append("SLA Breach")
            
        # Rule 3: High Fraud Rate
        if row['fraud_rate_30d'] > 0.015:
            score += 0.4
            reasons.append("High Fraud Rate")
            
        # Rule 4: Volume Spike (proxy via High Volume vs Avg)
        if row['txn_count_30d'] > 100: 
            # In real system we compare against rolling avg, but simplified here
            # to just raw high volume for risk bump
            score += 0.1
            reasons.append("High Vol")
            
        return min(score, 1.0), " | ".join(reasons)
        
    rules_out = df.apply(apply_rules, axis=1)
    df['rule_score'] = [x[0] for x in rules_out]
    df['rule_reason'] = [x[1] for x in rules_out]
    
    # 2. Hybrid Score (Model + Rules Override)
    # Using Challenger (XGBoost) as base model based on expected higher PR-AUC
    # Hybrid strategy: max(model_score, rule_score)
    df['final_risk_score'] = df[['challenger_score', 'rule_score']].max(axis=1)
    
    # 3. Cost-based thresholding
    # FN Cost (missing a default) = 10, FP Cost (reviewing a good merchant) = 1
    benefit_tp = 2  # caught early
    cost_fp = 1
    cost_fn = 10
    
    best_threshold = 0
    best_profit = -np.inf
    
    thresholds = np.linspace(0, 1, 100)
    for t in thresholds:
        preds = (df['final_risk_score'] >= t).astype(int)
        
        tp = ((preds == 1) & (df['default_next_60d'] == 1)).sum()
        fp = ((preds == 1) & (df['default_next_60d'] == 0)).sum()
        fn = ((preds == 0) & (df['default_next_60d'] == 1)).sum()
        
        profit = (tp * benefit_tp) - (fp * cost_fp) - (fn * cost_fn)
        if profit > best_profit:
            best_profit = profit
            best_threshold = t
            
    print(f"\n--- Cost-Based Optimization ---")
    print(f"Optimal Threshold: {best_threshold:.2f}")
    print(f"Max Profit Score: {best_profit}")
    
    # Apply Threshold
    df['alert_flag'] = (df['final_risk_score'] >= best_threshold).astype(int)
    
    print(f"\nAlert Rate: {df['alert_flag'].mean():.2%}")
    print(f"Capture Rate (Recall): {df[(df['alert_flag']==1) & (df['default_next_60d']==1)].shape[0] / df[df['default_next_60d']==1].shape[0]:.2%}")
    
    # Output final scored queue
    queue = df[df['alert_flag'] == 1].sort_values('final_risk_score', ascending=False)
    print(f"\nTop 5 in Risk Queue (from {len(queue)} total alerts):")
    print(queue[['merchant_id', 'final_risk_score', 'rule_reason', 'default_next_60d']].head())
    
    df.to_csv("data/processed/final_scored_portfolio.csv", index=False)
    print("\nSaved to data/processed/final_scored_portfolio.csv")

if __name__ == "__main__":
    run_scoring_system()
