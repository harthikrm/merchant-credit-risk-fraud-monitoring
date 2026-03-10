import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

os.makedirs("images", exist_ok=True)

df = pd.read_csv("data/processed/final_scored_portfolio.csv")

print(f"Total merchants scored: {df['merchant_id'].nunique()}")
print(f"Alert rate: {df['alert_flag'].mean():.2%}")
print(f"Defaults captured: {df[(df['alert_flag']==1) & (df['default_next_60d']==1)].shape[0]}")
print(f"Recall: {df[(df['alert_flag']==1) & (df['default_next_60d']==1)].shape[0] / df['default_next_60d'].sum():.2%}")

# Cost analysis
tp = ((df['alert_flag']==1) & (df['default_next_60d']==1)).sum()
fp = ((df['alert_flag']==1) & (df['default_next_60d']==0)).sum()
fn = ((df['alert_flag']==0) & (df['default_next_60d']==1)).sum()

print(f"\nTP: {tp} | FP: {fp} | FN: {fn}")
print(f"Cost avoided (FN*10): ${fn*10:,}")
print(f"Review cost (FP*1): ${fp:,}")
print(f"Net benefit: ${(tp*2) - fp - (fn*10):,}")

# Risk queue top merchants bar chart
# Assuming final_scored_portfolio.csv has 'merchant_id' and 'challenger_score' or 'risk_score'
score_col = 'risk_score' if 'risk_score' in df.columns else 'challenger_score' if 'challenger_score' in df.columns else 'score'
if score_col in df.columns:
    top_merchants = df.nlargest(10, score_col)
    plt.figure(figsize=(10, 6))
    plt.barh(top_merchants['merchant_id'].astype(str), top_merchants[score_col], color='darkred')
    plt.xlabel('Score')
    plt.ylabel('Merchant ID')
    plt.title('Risk Queue: Top 10 High-Risk Merchants')
    plt.gca().invert_yaxis()
    plt.tight_layout()
    plt.savefig('images/risk_queue.png')
    print("Saved images/risk_queue.png")
