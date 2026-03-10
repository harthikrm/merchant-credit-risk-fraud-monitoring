import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os

os.makedirs('images', exist_ok=True)

merchants = pd.read_csv("data/raw/merchants.csv")
sla = pd.read_csv("data/raw/sla_events.csv")
scored = pd.read_csv("data/processed/scored_test_set.csv")

# 1. Default rate by industry
print("Default rate by industry:")
print(scored.groupby("industry")["default_next_60d"].mean().sort_values(ascending=False))

# 2. Default rate by risk tier (sanity check)
merchants_scored = scored.merge(merchants[["merchant_id", "risk_tier_true"]], on="merchant_id", how="left")
print("\nDefault rate by tier:")
print(merchants_scored.groupby("risk_tier_true")["default_next_60d"].mean())

# 3. Feature distributions
fig, axes = plt.subplots(2, 3, figsize=(15, 8))
features = ["fraud_rate_30d", "chargeback_rate_30d", "decline_rate_30d", 
            "sla_breach_rate_60d", "z_score_fraud", "risk_interaction"]
for ax, feat in zip(axes.flatten(), features):
    scored[feat].hist(bins=40, ax=ax)
    ax.set_title(feat)
plt.tight_layout()
plt.savefig("images/feature_distributions.png")
print("Saved feature_distributions.png")

# 4. Class imbalance
print(f"\nDefault rate: {scored['default_next_60d'].mean():.2%}")
print(f"Class distribution:\n{scored['default_next_60d'].value_counts()}")

# 5. Correlation with target
corr = scored[features + ["default_next_60d"]].corr()["default_next_60d"].sort_values(ascending=False)
print("\nFeature correlations with default:")
print(corr)
