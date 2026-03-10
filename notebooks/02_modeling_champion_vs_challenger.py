# This script summarizes champion vs challenger results
# Full training logic is in src/train.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.metrics import precision_recall_curve, auc, roc_auc_score
import os

os.makedirs("images", exist_ok=True)

df = pd.read_csv("data/processed/scored_test_set.csv")

y = df["default_next_60d"]

plt.figure(figsize=(8, 6))

for model, col in [("Champion (Logistic Regression)", "champ_score"), 
                   ("Challenger (XGBoost)", "challenger_score")]:
    precision, recall, _ = precision_recall_curve(y, df[col])
    pr_auc = auc(recall, precision)
    roc = roc_auc_score(y, df[col])
    k = int(len(df) * 0.05)
    top_k = df.nlargest(k, col)
    prec_at_k = top_k["default_next_60d"].mean()
    print(f"{model}: PR-AUC={pr_auc:.4f} | ROC-AUC={roc:.4f} | Precision@5%={prec_at_k:.4f}")
    
    plt.plot(recall, precision, label=f"{model} (AUC = {pr_auc:.4f})")

plt.xlabel("Recall")
plt.ylabel("Precision")
plt.title("PR Curve: Champion vs Challenger")
plt.legend()
plt.tight_layout()
plt.savefig("images/pr_curve.png")
print("Saved images/pr_curve.png")
