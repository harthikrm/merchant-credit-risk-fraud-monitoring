# Merchant Credit Risk + Fraud Monitoring (Fiserv-aligned)
An end-to-end synthetic data pipeline and ML modeling project demonstrating risk scoring and portfolio monitoring for payment processing.

## Project Outcome
*   **Data Generation Plan:** Python script to synthesize event-log data for 1200 merchants, 400K transactions, and 40K SLA events. Showcases complex behavior patterns like fraud rate changes, industry seasonality, and causal links between chargebacks/SLA breaches and merchant default risk.
*   **SQL Feature Pipeline:** Built an analytics engineering pipeline implementing feature engineering at the merchant-month grain. Extracts past-window features (30-day volume, risk metrics, trends) and applies strict target leakage checks (no future knowledge in features). Shows rigorous data tests covering orphan records and anomalies.
*   **Machine Learning (Champion vs. Challenger):** Trained a Logistic Regression (Champion) and an XGBoost (Challenger) model. Optimized on PR-AUC. Implemented custom cost thresholds matching a realistic business environment scenarios (evaluating the cost of missing a true default against investigating a false positive).
*   **Rules Engine Framework:** Integrated ML predictive scores with hard-coded risk rules targeting sudden spikes in volume, sustained high chargebacks, and repeated SLA delays, ranking a hybrid "top priority investigations" queue.

## Folder Structure
```
credit-risk-monitoring/
├── README.md                           # Documentation
├── data/                               
│   ├── raw/                            # Synthesized CVS
│   └── processed/                      # Model outputs
├── sql/
│   ├── 01_schema.sql                   # DDL
│   ├── 02_load.sql                     # Staging
│   ├── 03_feature_views.sql            # Feature layer
│   ├── 04_data_tests.sql               # Anomaly checks
│   ├── 05_model_dataset.sql            # Core dataset
│   └── 06_marts.sql                    # Reporting aggregations
├── src/
│   ├── generate_data.py                # Synthetic generation script
│   ├── train.py                        # Model trainer
│   └── score.py                        # Rule engine processor
├── notebooks/                          
│   ├── 01_eda.py                       
│   ├── 02_modeling_champion_vs_challenger.py
│   └── 03_thresholding_cost_tradeoff.py
└── powerbi/
    └── risk_portfolio.pbix             # Dashboard placeholder
```

## Schema & Design
### Tables
1.  **`merchants`**: Hidden driver `risk_tier_true` used for generative logic, simulating behavior.
2.  **`transactions` (Event Log)**: Simulates the core of payments operations (amount, channel, status, chargeback flags).
3.  **`sla_events` (Event Log)**: Captures secondary operations delays corresponding to non-payment risk behavior.
4.  **`outcomes_monthly` (Labels)**: The ML objective table calculating whether a merchant defaults in the upcoming 60-day window (`default_next_60d`).

## How To Run Locally
1. Activate a Python `venv` and install the requirements file.
2. Run `src/generate_data.py` to synthesize the `data/raw/` CSV records.
3. Apply the SQL scripts inside the `sql/` directory to build the analytics structures in Postgres.
4. Run `src/train.py` (which mirrors the SQL logics in Pandas for ML execution) to generate model predictors, evaluate on PR-AUC, and write to `scored_test_set.csv`.
5. Run `src/score.py` to calculate thresholding profits and generate a high-priority risk investigation queue.

## Highlighted KPIs / Capabilities Focus
-   **Evaluation Strategy**: Precision@K (evaluating the top N-percent of the portfolio).
-   **Class Imbalance**: Enforced heavily imbalanced real-world distributions (fraud roughly 0.5-2%, overall default ~1-5%).
-   **Data Validation suite**: Ensures metric integrity.

## Key Resume Achievements
*   "Built merchant risk scoring system using SQL feature engineering + Python modeling; evaluated champion/challenger models and optimized thresholds for imbalanced fraud outcomes."
*   "Designed event-log portfolio monitoring framework (transactions + SLA events) with data validation suite to ensure metric integrity and leakage-free modeling."
*   "Delivered Power BI portfolio dashboard and investigation queue for high-risk merchants using risk scores + rule-based alerts."
