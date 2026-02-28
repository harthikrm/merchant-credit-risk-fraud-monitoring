import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import os

# Configuration
NUM_MERCHANTS = 1200
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime(2025, 12, 31)
TOTAL_DAYS = (END_DATE - START_DATE).days
TARGET_TXN_COUNT = 400000

# File paths
RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

def generate_merchants():
    """Step A: Generate Merchants"""
    np.random.seed(42)  # For reproducibility
    
    # 1. Tier Distribution
    tiers = np.random.choice(
        ['LOW', 'MED', 'HIGH'], 
        size=NUM_MERCHANTS, 
        p=[0.70, 0.25, 0.05]
    )
    
    # 2. Industry Distribution
    industries = np.random.choice(
        ['Retail', 'Travel', 'Subscription', 'Marketplace', 'DigitalGoods'],
        size=NUM_MERCHANTS,
        p=[0.4, 0.1, 0.2, 0.15, 0.15]
    )
    
    # 3. States (Pick 20 to keep clean)
    states_pool = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI', 
                   'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI']
    states = np.random.choice(states_pool, size=NUM_MERCHANTS)
    
    # 4. Onboard Date
    onboard_dates = []
    for _ in range(NUM_MERCHANTS):
        # Random between start_date-180d and start_date+30d
        offset_days = np.random.randint(-180, 30)
        onboard = START_DATE + timedelta(days=offset_days)
        onboard_dates.append(onboard.date())
    
    # 5. Build DataFrame
    merchants = pd.DataFrame({
        'merchant_id': [str(uuid.uuid4()) for _ in range(NUM_MERCHANTS)],
        'industry': industries,
        'state': states,
        'onboard_date': onboard_dates,
        'risk_tier_true': tiers
    })
    
    return merchants

def allocate_txns(merchants):
    """Step B: Allocate transaction counts per merchant"""
    # Base rates per day
    base_rate = {'LOW': 25, 'MED': 45, 'HIGH': 80} # Midpoints of ranges
    ind_mult = {
        'Retail': 1.0, 
        'Travel': 0.7, 
        'Subscription': 1.2, 
        'Marketplace': 1.1, 
        'DigitalGoods': 1.3
    }
    
    raw_counts = []
    for _, row in merchants.iterrows():
        # Active days based on onboard date (at most 365)
        onboard = datetime.combine(row['onboard_date'], datetime.min.time())
        est_active_days = min(TOTAL_DAYS, max(1, (END_DATE - max(START_DATE, onboard)).days))
        
        rate = base_rate[row['risk_tier_true']] * ind_mult[row['industry']]
        raw_counts.append(rate * est_active_days)
    
    # Normalize to 400k
    total_raw = sum(raw_counts)
    scale = TARGET_TXN_COUNT / total_raw
    final_counts = [int(c * scale) for c in raw_counts]
    
    merchants['txn_count'] = final_counts
    return merchants

def generate_transactions(merchants):
    """Steps C, D, E: Generate transactions"""
    print("Generating transactions...")
    txns = []
    
    # Pre-define tier rules
    rates = {
        'LOW': {'fraud': (0.003, 0.008), 'cb': (0.0015, 0.004), 'dec': (0.05, 0.10)},
        'MED': {'fraud': (0.008, 0.015), 'cb': (0.004, 0.008), 'dec': (0.10, 0.16)},
        'HIGH': {'fraud': (0.015, 0.030), 'cb': (0.008, 0.016), 'dec': (0.16, 0.25)}
    }
    
    amounts = {
        'Retail': (45, 10), 'Travel': (220, 80), 'Subscription': (25, 5), 
        'DigitalGoods': (15, 5), 'Marketplace': (80, 40)
    }
    
    # Global shock/seasonality
    for idx, m in merchants.iterrows():
        tier = m['risk_tier_true']
        ind = m['industry']
        num_txns = m['txn_count']
        
        # Determine actual probabilities for this specific merchant within tier range
        p_fraud_base = np.random.uniform(rates[tier]['fraud'][0], rates[tier]['fraud'][1])
        p_cb_base = np.random.uniform(rates[tier]['cb'][0], rates[tier]['cb'][1])
        p_dec_base = np.random.uniform(rates[tier]['dec'][0], rates[tier]['dec'][1])
        
        # Determine if this merchant is a "bad actor" that will experience progressive fraud buildup
        is_bad_actor = (np.random.random() < 0.15 and tier in ['HIGH', 'MED'])
        buildup_start_month = np.random.randint(2, 8) if is_bad_actor else 99
        
        # Active period
        onboard = datetime.combine(m['onboard_date'], datetime.min.time())
        actual_start = max(START_DATE, onboard)
        days_active = (END_DATE - actual_start).days
        if days_active <= 0: continue
        
        # Generate timestamps
        random_days = np.random.randint(0, days_active, size=num_txns)
        # Time of day weights (heavier 10am-9pm)
        # 0-5 (6h), 6-9 (4h), 10-20 (11h), 21-23 (3h)
        # Weights: 0.01*6 + 0.03*4 + 0.07*11 + 0.05*3 = 0.06 + 0.12 + 0.77 + 0.15 = 1.10 (Fixing weights)
        # Adjusted weights sum to 1.0:
        # 6 hours * 0.01  = 0.06
        # 4 hours * 0.035 = 0.14
        # 11 hours * 0.06 = 0.66
        # 3 hours * 0.0466... -> let's just normalize
        raw_weights = [0.01]*6 + [0.03]*4 + [0.07]*11 + [0.03]*3
        weights = np.array(raw_weights)
        weights = weights / weights.sum()
        
        hours = np.random.choice(range(24), size=num_txns, p=weights)
        mins = np.random.randint(0, 60, size=num_txns)
        
        tss = [actual_start + timedelta(days=int(d), hours=int(h), minutes=int(m_)) 
               for d, h, m_ in zip(random_days, hours, mins)]
        
        # Determine amounts
        mean_amt, std_amt = amounts[ind]
        amts = np.random.normal(mean_amt, std_amt, size=num_txns)
        amts = np.maximum(amts, 1.0) # No negative or zero amounts
        
        # Create vectors
        # Determine channels
        p_ach = 0.4 if ind in ['Subscription', 'Marketplace'] else 0.05
        channels = np.random.choice(['CARD', 'ACH'], size=num_txns, p=[1-p_ach, p_ach])
        
        for i in range(num_txns):
            ts = tss[i]
            amt = amts[i]
            
            # Month specific overrides
            month = ts.month
            p_dec = p_dec_base
            p_fraud = p_fraud_base
            
            if month == 6 and tier in ['MED', 'HIGH']:
                p_fraud += 0.008  # Fraud shock
                p_dec += 0.04     # Decline shock
                
            # Progressive Buildup (Simulates accelerating risk)
            p_cb_actual = p_cb_base
            if is_bad_actor and month >= buildup_start_month:
                months_active = month - buildup_start_month + 1
                p_fraud += 0.010 * months_active
                p_cb_actual += 0.005 * months_active
                p_dec += 0.020 * months_active
                
            # Status
            status = 'DECLINED' if np.random.random() < p_dec else 'APPROVED'
            
            # Fraud
            is_fraud = 1 if np.random.random() < p_fraud else 0
            
            # Chargeback
            is_chargeback = 0
            if status == 'APPROVED':
                if is_fraud == 1:
                    # High chance of CB if it was actual fraud that got approved
                    is_chargeback = 1 if np.random.random() < 0.6 else 0
                else:
                    is_chargeback = 1 if np.random.random() < p_cb_actual else 0
                    
            txns.append({
                'txn_id': str(uuid.uuid4()),
                'merchant_id': m['merchant_id'],
                'txn_ts': ts,
                'amount': round(amt, 2),
                'channel': channels[i],
                'status': status,
                'is_fraud': is_fraud,
                'is_chargeback': is_chargeback
            })
            
    return pd.DataFrame(txns)

def generate_sla_events(merchants, num_txns):
    """Step F: SLA events"""
    print("Generating SLA events...")
    target_count = int(num_txns * 0.1)  # ~40k
    
    events = []
    
    tier_rate_mult = {'LOW': 1, 'MED': 3, 'HIGH': 6}
    
    # weights for events
    weights = merchants['risk_tier_true'].map(tier_rate_mult).values
    weights = weights / sum(weights)
    
    event_merchants = np.random.choice(merchants['merchant_id'], size=target_count, p=weights)
    
    breach_rates = {'LOW': 0.03, 'MED': 0.06, 'HIGH': 0.12}
    
    for mid in event_merchants:
        m = merchants[merchants['merchant_id'] == mid].iloc[0]
        tier = m['risk_tier_true']
        
        # Time
        onboard = datetime.combine(m['onboard_date'], datetime.min.time())
        actual_start = max(START_DATE, onboard)
        days_active = (END_DATE - actual_start).days
        if days_active <= 0: continue
        
        ts = actual_start + timedelta(days=np.random.randint(0, days_active), 
                                      hours=np.random.randint(0, 24))
        
        etype = np.random.choice(['DISPUTE_RESPONSE_DELAY', 'SETTLEMENT_DELAY', 'DOC_REQUEST_MISSED'])
        breached = 1 if np.random.random() < breach_rates[tier] else 0
        
        events.append({
            'event_id': str(uuid.uuid4()),
            'merchant_id': mid,
            'event_ts': ts,
            'event_type': etype,
            'breached': breached
        })
        
    return pd.DataFrame(events)

def main():
    print("1. Generating Merchants...")
    merchants = generate_merchants()
    merchants = allocate_txns(merchants)
    
    print(f"Target txns: {merchants['txn_count'].sum()}")
    
    print("2. Generating Transactions...")
    transactions = generate_transactions(merchants)
    
    print(f"Actual txns: {len(transactions)}")
    
    print("3. Generating SLA Events...")
    sla = generate_sla_events(merchants, len(transactions))
    
    # Drop temp column
    merchants.drop('txn_count', axis=1, inplace=True)
    
    print("Saving to CSV...")
    merchants.to_csv(f"{RAW_DIR}/merchants.csv", index=False)
    transactions.to_csv(f"{RAW_DIR}/transactions.csv", index=False)
    sla.to_csv(f"{RAW_DIR}/sla_events.csv", index=False)
    print("Done!")

if __name__ == "__main__":
    main()
