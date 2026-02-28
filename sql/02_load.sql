-- Execute via psql:
-- psql -d risk_db -f sql/02_load.sql

\COPY merchants FROM 'data/raw/merchants.csv' WITH (FORMAT csv, HEADER true);
\COPY transactions FROM 'data/raw/transactions.csv' WITH (FORMAT csv, HEADER true);
\COPY sla_events FROM 'data/raw/sla_events.csv' WITH (FORMAT csv, HEADER true);
