-- ============================================================
-- Filename:     NorthAxis_Bank_plc.sql
-- Author:       Amudipe Ayomide
-- Date:         2026-04-08
-- ============================================================
-- Description:
--   End-to-end transaction fraud detection and risk intelligence
--   analysis for NorthAxis Bank plc (Operation Clearwater).
--   Covers database setup, data import, and 5 analytical tasks
--   investigating $2.3M+ in suspicious outflows across Q3 2024.
-- ============================================================
-- Project:      Operation Clearwater — Risk Intelligence Division
-- Reference:    NAB-RI-2024-09
-- Data period:  Jan – Sep 2024
-- Tool:         MySQL Workbench
-- ============================================================
-- Tables used:
--   fact_transactions  (transaction_key, transaction_id,
--                       customer_key, account_key, merchant_key,
--                       location_key, date_key, amount_usd,
--                       is_flagged, is_off_hours, channel, ...)
--   dim_customer       (customer_key, customer_id, full_name,
--                       country, kyc_status, customer_segment, ...)
--   dim_account        (account_key, account_id, account_type,
--                       balance_tier, account_status, ...)
--   dim_merchant       (merchant_key, merchant_name,
--                       merchant_category, is_shell_merchant,
--                       risk_rating, ...)
--   dim_location       (location_key, country, city, region,
--                       is_high_risk_country, ...)
--   dim_date           (date_key, full_date, month_name,
--                       quarter_number, is_weekend, ...)
-- ============================================================
-- Tasks:
--   Setup   Database creation & CSV data import
--   Task 1  Transaction Overview & Baseline KPIs
--   Task 2  Anomaly Detection & Velocity Checks
--   Task 3  Customer Risk Profiling
--   Task 4  Merchant & Channel Risk Scoring
--   Task 5  Fraud Risk Scoring Model (NTILE + weighted flags)
-- ============================================================



CREATE DATABASE `northaxis bank`;
USE `northaxis bank`;

SHOW VARIABLES LIKE 'secure_file_priv';

TRUNCATE TABLE fact_transactions; -- clears the 1000 bad rows first

SET FOREIGN_KEY_CHECKS = 0;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/fact_transactions.csv'
INTO TABLE fact_transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET FOREIGN_KEY_CHECKS = 1;

SHOW VARIABLES LIKE 'secure_file_priv';

TRUNCATE TABLE dim_date;

SET FOREIGN_KEY_CHECKS = 0;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dim_date.csv'
INTO TABLE dim_date
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

SET FOREIGN_KEY_CHECKS = 1;



-- ============================================================
-- NorthAxis Bank | Operation Clearwater
-- MySQL Setup Script 
-- TASK 1: Transaction Overview & Baseline KPIs
-- ============================================================

 
-- ------------------------------------------------------------
-- Overall Summary — Total volume, value, flagged count
-- ------------------------------------------------------------
SELECT
    COUNT(*)                                        AS `total transactions`,
    ROUND(SUM(amount_usd), 2)                       AS `total value usd`,
    ROUND(AVG(amount_usd), 2)                       AS `average transaction usd`,
    ROUND(MIN(amount_usd), 2)                       AS `min transaction usd`,
    ROUND(MAX(amount_usd), 2)                       AS `max transaction usd`,
    SUM(is_flagged)                                 AS `total flagged`,
    ROUND(SUM(is_flagged) / COUNT(*) * 100, 2)      AS `flagged percentage`
FROM fact_transactions;

-- ------------------------------------------------------------
-- Monthly Trend — Volume & value by month
-- ------------------------------------------------------------
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    COUNT(*)                                        AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                     AS `average amount usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)    AS `flagged percentage`
FROM fact_transactions t
JOIN dim_date d ON t.date_key = d.date_key
GROUP BY d.year_number, d.month_number, d.month_name
ORDER BY d.year_number, d.month_number;

-- ------------------------------------------------------------
-- Channel Breakdown — Which channels drive the most volume
-- ------------------------------------------------------------
SELECT
    channel,
    COUNT(*)                                        AS `total transactions`,
    ROUND(SUM(amount_usd), 2)                       AS `total value usd`,
    ROUND(AVG(amount_usd), 2)                       AS `average amount usd`,
    SUM(is_flagged)                                 AS `flagged count`,
    ROUND(SUM(is_flagged) / COUNT(*) * 100, 2)      AS `flagged percentage`
FROM fact_transactions
GROUP BY channel
ORDER BY `total value usd` DESC;
 
 
-- ------------------------------------------------------------
-- Transaction Type Breakdown
-- ------------------------------------------------------------
SELECT
    transaction_type,
    COUNT(*)                                        AS `total transactions`,
    ROUND(SUM(amount_usd), 2)                       AS `total value usd`,
    ROUND(AVG(amount_usd), 2)                       AS `average amount usd`,
    SUM(is_flagged)                                 AS `flagged count`,
    ROUND(SUM(is_flagged) / COUNT(*) * 100, 2)      AS `flagged pct`
FROM fact_transactions
GROUP BY transaction_type
ORDER BY `total value usd` DESC;
 
 
-- ------------------------------------------------------------
-- Transaction Status Breakdown
-- ------------------------------------------------------------
SELECT
    status,
    COUNT(*)                                        AS `total transactions`,
    ROUND(SUM(amount_usd), 2)                       AS `total value usd`,
    ROUND(SUM(is_flagged) / COUNT(*) * 100, 2)      AS `flagged percentage`
FROM fact_transactions
GROUP BY status
ORDER BY `total transactions` DESC;
 
 
-- ------------------------------------------------------------
-- Hourly Distribution — When do transactions happen?
-- ------------------------------------------------------------
SELECT
    transaction_hour,
    COUNT(*)                                        AS `total transactions`,
    ROUND(SUM(amount_usd), 2)                       AS `total value usd`,
    SUM(is_flagged)                                 AS `flagged count`,
    ROUND(SUM(is_flagged) / COUNT(*) * 100, 2)      AS `flagged percentage`
FROM fact_transactions
GROUP BY transaction_hour
ORDER BY transaction_hour;

-- ------------------------------------------------------------
-- Customer Segment Breakdown
-- How much activity comes from Retail vs SME vs Corporate?
-- ------------------------------------------------------------
SELECT
    c.customer_segment,
    COUNT(*)                                        AS `total transactions`,
    COUNT(DISTINCT t.customer_key)                  AS `unique customers`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                     AS `average amount usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)    AS `flagged percentage`
FROM fact_transactions t
JOIN dim_customer c ON t.customer_key = c.customer_key
GROUP BY c.customer_segment
ORDER BY `total value usd` DESC;


-- ------------------------------------------------------------
-- Weekend vs Weekday Patterns
-- Do fraudsters prefer weekends when oversight is lower?
-- ------------------------------------------------------------
SELECT
    CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END  AS `day type`,
    COUNT(*)                                        AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                     AS `avg amount usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)    AS `flagged percentage`
FROM fact_transactions t
JOIN dim_date d ON t.date_key = d.date_key
GROUP BY `day type`
ORDER BY `day type`;


-- ------------------------------------------------------------
-- Breakdown by Day of Week
-- Which specific day has the most suspicious activity?
-- ------------------------------------------------------------
SELECT
    d.day_of_week,
    d.is_weekend,
    COUNT(*)                                        AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                     AS `average amount usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)    AS `flagged percentage`
FROM fact_transactions t
JOIN dim_date d ON t.date_key = d.date_key
GROUP BY d.day_of_week, d.is_weekend
ORDER BY FIELD(d.day_of_week, 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');


-- ------------------------------------------------------------
-- Geographic Breakdown by Country
-- Where in the world are transactions coming from?
-- ------------------------------------------------------------
SELECT
    l.country,
    l.region,
    COUNT(*)                                        AS `total transactions`,
    COUNT(DISTINCT t.customer_key)                  AS `unique customers`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                     AS `average amount usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)    AS `flagged percentage`
FROM fact_transactions t
JOIN dim_location l ON t.location_key = l.location_key
GROUP BY l.country, l.region
ORDER BY `total value usd` DESC;


-- ------------------------------------------------------------
-- Geographic Breakdown by City
-- Drill down — which cities are hotspots?
-- ------------------------------------------------------------
SELECT
    l.country,
    l.city,
    COUNT(*)                                        AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value_usd`,
    ROUND(AVG(t.amount_usd), 2)                     AS `average amount usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)    AS `flagged perecentage`
FROM fact_transactions t
JOIN dim_location l ON t.location_key = l.location_key
GROUP BY l.country, l.city
ORDER BY `flagged count` DESC
LIMIT 20;


-- ------------------------------------------------------------
-- High-Risk Country Transactions
-- Transactions originating from flagged high-risk countries
-- ------------------------------------------------------------
SELECT
    l.country,
    l.city,
    COUNT(*)                                        AS `total transactions`,
    COUNT(DISTINCT t.customer_key)                  AS `unique customers`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                     AS `average amount usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)    AS `flagged percentage`
FROM fact_transactions t
JOIN dim_location l ON t.location_key = l.location_key
WHERE l.is_high_risk_country = 1
GROUP BY l.country, l.city
ORDER BY `total value usd` DESC;


-- ------------------------------------------------------------
-- High-Risk Country — Share of Total Business
-- How much of the bank's total exposure is in risky countries?
-- ------------------------------------------------------------
SELECT
    CASE WHEN l.is_high_risk_country = 1 THEN 'High Risk Country' ELSE 'Normal Country' END AS `country risk`,
    COUNT(*)                                            AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    ROUND(SUM(t.amount_usd) / SUM(SUM(t.amount_usd)) OVER () * 100, 2) AS `percentage of total value`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`
FROM fact_transactions t
JOIN dim_location l ON t.location_key = l.location_key
GROUP BY `country risk`
ORDER BY `country risk`;


-- ------------------------------------------------------------
-- Average Transaction by Account Type
-- Do certain account types attract higher-value fraud?
-- ------------------------------------------------------------
SELECT
    a.account_type,
    a.balance_tier,
    COUNT(*)                                        AS `total transactions`,
    COUNT(DISTINCT t.customer_key)                  AS `unique customers`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                     AS `average amount usd`,
    ROUND(MAX(t.amount_usd), 2)                     AS `max transaction usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)    AS `flagged percentage`
FROM fact_transactions t
JOIN dim_account a ON t.account_key = a.account_key
GROUP BY a.account_type, a.balance_tier
ORDER BY `average amount usd` DESC;

-- ============================================================
-- NorthAxis Bank | Operation Clearwater
-- TASK 2: Anomaly Detection & Velocity Checks
-- ============================================================
USE northaxis_bank;

-- ------------------------------------------------------------
-- Off-Hours Transactions (1AM - 4AM)
-- High volume at unusual hours is a key fraud signal
-- ------------------------------------------------------------
SELECT
    t.transaction_hour,
    COUNT(*)                                        AS `total transactions`,
    COUNT(DISTINCT t.customer_key)                  AS `unique customers`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                     AS `average amount usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)    AS `flagged percentage`
FROM fact_transactions t
WHERE t.is_off_hours = 1
GROUP BY t.transaction_hour
ORDER BY `flagged percentage` DESC;


-- ------------------------------------------------------------
-- Off-Hours vs Normal Hours — Side by Side Comparison
-- How different is off-hours behaviour from normal hours?
-- ------------------------------------------------------------
SELECT
    CASE WHEN is_off_hours = 1 THEN 'Off-Hours (1AM-4AM)' ELSE 'Normal Hours' END  AS `time period`,
    COUNT(*)                                        AS `total transactions`,
    COUNT(DISTINCT customer_key)                    AS `unique customers`,
    ROUND(SUM(amount_usd), 2)                       AS `total value usd`,
    ROUND(AVG(amount_usd), 2)                       AS `avg amount usd`,
    ROUND(MAX(amount_usd), 2)                       AS `max amount usd`,
    SUM(is_flagged)                                 AS `flagged count`,
    ROUND(SUM(is_flagged) / COUNT(*) * 100, 2)      AS `flagged percentage`
FROM fact_transactions
GROUP BY `time period`
ORDER BY `time period`;


-- ------------------------------------------------------------
-- Off-Hours Transactions by Channel
-- Which channel is most abused during off-hours?
-- ------------------------------------------------------------
SELECT
    channel,
    COUNT(*)                                        AS `off hours transactions`,
    ROUND(SUM(amount_usd), 2)                       AS `total value usd`,
    ROUND(AVG(amount_usd), 2)                       AS `average amount usd`,
    SUM(is_flagged)                                 AS `flagged count`,
    ROUND(SUM(is_flagged) / COUNT(*) * 100, 2)      AS `flagged percentage`
FROM fact_transactions
WHERE is_off_hours = 1
GROUP BY channel
ORDER BY `flagged percentage` DESC;


-- ------------------------------------------------------------
-- Velocity Check — Multiple transactions within 10 minutes
-- Same account making rapid repeat transactions = major red flag
-- ------------------------------------------------------------
WITH flagged_txns AS (
    SELECT
        transaction_id,
        account_key,
        customer_key,
        transaction_datetime,
        amount_usd,
        channel
    FROM fact_transactions
    WHERE is_flagged = 1
)
SELECT
    t1.account_key,
    a.account_id,
    c.full_name,
    c.customer_segment,
    t1.transaction_id                               AS `first txn`,
    t2.transaction_id                               AS `second txn`,
    t1.transaction_datetime                         AS `first txn time`,
    t2.transaction_datetime                         AS `second txn time`,
    TIMESTAMPDIFF(MINUTE, t1.transaction_datetime, t2.transaction_datetime) AS `minutes apart`,
    ROUND(t1.amount_usd, 2)                         AS `first amount`,
    ROUND(t2.amount_usd, 2)                         AS `second amount`,
    ROUND(t1.amount_usd + t2.amount_usd, 2)         AS `combined amount`,
    t1.channel
FROM flagged_txns t1
JOIN flagged_txns t2
    ON  t1.account_key = t2.account_key
    AND t2.transaction_datetime > t1.transaction_datetime
    AND TIMESTAMPDIFF(MINUTE, t1.transaction_datetime, t2.transaction_datetime) <= 10
    AND t1.transaction_id != t2.transaction_id
JOIN dim_account a  ON t1.account_key = a.account_key
JOIN dim_customer c ON t1.customer_key = c.customer_key
ORDER BY `minutes apart` ASC, `combined amount` DESC
LIMIT 50;

-- ------------------------------------------------------------
-- Velocity Summary — Accounts with the most rapid transactions
-- Ranks accounts by how many times they triggered velocity alerts
-- ------------------------------------------------------------
WITH flagged_txns AS (
    SELECT
        transaction_id,
        account_key,
        customer_key,
        transaction_datetime,
        amount_usd
    FROM fact_transactions
    WHERE is_flagged = 1
)
SELECT
    t1.account_key,
    a.account_id,
    c.full_name,
    c.customer_segment,
    COUNT(*)                                        AS `velocity hits`,
    ROUND(SUM(t1.amount_usd), 2)                    AS `total value usd`,
    ROUND(AVG(t1.amount_usd), 2)                    AS `average amount usd`,
    MAX(t1.transaction_datetime)                    AS `last seen`
FROM flagged_txns t1
JOIN flagged_txns t2
    ON  t1.account_key = t2.account_key
    AND t2.transaction_datetime > t1.transaction_datetime
    AND TIMESTAMPDIFF(MINUTE, t1.transaction_datetime, t2.transaction_datetime) <= 10
    AND t1.transaction_id != t2.transaction_id
JOIN dim_account a  ON t1.account_key = a.account_key
JOIN dim_customer c ON t1.customer_key = c.customer_key
GROUP BY t1.account_key, a.account_id, c.full_name, c.customer_segment
ORDER BY `velocity hits` DESC
LIMIT 20;


-- ------------------------------------------------------------
-- Amount Outliers — Transactions far above customer average
-- Flags transactions that are 3x higher than the customer's own average
-- ------------------------------------------------------------
WITH customer_avg AS (
    SELECT
        customer_key,
        ROUND(AVG(amount_usd), 2)    AS `avg_amount`,
        ROUND(STDDEV(amount_usd), 2) AS `stddev_amount`
    FROM fact_transactions
    GROUP BY customer_key
)
SELECT
    t.transaction_id,
    t.customer_key,
    c.full_name,
    c.customer_segment,
    t.transaction_datetime,
    t.channel,
    t.transaction_type,
    ROUND(t.amount_usd, 2)                             AS `transaction amount`,
    ca.`avg_amount`                                    AS `customer average amount`,
    ca.`stddev_amount`                                 AS `standard deviation amount`,
    ROUND(t.amount_usd / NULLIF(ca.`avg_amount`, 0), 2) AS `times above average`,
    t.is_flagged
FROM fact_transactions t
JOIN customer_avg ca ON t.customer_key = ca.customer_key
JOIN dim_customer c  ON t.customer_key = c.customer_key
WHERE t.amount_usd >= (ca.`avg_amount` + (3 * ca.`stddev_amount`))
ORDER BY `times above average` DESC
LIMIT 50;


-- ------------------------------------------------------------
-- Daily Transaction Spike Detection
-- Finds days where transaction volume was abnormally high
-- (more than 2x the daily average — potential burst fraud days)
-- ------------------------------------------------------------
WITH daily_counts AS (
    SELECT
        transaction_date,
        COUNT(*)                AS daily_txn_count,
        ROUND(SUM(amount_usd), 2) AS daily_value,
        SUM(is_flagged)         AS daily_flagged
    FROM fact_transactions
    GROUP BY transaction_date
),
stats AS (
    SELECT
        ROUND(AVG(daily_txn_count), 2)    AS avg_daily_txns,
        ROUND(STDDEV(daily_txn_count), 2) AS stddev_daily_txns
    FROM daily_counts
)
SELECT
    dc.transaction_date,
    dc.daily_txn_count,
    dc.daily_value,
    dc.daily_flagged,
    s.avg_daily_txns,
    ROUND(dc.daily_txn_count / s.avg_daily_txns, 2) AS times_above_avg
FROM daily_counts dc
CROSS JOIN stats s
WHERE dc.daily_txn_count > (s.avg_daily_txns + (2 * s.stddev_daily_txns))
ORDER BY dc.daily_txn_count DESC;


-- ------------------------------------------------------------
-- Geographic Mismatch
-- Customers transacting from a country different to their home country
-- ------------------------------------------------------------
SELECT
    t.transaction_id,
    c.customer_id,
    c.full_name,
    c.country                                       AS `home country`,
    l.country                                       AS `transaction country`,
    l.city                                          AS `transaction city`,
    l.is_high_risk_country,
    t.transaction_datetime,
    ROUND(t.amount_usd, 2)                          AS `amount usd`,
    t.channel,
    t.is_flagged
FROM fact_transactions t
JOIN dim_customer c  ON t.customer_key = c.customer_key
JOIN dim_location l  ON t.location_key = l.location_key
WHERE c.country != l.country
ORDER BY l.is_high_risk_country DESC, t.amount_usd DESC
LIMIT 50;


-- ------------------------------------------------------------
-- Geographic Mismatch Summary by Customer
-- Which customers have the most cross-country transactions?
-- ------------------------------------------------------------
SELECT
    c.customer_id,
    c.full_name,
    c.country                                       AS `home country`,
    COUNT(*)                                        AS `mismatch txn count`,
    COUNT(DISTINCT l.country)                       AS `countries transacted in`,
    ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`,
    SUM(t.is_flagged)                               AS `flagged count`,
    SUM(l.is_high_risk_country)                     AS `high risk country hits`
FROM fact_transactions t
JOIN dim_customer c  ON t.customer_key = c.customer_key
JOIN dim_location l  ON t.location_key = l.location_key
WHERE c.country != l.country
GROUP BY c.customer_id, c.full_name, c.country
ORDER BY `high risk country hits` DESC, `mismatch txn count` DESC
LIMIT 20;

-- ============================================================
-- NorthAxis Bank | Operation Clearwater
-- TASK 3: Customer Risk Profiling
-- ============================================================
USE northaxis_bank;

-- ------------------------------------------------------------
-- Customer Transaction Baseline
-- Build a behavioral profile for every customer
-- ------------------------------------------------------------
SELECT
    c.customer_id,
    c.full_name,
    c.customer_segment,
    c.country                                           AS `home country`,
    c.kyc_status,
    c.preferred_channel,
    COUNT(t.transaction_key)                            AS `total transactions`,
    COUNT(DISTINCT t.account_key)                       AS `accounts used`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total spend usd`,
    ROUND(AVG(t.amount_usd), 2)                         AS `average transaction usd`,
    ROUND(MAX(t.amount_usd), 2)                         AS `max single transaction usd`,
    ROUND(STDDEV(t.amount_usd), 2)                      AS `spend volatility`,
    SUM(t.is_flagged)                                   AS `total flagged`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`,
    SUM(t.is_off_hours)                                 AS `off hours transactions`,
    ROUND(SUM(t.is_off_hours) / COUNT(*) * 100, 2)      AS `off hours percentage`,
    MIN(t.transaction_date)                             AS `first transaction`,
    MAX(t.transaction_date)                             AS `last transaction`
FROM fact_transactions t
JOIN dim_customer c ON t.customer_key = c.customer_key
GROUP BY
    c.customer_id, c.full_name, c.customer_segment,
    c.country, c.kyc_status, c.preferred_channel
ORDER BY `total flagged` DESC
LIMIT 50;


-- ------------------------------------------------------------
-- High Risk Customers
-- Customers with high flagged %, off-hours activity,
-- poor KYC status, and large total spend
-- ------------------------------------------------------------
SELECT
    c.customer_id,
    c.full_name,
    c.customer_segment,
    c.kyc_status,
    c.country                                           AS `home country`,
    COUNT(t.transaction_key)                            AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total spend usd`,
    ROUND(AVG(t.amount_usd), 2)                         AS `average transaction usd`,
    SUM(t.is_flagged)                                   AS `total flagged`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`,
    SUM(t.is_off_hours)                                 AS `off hours transactions`,
    ROUND(SUM(t.is_off_hours) / COUNT(*) * 100, 2)      AS `off hours percentage`
FROM fact_transactions t
JOIN dim_customer c ON t.customer_key = c.customer_key
GROUP BY
    c.customer_id, c.full_name, c.customer_segment,
    c.kyc_status, c.country
HAVING
    `flagged percentage` > 20
    AND `total transactions` >= 5
ORDER BY `flagged percentage` DESC, `total spend usd` DESC
LIMIT 30;


-- ------------------------------------------------------------
-- Customer Channel Behaviour
-- Does a customer suddenly switch to an unusual channel?
-- Compares each customer's preferred channel vs actual usage
-- ------------------------------------------------------------
SELECT
    c.customer_id,
    c.full_name,
    c.preferred_channel,
    t.channel                                           AS `channel used`,
    COUNT(*)                                            AS `transaction count`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`,
    CASE
        WHEN t.channel != c.preferred_channel THEN 'Unusual Channel'
        ELSE 'Normal Channel'
    END                                                 AS `channel pattern`
FROM fact_transactions t
JOIN dim_customer c ON t.customer_key = c.customer_key
GROUP BY
    c.customer_id, c.full_name, c.preferred_channel, t.channel
HAVING `channel pattern` = 'Unusual Channel'
ORDER BY `flagged count` DESC, `total value usd` DESC
LIMIT 30;


-- ------------------------------------------------------------
-- Customer Monthly Spend Deviation
-- Flags customers whose Q3 spend spiked vs their own Q1/Q2 average
-- This directly targets the 6-week fraud window
-- ------------------------------------------------------------
WITH monthly_spend AS (
    SELECT
        t.customer_key,
        d.month_number,
        d.month_name,
        d.quarter_number,
        ROUND(SUM(t.amount_usd), 2)         AS `monthly spend`,
        COUNT(*)                             AS `monthly txn count`,
        SUM(t.is_flagged)                   AS `monthly flagged`
    FROM fact_transactions t
    JOIN dim_date d ON t.date_key = d.date_key
    GROUP BY t.customer_key, d.month_number, d.month_name, d.quarter_number
),
customer_baseline AS (
    SELECT
        customer_key,
        ROUND(AVG(`monthly spend`), 2)      AS `average monthly spend`,
        ROUND(AVG(`monthly txn count`), 2)  AS `average monthly txns`
    FROM monthly_spend
    WHERE quarter_number IN (1, 2)
    GROUP BY customer_key
)
SELECT
    c.customer_id,
    c.full_name,
    c.customer_segment,
    ms.month_name,
    ms.quarter_number                                   AS `quarter`,
    ms.`monthly spend`,
    ms.`monthly txn count`,
    ms.`monthly flagged`,
    cb.`average monthly spend`                              AS `q1 q2 baseline spend`,
    ROUND(ms.`monthly spend` / NULLIF(cb.`average monthly spend`, 0), 2) AS `spend multiplier`
FROM monthly_spend ms
JOIN customer_baseline cb   ON ms.customer_key = cb.customer_key
JOIN dim_customer c         ON ms.customer_key = c.customer_key
WHERE
    ms.quarter_number = 3
    AND ms.`monthly spend` > (cb.`average monthly spend` * 2)
ORDER BY `spend multiplier` DESC
LIMIT 30;


-- ------------------------------------------------------------
-- Dormant Customer Suddenly Active
-- Customers with no activity for 60+ days who suddenly transact
-- Classic account takeover pattern
-- ------------------------------------------------------------
WITH customer_activity AS (
    SELECT
        customer_key,
        transaction_date,
        amount_usd,
        is_flagged,
        channel,
        LAG(transaction_date) OVER (
            PARTITION BY customer_key ORDER BY transaction_date
        )                               AS `previous transaction date`
    FROM fact_transactions
)
SELECT
    c.customer_id,
    c.full_name,
    c.customer_segment,
    c.kyc_status,
    ca.`previous transaction date`,
    ca.transaction_date                             AS `return date`,
    DATEDIFF(ca.transaction_date, ca.`previous transaction date`) AS `days dormant`,
    ROUND(ca.amount_usd, 2)                         AS `return transaction amount`,
    ca.channel,
    ca.is_flagged
FROM customer_activity ca
JOIN dim_customer c ON ca.customer_key = c.customer_key
WHERE DATEDIFF(ca.transaction_date, ca.`previous transaction date`) >= 60
ORDER BY `days dormant` DESC, `return transaction amount` DESC
LIMIT 30;


-- ------------------------------------------------------------
--  KYC Risk — Transactions by KYC Status
-- Unverified or expired KYC customers making large transactions
-- ------------------------------------------------------------
SELECT
    c.kyc_status,
    COUNT(DISTINCT c.customer_id)                       AS `unique customers`,
    COUNT(t.transaction_key)                            AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                         AS `average transaction usd`,
    ROUND(MAX(t.amount_usd), 2)                         AS `max transaction usd`,
    SUM(t.is_flagged)                                   AS `total flagged`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`
FROM fact_transactions t
JOIN dim_customer c ON t.customer_key = c.customer_key
GROUP BY c.kyc_status
ORDER BY `flagged percentage` DESC;


-- ------------------------------------------------------------
-- Top 20 Highest Risk Customer Watchlist
-- Combines flagged transactions, off-hours, geo mismatch,
-- and KYC status into one unified watchlist
-- ------------------------------------------------------------
SELECT
    c.customer_id,
    c.full_name,
    c.customer_segment,
    c.kyc_status,
    c.country                                           AS `home country`,
    COUNT(t.transaction_key)                            AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total spend usd`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`,
    SUM(t.is_off_hours)                                 AS `off hours count`,
    SUM(CASE WHEN c.country != l.country THEN 1 ELSE 0 END) AS `geo mismatch count`,
    SUM(CASE WHEN l.is_high_risk_country = 1 THEN 1 ELSE 0 END) AS `high risk country hits`,
    CASE
        WHEN c.kyc_status IN ('Unverified', 'Expired') THEN 'YES'
        ELSE 'NO'
    END                                                 AS `poor kyc`
FROM fact_transactions t
JOIN dim_customer c  ON t.customer_key = c.customer_key
JOIN dim_location l  ON t.location_key = l.location_key
GROUP BY
    c.customer_id, c.full_name, c.customer_segment,
    c.kyc_status, c.country
ORDER BY
    `flagged count` DESC,
    `geo mismatch count` DESC,
    `high risk country hits` DESC
LIMIT 20;

-- ============================================================
-- NorthAxis Bank | Operation Clearwater
-- TASK 4: Merchant & Channel Risk Scoring
-- ============================================================
USE northaxis_bank;

-- ------------------------------------------------------------
-- Merchant Overview
-- Full breakdown of every merchant by transaction activity
-- ------------------------------------------------------------
SELECT
    m.merchant_name,
    m.merchant_category,
    m.risk_rating,
    m.country                                           AS `merchant country`,
    m.is_shell_merchant,
    COUNT(t.transaction_key)                            AS `total transactions`,
    COUNT(DISTINCT t.customer_key)                      AS `unique customers`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                         AS `average transaction usd`,
    ROUND(MAX(t.amount_usd), 2)                         AS `max transaction usd`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`,
    SUM(t.is_off_hours)                                 AS `off hours count`
FROM fact_transactions t
JOIN dim_merchant m ON t.merchant_key = m.merchant_key
GROUP BY
    m.merchant_name, m.merchant_category,
    m.risk_rating, m.country, m.is_shell_merchant
ORDER BY `flagged percentage` DESC;


-- ------------------------------------------------------------
-- Merchant Risk Ranking
-- Ranks merchants by a combined risk score:
-- flagged %, volume through high-risk hours, and shell status
-- ------------------------------------------------------------
SELECT
    m.merchant_name,
    m.merchant_category,
    m.risk_rating,
    m.is_shell_merchant,
    COUNT(t.transaction_key)                            AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`,
    SUM(t.is_off_hours)                                 AS `off hours count`,
    ROUND(SUM(t.is_off_hours) / COUNT(*) * 100, 2)      AS `off hours percentage`,
    -- Risk Score: weighted combination of fraud signals
    ROUND(
        (SUM(t.is_flagged) / COUNT(*) * 50)             -- 50% weight: flagged pct
        + (SUM(t.is_off_hours) / COUNT(*) * 30)         -- 30% weight: off hours pct
        + (m.is_shell_merchant * 20)                    -- 20% weight: shell merchant flag
    , 2)                                                AS `merchant risk score`
FROM fact_transactions t
JOIN dim_merchant m ON t.merchant_key = m.merchant_key
GROUP BY
    m.merchant_name, m.merchant_category,
    m.risk_rating, m.is_shell_merchant
ORDER BY `merchant risk score` DESC;


-- ------------------------------------------------------------
-- Shell Merchant Analysis
-- Shell merchants are pre-flagged as suspicious in the data
-- How much money is flowing through them?
-- ------------------------------------------------------------
SELECT
    m.merchant_name,
    m.merchant_category,
    m.country                                           AS `merchant country`,
    m.risk_rating,
    COUNT(t.transaction_key)                            AS `total transactions`,
    COUNT(DISTINCT t.customer_key)                      AS `unique customers`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                         AS `average transaction usd`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`
FROM fact_transactions t
JOIN dim_merchant m ON t.merchant_key = m.merchant_key
WHERE m.is_shell_merchant = 1
GROUP BY
    m.merchant_name, m.merchant_category,
    m.country, m.risk_rating
ORDER BY `total value usd` DESC;


-- ------------------------------------------------------------
-- Merchant Category Risk Summary
-- Which product/service categories attract the most fraud?
-- ------------------------------------------------------------
SELECT
    m.merchant_category,
    COUNT(DISTINCT m.merchant_name)                     AS `merchant count`,
    COUNT(t.transaction_key)                            AS `total transactions`,
    COUNT(DISTINCT t.customer_key)                      AS `unique customers`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                         AS `average transaction usd`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`,
    SUM(t.is_off_hours)                                 AS `off hours count`,
    ROUND(SUM(t.is_off_hours) / COUNT(*) * 100, 2)      AS `off hours percentage`
FROM fact_transactions t
JOIN dim_merchant m ON t.merchant_key = m.merchant_key
GROUP BY m.merchant_category
ORDER BY `flagged percentage` DESC;


-- ------------------------------------------------------------
-- Channel Risk Scoring
-- Ranks each channel by flagged %, off-hours abuse,
-- and average transaction size
-- ------------------------------------------------------------
SELECT
    t.channel,
    COUNT(t.transaction_key)                            AS `total transactions`,
    COUNT(DISTINCT t.customer_key)                      AS `unique customers`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    ROUND(AVG(t.amount_usd), 2)                         AS `average transaction usd`,
    ROUND(MAX(t.amount_usd), 2)                         AS `max transaction usd`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`,
    SUM(t.is_off_hours)                                 AS `off hours count`,
    ROUND(SUM(t.is_off_hours) / COUNT(*) * 100, 2)      AS `off hours percentage`,
    -- Channel Risk Score
    ROUND(
        (SUM(t.is_flagged) / COUNT(*) * 60)             -- 60% weight: flagged percentage
        + (SUM(t.is_off_hours) / COUNT(*) * 40)         -- 40% weight: off hours percentage
    , 2)                                                AS `channel risk score`
FROM fact_transactions t
GROUP BY t.channel
ORDER BY `channel risk score` DESC;


-- ------------------------------------------------------------
-- Channel Trend by Month
-- Did a specific channel spike in Q3 during the fraud window?
-- ------------------------------------------------------------
SELECT
    t.channel,
    d.month_name,
    d.month_number,
    d.quarter_number                                    AS `quarter`,
    COUNT(t.transaction_key)                            AS `total transactions`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`
FROM fact_transactions t
JOIN dim_date d ON t.date_key = d.date_key
GROUP BY t.channel, d.month_name, d.month_number, d.quarter_number
ORDER BY t.channel, d.month_number;


-- ------------------------------------------------------------
-- High Risk Merchant + High Risk Customer Overlap
-- The most dangerous combination — risky merchants
-- being used by already flagged customers
-- ------------------------------------------------------------
SELECT
    m.merchant_name,
    m.merchant_category,
    m.risk_rating,
    m.is_shell_merchant,
    c.customer_id,
    c.full_name,
    c.customer_segment,
    c.kyc_status,
    COUNT(t.transaction_key)                            AS `transactions together`,
    ROUND(SUM(t.amount_usd), 2)                         AS `total value usd`,
    SUM(t.is_flagged)                                   AS `flagged count`,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2)        AS `flagged percentage`
FROM fact_transactions t
JOIN dim_merchant m  ON t.merchant_key = m.merchant_key
JOIN dim_customer c  ON t.customer_key = c.customer_key
WHERE
    m.risk_rating IN ('High', 'Critical')
    OR m.is_shell_merchant = 1
GROUP BY
    m.merchant_name, m.merchant_category, m.risk_rating,
    m.is_shell_merchant, c.customer_id, c.full_name,
    c.customer_segment, c.kyc_status
HAVING `flagged count` > 0
ORDER BY `flagged count` DESC, `total value usd` DESC
LIMIT 30;


-- ------------------------------------------------------------
-- Merchant Concentration Risk
-- Are a small number of merchants responsible for
-- a disproportionate share of flagged transactions?
-- (Pareto / 80-20 analysis)
-- ------------------------------------------------------------
WITH merchant_flagged AS (
    SELECT
        m.merchant_name,
        m.merchant_category,
        m.risk_rating,
        SUM(t.is_flagged)                               AS `flagged count`,
        ROUND(SUM(t.amount_usd), 2)                     AS `total value usd`
    FROM fact_transactions t
    JOIN dim_merchant m ON t.merchant_key = m.merchant_key
    GROUP BY m.merchant_name, m.merchant_category, m.risk_rating
),
totals AS (
    SELECT SUM(`flagged count`) AS `total flagged` FROM merchant_flagged
)
SELECT
    mf.merchant_name,
    mf.merchant_category,
    mf.risk_rating,
    mf.`flagged count`,
    mf.`total value usd`,
    ROUND(mf.`flagged count` / t.`total flagged` * 100, 2) AS `percentage of all fraud`,
    ROUND(SUM(mf.`flagged count`) OVER (
        ORDER BY mf.`flagged count` DESC
    ) / t.`total flagged` * 100, 2)                     AS `cumulative fraud percentage`
FROM merchant_flagged mf
CROSS JOIN totals t
ORDER BY mf.`flagged count` DESC
LIMIT 20;

-- ============================================================
-- NorthAxis Bank | Operation Clearwater
-- TASK 5: Fraud Risk Scoring Model (Optimised)
-- ============================================================
USE northaxis_bank;

-- Run this first to increase timeout
SET SESSION wait_timeout = 600;
SET SESSION interactive_timeout = 600;
SET SESSION net_read_timeout = 600;


-- ------------------------------------------------------------
-- STEP 1: Pre-compute customer averages into a temp table
-- This avoids the slow correlated subquery in amount_signal
-- ------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS temp_customer_stats;
CREATE TEMPORARY TABLE temp_customer_stats AS
SELECT
    customer_key,
    AVG(amount_usd)                                     AS avg_amount,
    STDDEV(amount_usd)                                  AS stddev_amount,
    AVG(amount_usd) + (3 * STDDEV(amount_usd))          AS outlier_threshold
FROM fact_transactions
GROUP BY customer_key;


-- ------------------------------------------------------------
-- STEP 2: Pre-compute velocity into a temp table
-- Uses flagged-only transactions to reduce self-join size
-- ------------------------------------------------------------
DROP TEMPORARY TABLE IF EXISTS temp_velocity;
CREATE TEMPORARY TABLE temp_velocity AS
SELECT
    t1.customer_key,
    COUNT(*)                                            AS velocity_hit_count
FROM fact_transactions t1
JOIN fact_transactions t2
    ON  t1.account_key = t2.account_key
    AND t2.transaction_datetime > t1.transaction_datetime
    AND TIMESTAMPDIFF(MINUTE, t1.transaction_datetime, t2.transaction_datetime) <= 10
    AND t1.transaction_id != t2.transaction_id
    AND t1.is_flagged = 1
GROUP BY t1.customer_key;


-- ------------------------------------------------------------
-- Top 50 Riskiest Customers — Full Watchlist
-- ------------------------------------------------------------
WITH

flagged_signal AS (
    SELECT
        customer_key,
        COUNT(*)                                        AS total_txns,
        SUM(is_flagged)                                 AS flagged_count,
        ROUND(SUM(is_flagged) / COUNT(*) * 100, 2)      AS flagged_pct,
        ROUND(SUM(is_off_hours) / COUNT(*) * 100, 2)    AS off_hours_pct
    FROM fact_transactions
    GROUP BY customer_key
),

geo_signal AS (
    SELECT
        t.customer_key,
        SUM(CASE WHEN c.country != l.country THEN 1 ELSE 0 END)         AS geo_mismatch_count,
        SUM(CASE WHEN l.is_high_risk_country = 1 THEN 1 ELSE 0 END)     AS high_risk_country_count
    FROM fact_transactions t
    JOIN dim_customer c ON t.customer_key = c.customer_key
    JOIN dim_location l ON t.location_key = l.location_key
    GROUP BY t.customer_key
),

shell_signal AS (
    SELECT
        t.customer_key,
        ROUND(SUM(CASE WHEN m.is_shell_merchant = 1 THEN 1 ELSE 0 END)
            / COUNT(*) * 100, 2)                        AS shell_txn_pct
    FROM fact_transactions t
    JOIN dim_merchant m ON t.merchant_key = m.merchant_key
    GROUP BY t.customer_key
),

-- Uses temp table instead of correlated subquery
amount_signal AS (
    SELECT
        t.customer_key,
        SUM(CASE WHEN t.amount_usd >= cs.outlier_threshold THEN 1 ELSE 0 END) AS outlier_txn_count
    FROM fact_transactions t
    JOIN temp_customer_stats cs ON t.customer_key = cs.customer_key
    GROUP BY t.customer_key
),

combined AS (
    SELECT
        fs.customer_key,
        fs.total_txns,
        fs.flagged_count,
        fs.flagged_pct,
        fs.off_hours_pct,
        gs.geo_mismatch_count,
        gs.high_risk_country_count,
        ss.shell_txn_pct,
        am.outlier_txn_count,
        COALESCE(v.velocity_hit_count, 0)               AS velocity_hit_count
    FROM flagged_signal fs
    LEFT JOIN geo_signal gs         ON fs.customer_key = gs.customer_key
    LEFT JOIN shell_signal ss       ON fs.customer_key = ss.customer_key
    LEFT JOIN amount_signal am      ON fs.customer_key = am.customer_key
    LEFT JOIN temp_velocity v       ON fs.customer_key = v.customer_key
),

ntile_scores AS (
    SELECT
        customer_key,
        total_txns,
        flagged_count,
        flagged_pct,
        off_hours_pct,
        geo_mismatch_count,
        high_risk_country_count,
        shell_txn_pct,
        outlier_txn_count,
        velocity_hit_count,
        NTILE(5) OVER (ORDER BY flagged_pct ASC)            AS `flagged score`,
        NTILE(5) OVER (ORDER BY off_hours_pct ASC)          AS `off hours score`,
        NTILE(5) OVER (ORDER BY geo_mismatch_count ASC)     AS `geo score`,
        NTILE(5) OVER (ORDER BY high_risk_country_count ASC) AS `high risk country score`,
        NTILE(5) OVER (ORDER BY shell_txn_pct ASC)          AS `shell score`,
        NTILE(5) OVER (ORDER BY outlier_txn_count ASC)      AS `outlier score`,
        NTILE(5) OVER (ORDER BY velocity_hit_count ASC)     AS `velocity score`
    FROM combined
)

SELECT
    c.customer_id,
    c.full_name,
    c.customer_segment,
    c.kyc_status,
    c.country                                           AS `home country`,
    ns.flagged_pct                                      AS `flagged pct`,
    ns.off_hours_pct                                    AS `off hours pct`,
    ns.geo_mismatch_count                               AS `geo mismatches`,
    ns.high_risk_country_count                          AS `high risk country txns`,
    ns.shell_txn_pct                                    AS `shell merchant pct`,
    ns.outlier_txn_count                                AS `amount outliers`,
    ns.velocity_hit_count                               AS `velocity hits`,
    ROUND(
        (`flagged score`            * 30 / 5)
      + (`velocity score`           * 20 / 5)
      + (`off hours score`          * 15 / 5)
      + (`geo score`                * 15 / 5)
      + (`shell score`              * 10 / 5)
      + (`high risk country score`  *  5 / 5)
      + (`outlier score`            *  5 / 5)
    , 2)                                                AS `composite risk score`,
    CASE
        WHEN ROUND((`flagged score`*30/5)+(`velocity score`*20/5)+(`off hours score`*15/5)
             +(`geo score`*15/5)+(`shell score`*10/5)+(`high risk country score`*5/5)
             +(`outlier score`*5/5),2) >= 80 THEN 'CRITICAL'
        WHEN ROUND((`flagged score`*30/5)+(`velocity score`*20/5)+(`off hours score`*15/5)
             +(`geo score`*15/5)+(`shell score`*10/5)+(`high risk country score`*5/5)
             +(`outlier score`*5/5),2) >= 60 THEN 'HIGH'
        WHEN ROUND((`flagged score`*30/5)+(`velocity score`*20/5)+(`off hours score`*15/5)
             +(`geo score`*15/5)+(`shell score`*10/5)+(`high risk country score`*5/5)
             +(`outlier score`*5/5),2) >= 40 THEN 'MEDIUM'
        WHEN ROUND((`flagged score`*30/5)+(`velocity score`*20/5)+(`off hours score`*15/5)
             +(`geo score`*15/5)+(`shell score`*10/5)+(`high risk country score`*5/5)
             +(`outlier score`*5/5),2) >= 20 THEN 'LOW'
        ELSE 'MINIMAL'
    END                                                 AS `risk tier`
FROM ntile_scores ns
JOIN dim_customer c ON ns.customer_key = c.customer_key
ORDER BY `composite risk score` DESC
LIMIT 50;


-- ------------------------------------------------------------
-- Risk Tier Summary — How many customers per tier?
-- ------------------------------------------------------------
WITH

flagged_signal AS (
    SELECT
        customer_key,
        ROUND(SUM(is_flagged) / COUNT(*) * 100, 2)      AS flagged_pct,
        ROUND(SUM(is_off_hours) / COUNT(*) * 100, 2)    AS off_hours_pct
    FROM fact_transactions
    GROUP BY customer_key
),

geo_signal AS (
    SELECT
        t.customer_key,
        SUM(CASE WHEN c.country != l.country THEN 1 ELSE 0 END)         AS geo_mismatch_count,
        SUM(CASE WHEN l.is_high_risk_country = 1 THEN 1 ELSE 0 END)     AS high_risk_country_count
    FROM fact_transactions t
    JOIN dim_customer c ON t.customer_key = c.customer_key
    JOIN dim_location l ON t.location_key = l.location_key
    GROUP BY t.customer_key
),

shell_signal AS (
    SELECT
        t.customer_key,
        ROUND(SUM(CASE WHEN m.is_shell_merchant = 1 THEN 1 ELSE 0 END)
            / COUNT(*) * 100, 2)                        AS shell_txn_pct
    FROM fact_transactions t
    JOIN dim_merchant m ON t.merchant_key = m.merchant_key
    GROUP BY t.customer_key
),

amount_signal AS (
    SELECT
        t.customer_key,
        SUM(CASE WHEN t.amount_usd >= cs.outlier_threshold THEN 1 ELSE 0 END) AS outlier_txn_count
    FROM fact_transactions t
    JOIN temp_customer_stats cs ON t.customer_key = cs.customer_key
    GROUP BY t.customer_key
),

ntile_scores AS (
    SELECT
        fs.customer_key,
        NTILE(5) OVER (ORDER BY fs.flagged_pct ASC)                     AS `flagged score`,
        NTILE(5) OVER (ORDER BY fs.off_hours_pct ASC)                   AS `off hours score`,
        NTILE(5) OVER (ORDER BY gs.geo_mismatch_count ASC)              AS `geo score`,
        NTILE(5) OVER (ORDER BY gs.high_risk_country_count ASC)         AS `high risk country score`,
        NTILE(5) OVER (ORDER BY ss.shell_txn_pct ASC)                   AS `shell score`,
        NTILE(5) OVER (ORDER BY am.outlier_txn_count ASC)               AS `outlier score`,
        NTILE(5) OVER (ORDER BY COALESCE(v.velocity_hit_count, 0) ASC)  AS `velocity score`
    FROM flagged_signal fs
    LEFT JOIN geo_signal gs     ON fs.customer_key = gs.customer_key
    LEFT JOIN shell_signal ss   ON fs.customer_key = ss.customer_key
    LEFT JOIN amount_signal am  ON fs.customer_key = am.customer_key
    LEFT JOIN temp_velocity v   ON fs.customer_key = v.customer_key
),

composite_score AS (
    SELECT
        customer_key,
        ROUND(
            (`flagged score`            * 30 / 5)
          + (`velocity score`           * 20 / 5)
          + (`off hours score`          * 15 / 5)
          + (`geo score`                * 15 / 5)
          + (`shell score`              * 10 / 5)
          + (`high risk country score`  *  5 / 5)
          + (`outlier score`            *  5 / 5)
        , 2)                                            AS `composite risk score`
    FROM ntile_scores
)

SELECT
    CASE
        WHEN `composite risk score` >= 80 THEN 'CRITICAL'
        WHEN `composite risk score` >= 60 THEN 'HIGH'
        WHEN `composite risk score` >= 40 THEN 'MEDIUM'
        WHEN `composite risk score` >= 20 THEN 'LOW'
        ELSE 'MINIMAL'
    END                                                 AS `risk tier`,
    COUNT(*)                                            AS `customer count`,
    ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 2)   AS `percentage of customers`
FROM composite_score
GROUP BY `risk tier`
ORDER BY FIELD(`risk tier`, 'CRITICAL','HIGH','MEDIUM','LOW','MINIMAL');