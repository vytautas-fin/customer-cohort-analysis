-- Use a CTE to find the maximum start date once
WITH subscription_max_date AS (
  SELECT MAX(subscription_start) AS max_start_date
  FROM `tc-da-1.turing_data_analytics.subscriptions`
),

modified_subscriptions AS (
  SELECT
    s.*,
    DATE_TRUNC(s.subscription_start, WEEK) AS start_week,
    COALESCE(DATE_TRUNC(s.subscription_end, WEEK), DATE('2099-01-01')) AS end_week,
    
    -- Calculate the subscription length in weeks
    DATE_DIFF(
      COALESCE(DATE_TRUNC(s.subscription_end, WEEK), DATE('2099-01-01')),
      DATE_TRUNC(s.subscription_start, WEEK),
      WEEK
    ) AS subscription_length_weeks,
    
    -- Boolean indicator whether a subscription week has a full week of data
    DATE_DIFF(
      m.max_start_date, 
      DATE_TRUNC(s.subscription_start, WEEK), 
      WEEK
    ) >= 1 AS is_full_week
    
  FROM 
    `tc-da-1.turing_data_analytics.subscriptions` AS s
  -- CROSS JOIN makes the single max_start_date value available to every row
  CROSS JOIN 
    subscription_max_date AS m
),

-- Find the maximum full week to ensure we do not cross it.
date_params AS (
  SELECT 
    DATE_TRUNC(MAX(subscription_start), WEEK) AS max_week
  FROM modified_subscriptions
  WHERE is_full_week = TRUE
),

cohort_table AS (
    SELECT
      start_week,
      -- Calculate number of subscriptions for each cohort
      COUNT(*) AS cohort_size,
      -- Because we evaluate full weeks, each cohort will have a first week count. Use subscription lenght to identify it.
      COUNTIF(subscription_length_weeks >= 1) AS week_1_count,
      -- For the most recent cohort, set week_2_count to NULL as the data is not yet available. Otherwise perform the count.
      CASE WHEN DATE_DIFF(max_week, start_week, WEEK)  = 0 THEN NULL ELSE COUNTIF(subscription_length_weeks >= 2) END AS week_2_count,
      -- This calculates Week 3 retention. A cohort must be at least 3 weeks old for this to be a valid metric.
      -- The condition `DATE_DIFF(...) <= 1` checks if the cohort is one of the two most recent complete cohorts.
      -- If it is, we set the value to NULL because not enough time has passed to measure 3 weeks of retention.
      -- This prevents showing a misleading 0 for these newer groups. 
      CASE WHEN DATE_DIFF(max_week, start_week, WEEK) <= 1 THEN NULL ELSE COUNTIF(subscription_length_weeks >= 3) END AS week_3_count,
      CASE WHEN DATE_DIFF(max_week, start_week, WEEK) <= 2 THEN NULL ELSE COUNTIF(subscription_length_weeks >= 4) END AS week_4_count,
      CASE WHEN DATE_DIFF(max_week, start_week, WEEK) <= 3 THEN NULL ELSE COUNTIF(subscription_length_weeks >= 5) END AS week_5_count,
      CASE WHEN DATE_DIFF(max_week, start_week, WEEK) <= 4 THEN NULL ELSE COUNTIF(subscription_length_weeks >= 6) END AS week_6_count,
    FROM modified_subscriptions, date_params
    WHERE is_full_week = TRUE
    GROUP BY start_week, max_week
)

SELECT * FROM cohort_table
