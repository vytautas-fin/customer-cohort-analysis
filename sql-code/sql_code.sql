WITH
date_range AS (
    SELECT DATE_TRUNC(MAX(subscription_start), WEEK) AS max_start
    ,DATE_TRUNC(MAX(subscription_end), WEEK) AS max_end
    FROM `tc-da-1.turing_data_analytics.subscriptions`
    WHERE 1=1
      AND subscription_start < '2021-01-31' -- Exclude the last day in dataset to only have full weekly cohorts
      AND subscription_end < '2021-01-31' -- Exclude the last day in dataset to only have full weekly cohorts
),

week_grouping AS (
SELECT *
  ,DATE_TRUNC(subscription_start, WEEK) AS cohort_week
  ,DATE_TRUNC(subscription_end, WEEK) AS end_week
  ,DATE_DIFF(DATE_TRUNC(subscription_end, WEEK),DATE_TRUNC(subscription_start, WEEK), WEEK) AS subscription_length -- Difference in weeks between subscription_start and subscription_end
FROM `tc-da-1.turing_data_analytics.subscriptions`
WHERE 1=1
  AND subscription_start < '2021-01-31' -- Exclude the last day in dataset to only have full weekly cohorts
  --AND user_pseudo_id = '1526801.0560015811'
),

-- Count subscription activity based on measuring whether subscription was active at the beginning of particular week.
subscription_activity AS (
SELECT *
  ,CASE
    WHEN subscription_length = 0 THEN NULL -- subscription was ended on the same week as started, thus it is not counted at beginning of week 1.
    ELSE 1
  END as week_1 
  ,CASE
    WHEN subscription_length <= 1 THEN NULL -- If subscription_length is <= 1 THEN subscription IS NOT active one week after start
    WHEN subscription_length > 1 THEN 1 -- If subscription_length is >1 THEN subscription IS active one week after start
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) = 0 THEN NULL -- If subscription_length is NULL, AND difference between cohort_week and max_end is 0 THEN it means we are at the last point of our data.
    ELSE 1
  END AS week_2
   ,CASE
    WHEN subscription_length <= 2 THEN NULL
    WHEN subscription_length > 2 THEN 1
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) <= 1 THEN NULL
    ELSE 1
  END AS week_3
   ,CASE
    WHEN subscription_length <= 3 THEN NULL
    WHEN subscription_length > 3 THEN 1
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) <= 2 THEN NULL
    ELSE 1
  END AS week_4
   ,CASE
    WHEN subscription_length <= 4 THEN NULL
    WHEN subscription_length > 4 THEN 1
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) <= 3 THEN NULL
    ELSE 1
  END AS week_5
   ,CASE
    WHEN subscription_length <= 5 THEN NULL
    WHEN subscription_length > 5 THEN 1
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) <= 4 THEN NULL
    ELSE 1
  END AS week_6

FROM week_grouping, date_range
)

SELECT cohort_week
  ,COUNT(*) AS subscriptions -- Number of subscriptions acquired ib particular cohort week
  ,SUM(week_1) AS week_1 -- Number of subscriptions that remained active after initial cohort week (measured at the start of week 1)
  ,SUM(week_2) AS week_2 -- Number of subscriptions that remained active after 2 weeks since cohort week (measured at the start of week 2)
  ,SUM(week_3) AS week_3 -- Number of subscriptions that remained active after 3 weeks since cohort week (measured at the start of week 3)
  ,SUM(week_4) AS week_4 -- Number of subscriptions that remained active after 4 weeks since cohort week (measured at the start of week 4)
  ,SUM(week_5) AS week_5 -- Number of subscriptions that remained active after 5 weeks since cohort week (measured at the start of week 5)
  ,SUM(week_6) AS week_6 -- Number of subscriptions that remained active after 6 weeks since cohort week (measured at the start of week 6)

FROM subscription_activity
GROUP BY cohort_week;




-- Data table:

WITH
date_range AS (
    SELECT DATE_TRUNC(MAX(subscription_start), WEEK) AS max_start
    ,DATE_TRUNC(MAX(subscription_end), WEEK) AS max_end
    FROM `tc-da-1.turing_data_analytics.subscriptions`
    WHERE 1=1
      AND subscription_start < '2021-01-31' -- Exclude the last day in dataset to only have full weekly cohorts
      AND subscription_end < '2021-01-31' -- Exclude the last day in dataset to only have full weekly cohorts
),

week_grouping AS (
SELECT *
  ,DATE_TRUNC(subscription_start, WEEK) AS cohort_week
  ,DATE_TRUNC(subscription_end, WEEK) AS end_week
  ,DATE_DIFF(DATE_TRUNC(subscription_end, WEEK),DATE_TRUNC(subscription_start, WEEK), WEEK) AS subscription_length -- Difference in weeks between subscription_start and subscription_end
FROM `tc-da-1.turing_data_analytics.subscriptions`
WHERE 1=1
  AND subscription_start < '2021-01-31' -- Exclude the last day in dataset to only have full weekly cohorts
  --AND user_pseudo_id = '1526801.0560015811'
),

-- Count subscription activity based on measuring whether subscription was active at the beginning of particular week.
subscription_activity AS (
SELECT *
  ,CASE
    WHEN subscription_length = 0 THEN NULL -- subscription was ended on the same week as started, thus it is not counted at beginning of week 1.
    ELSE 1
  END as week_1 
  ,CASE
    WHEN subscription_length <= 1 THEN NULL -- If subscription_length is <= 1 THEN subscription IS NOT active one week after start
    WHEN subscription_length > 1 THEN 1 -- If subscription_length is >1 THEN subscription IS active one week after start
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) = 0 THEN NULL -- If subscription_length is NULL, AND difference between cohort_week and max_end is 0 THEN it means we are at the last point of our data.
    ELSE 1
  END AS week_2
   ,CASE
    WHEN subscription_length <= 2 THEN NULL
    WHEN subscription_length > 2 THEN 1
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) <= 1 THEN NULL
    ELSE 1
  END AS week_3
   ,CASE
    WHEN subscription_length <= 3 THEN NULL
    WHEN subscription_length > 3 THEN 1
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) <= 2 THEN NULL
    ELSE 1
  END AS week_4
   ,CASE
    WHEN subscription_length <= 4 THEN NULL
    WHEN subscription_length > 4 THEN 1
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) <= 3 THEN NULL
    ELSE 1
  END AS week_5
   ,CASE
    WHEN subscription_length <= 5 THEN NULL
    WHEN subscription_length > 5 THEN 1
    WHEN subscription_length IS NULL AND DATE_DIFF(max_end, cohort_week, WEEK) <= 4 THEN NULL
    ELSE 1
  END AS week_6

FROM week_grouping, date_range
)
SELECT user_pseudo_id
  ,category
  ,country
  ,subscription_activity.cohort_week
  ,subscription_activity.end_week
  ,subscription_activity.subscription_length
  ,1 AS subscriptions
  ,week_1
  ,week_2
  ,week_3
  ,week_4
  ,week_5
  ,week_6
FROM subscription_activity;