#!/bin/bash
# 1. Create the dataset (no change)
bq mk bqml_lab

# 2. Train the model (FASTER)
#    We reduced the scan from 11 months to 1 month.
#    This will still find 100,000 rows, but much faster.
echo "--- 1. Training model on a smaller, faster dataset... ---"
bq query --use_legacy_sql=false \
"
#standardSQL
CREATE OR REPLACE MODEL \`bqml_lab.sample_model\`
OPTIONS(model_type='logistic_reg') AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, '') AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, '') AS country,
  IFNULL(totals.pageviews, 0) AS pageviews
FROM
  \`bigquery-public-data.google_analytics_sample.ga_sessions_*\`
WHERE
  _TABLE_SUFFIX BETWEEN '20170601' AND '20170630' -- WAS 11 MONTHS
LIMIT 100000;
"

# 3. Cache the evaluation/prediction data (NEW STEP)
#    This scans the July 2017 data ONCE and saves it.
echo "--- 2. Caching evaluation data into a temporary table... ---"
bq query --use_legacy_sql=false \
"
#standardSQL
CREATE OR REPLACE TABLE \`bqml_lab.eval_data\` AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, '') AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, '') AS country,
  IFNULL(totals.pageviews, 0) AS pageviews,
  fullVisitorId
FROM
  \`bigquery-public-data.google_analytics_sample.ga_sessions_*\`
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170801';
"

# 4. Run all three queries IN PARALLEL (FASTER)
#    These run at the same time against the small 'eval_data' table.
echo "--- 3. Running evaluation and predictions in parallel... ---"

# Job 1: Evaluate
bq query --use_legacy_sql=false \
"
#standardSQL
SELECT
  *
FROM
  ml.EVALUATE(MODEL \`bqml_lab.sample_model\`, (
    TABLE \`bqml_lab.eval_data\`
  ));
" &

# Job 2: Predict by Country
bq query --use_legacy_sql=false \
"
#standardSQL
SELECT
  country,
  SUM(predicted_label) as total_predicted_purchases
FROM
  ml.PREDICT(MODEL \`bqml_lab.sample_model\`, (
    -- We only select the columns needed for prediction
    SELECT os, is_mobile, pageviews, country FROM \`bqml_lab.eval_data\`
  ))
GROUP BY country
ORDER BY total_predicted_purchases DESC
LIMIT 10;
" &

# Job 3: Predict by Visitor
bq query --use_legacy_sql=false \
"
#standardSQL
SELECT
  fullVisitorId,
  SUM(predicted_label) as total_predicted_purchases
FROM
  ml.PREDICT(MODEL \`bqml_lab.sample_model\`, (
    -- We select all columns needed for prediction
    SELECT os, is_mobile, pageviews, country, fullVisitorId FROM \`bqml_lab.eval_data\`
  ))
GROUP BY fullVisitorId
ORDER BY total_predicted_purchases DESC
LIMIT 10;
" &

# 5. Wait for all 3 background jobs to finish (NEW STEP)
wait

# 6. Clean up the temporary table (NEW STEP)
echo "--- 4. Cleaning up temporary table... ---"
bq rm -f bqml_lab.eval_data

echo "--- All tasks complete. ---"
