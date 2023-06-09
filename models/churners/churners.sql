WITH
  fixed AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY user_id ORDER BY created_date) AS numbered_actions
  FROM
    `jovial-rex-384219.neobank.total_timeline`
  WHERE
    reason IS NULL
  ORDER BY
    user_id,
    created_date ),
  shifted_timeline AS (
  SELECT
    a.*,
    b.user_id AS user_id_1,
    b.created_date AS created_date_1,
    b.transactions_type AS transactions_type_1,
    b.reason AS reason_1
  FROM
    fixed a
  LEFT JOIN
    fixed b
  ON
    a.user_id = b.user_id
    AND a.numbered_actions = b.numbered_actions + 1 ),
  int_ttt_view AS (
  SELECT
    *,
    DATETIME_DIFF( created_date, created_date_1, hour ) AS hours_since_last_activity,
    CASE
      WHEN reason IS NULL AND transactions_type IS NULL THEN "creation_of_account"
      WHEN reason_1 IS NOT NULL
    AND transactions_type IS NOT NULL THEN "1_transaction_after_notif"
      WHEN reason_1 IS NULL AND transactions_type_1 IS NULL AND reason IS NULL THEN "transaction_after_creation"
      WHEN reason_1 IS NULL
    AND transactions_type_1 IS NULL
    AND reason IS NOT NULL THEN "notif_after_creation"
      WHEN reason_1 IS NULL AND transactions_type IS NOT NULL THEN "transaction_after_transaction"
      WHEN reason_1 IS NOT NULL
    AND reason IS NOT NULL THEN "consecutive_notif"
    ELSE
    NULL
  END
    AS description
  FROM
    shifted_timeline
  ORDER BY
    user_id,
    created_date ),
  int_transaction_quartile AS (
  SELECT
    *,
    NTILE(5) OVER (PARTITION BY user_id ORDER BY hours_since_last_activity ASC ) AS quartile
  FROM
    int_ttt_view ),
  int_metodo_churn AS (
  WITH
    avg_t_time AS (
    SELECT
      user_id,
      MAX(
      IF
        (quartile = 4, hours_since_last_activity / 24,0) ) AS _80th_percentile,
      AVG(DATETIME_DIFF(created_date, created_date_1, hour)) / 24 AS avg_day_between_transaction,
    FROM
      int_transaction_quartile
    GROUP BY
      user_id ),
    joined AS (
    SELECT
      avge.*,
      DATE_DIFF( '2019-05-16', user.last_action_date, day ) AS days_since_last_transaction,
      DATE_ADD( user.last_action_date, INTERVAL CAST( ROUND(
          IF
            (_80th_percentile > avg_day_between_transaction,_80th_percentile,avg_day_between_transaction), 0 ) AS int64 ) day ) AS churned_date
    FROM
      avg_t_time avge
    LEFT JOIN
      `dbt_rclerc_user1.user_dash` user
    USING
      (user_id) )
  SELECT
    *
  FROM
    joined ),
  boolean_1 AS(
  SELECT
    churn.*,
    firstt.created_date,
    user.last_action_date,
    CASE
      WHEN churned_date IS NULL THEN 0
      WHEN churned_date > '2019-05-01' THEN NULL
    ELSE
    DATE_DIFF(churned_date, DATE(created_date), month)
  END
    AS month_when_churned,
    CASE
      WHEN DATE_DIFF(churned_date, DATE(created_date), day) =0 THEN 1
      WHEN churned_date > '2019-05-01' THEN null
    ELSE
    DATE_DIFF(churned_date, DATE(created_date), day)
  END
    AS day_when_churned,
    user.cohort,
  FROM
    int_metodo_churn churn
  LEFT JOIN
    `neobank.users` firstt
  USING
    (user_id)
  LEFT JOIN
    `dbt_rclerc_user1.user_dash` user
  USING
    (user_id))
SELECT
  *,
IF
  (day_when_churned IS NULL, 0,1) AS churned
FROM
  boolean_1