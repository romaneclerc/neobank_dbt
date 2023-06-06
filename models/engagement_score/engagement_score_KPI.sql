WITH
  fixed AS (
  SELECT
    *,
    RANK() OVER(PARTITION BY user_id ORDER BY created_date) AS numbered_actions
  FROM
    `jovial-rex-384219.neobank.total_timeline`
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
    AND a.numbered_actions = b.numbered_actions +1),
  all_description AS (
  SELECT
    *,
    DATETIME_DIFF(created_date,created_date_1, hour) AS hours_since_last_activity,
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
    AS description,
  IF
    (reason_1 IS NOT NULL
      AND reason IS NOT NULL, 1, 0) AS consecutive_notification
  FROM
    shifted_timeline
  ORDER BY
    user_id,
    created_date),
  extract_time AS (
  SELECT
    user_id,
    SUM(consecutive_notification)*(-1) AS sum_consecutive_notification,
    AVG(
    IF
      (description = "1_transaction_after_notif",DATETIME_DIFF(created_date,created_date_1,hour),NULL)) AS time_between_transaction_notif
  FROM
    all_description
  GROUP BY
    user_id)

, plan as (
  SELECT
    user_id,
    case when plan = 'STANDARD' then 1
          when plan = 'PREMIUM' then 2
          when plan = 'METAL' then 3
          when plan = 'METAL_FREE' then 3
          when plan = 'PREMIUM_OFFER' then 2
          when plan = 'PREMIUM_FREE' then 2 end as plan_score
from `dbt_rclerc_user1.user_dash`
)

SELECT
  time.*,
  dbt.avg_day_between_transaction*24 AS avg_time_between_transaction,
  ((dbt.avg_day_between_transaction*24-time.time_between_transaction_notif)) AS difference_in_avg_time,
  u.days_sub_last,
  p.plan_score,
  u.nb_transactions
FROM
  extract_time time
LEFT JOIN
  neobank.churn_month_dbt dbt
USING
  (user_id)
LEFT JOIN `dbt_rclerc_user1.user_dash` u
USING
  (user_id)
LEFT JOIN plan p
USING
  (user_id)