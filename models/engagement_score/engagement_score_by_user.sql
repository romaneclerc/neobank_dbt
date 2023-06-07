WITH avg_stdev as (
  SELECT
      AVG(consecutive_notification) as avg_consecutive_notification
    , STDDEV(consecutive_notification) as stdev_consecutive_notification
    , AVG(receptive_notif_in_hours) as avg_receptive_notif_in_hours
    , STDDEV (receptive_notif_in_hours) as stdev_receptive_notif_in_hours
    , AVG(life_time_in_days) as avg_life_time_in_days
    , STDDEV(life_time_in_days) as stdev_life_time_in_days
    , AVG(type_of_card) as avg_type_of_card
    , STDDEV(type_of_card) as stdev_type_of_card
    , AVG(nb_transactions) as avg_nb_transactions
    , STDDEV(nb_transactions) as stdev_nb_transactions
  from `dbt_rclerc_user1.engagement_score_KPI_V2`)

, normalization as (
    SELECT
      user_id
    , IF(consecutive_notification IS NULL,0,(consecutive_notification-avg_consecutive_notification)/stdev_consecutive_notification) as consecutive_notification_score
    , IF(receptive_notif_in_hours IS NULL, 0, (receptive_notif_in_hours-avg_receptive_notif_in_hours)/stdev_receptive_notif_in_hours) as receptive_notif_in_hours_score
    , IF(life_time_in_days IS NULL, 0, (life_time_in_days-avg_life_time_in_days)/stdev_life_time_in_days) as life_time_in_days_score
    , (type_of_card-avg_type_of_card)/stdev_type_of_card as type_of_card_score  
    , IF(nb_transactions IS NULL, 0, (nb_transactions-avg_nb_transactions)/stdev_nb_transactions) as nb_transactions_score
  FROM `dbt_rclerc_user1.engagement_score_KPI_V2` a
  LEFT JOIN avg_stdev b on a.user_id IS NOT NULL
  )

, score as (
  SELECT
  user_id
  , consecutive_notification_score + receptive_notif_in_hours_score + life_time_in_days_score + type_of_card_score + nb_transactions_score as engagement_score
FROM normalization)

, min_max as (
  SELECT 
    min(engagement_score) as min_score
    , max(engagement_score) as max_score
  FROM score)

SELECT
  a.user_id
  , (engagement_score-min_score)/(max_score-min_score) *100 as engagement_score
FROM score a
LEFT JOIN min_max on a.user_id IS NOT NULL