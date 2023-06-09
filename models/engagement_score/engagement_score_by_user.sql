WITH avg_stdev as (
  SELECT
      AVG(consecutive_notification) as avg_consecutive_notification
    , STDDEV(consecutive_notification) as stdev_consecutive_notification
    , AVG(receptive_notif_in_hours) as avg_receptive_notif_in_hours
    , STDDEV (receptive_notif_in_hours) as stdev_receptive_notif_in_hours
    , AVG(Nb_trans_life_time) as avg_Nb_trans_life_time
    , STDDEV(Nb_trans_life_time) as stdev_Nb_trans_life_time
    , AVG(type_of_card) as avg_type_of_card
    , STDDEV(type_of_card) as stdev_type_of_card
  from `dbt_rclerc_user1.engagement_score_KPI`)

, normalization as (
    SELECT
      user_id
    , IF(consecutive_notification IS NULL,0,(consecutive_notification-avg_consecutive_notification)/stdev_consecutive_notification) as consecutive_notification_score
    , IF(receptive_notif_in_hours IS NULL, 0, (receptive_notif_in_hours-avg_receptive_notif_in_hours)/stdev_receptive_notif_in_hours) as receptive_notif_in_hours_score
    , IF(Nb_trans_life_time IS NULL, 0, (Nb_trans_life_time-avg_Nb_trans_life_time)/stdev_Nb_trans_life_time) as Nb_trans_life_time_score
    , (type_of_card-avg_type_of_card)/stdev_type_of_card as type_of_card_score  
  FROM `dbt_rclerc_user1.engagement_score_KPI` a
  LEFT JOIN avg_stdev b on a.user_id IS NOT NULL
  )

, score as (
  SELECT
  user_id
  , consecutive_notification_score + receptive_notif_in_hours_score + type_of_card_score + Nb_trans_life_time_score as engagement_score
FROM normalization)

, min_max as (
  SELECT 
    min(engagement_score) as min_score
    , max(engagement_score) as max_score
  FROM score)

SELECT
  a.user_id
  , (engagement_score-min_score)/(max_score-min_score) *100 as engagement_score
  , b.country
  , b.generation
  , b.cohort
  , b.user_settings_crypto_unlocked
  , b.num_contacts
  , b.devices_type
  , b.ratio_international
FROM score a
LEFT JOIN min_max on a.user_id IS NOT NULL
LEFT JOIN `dbt_rclerc_user1.user_dash` b on a.user_id = b.user_id