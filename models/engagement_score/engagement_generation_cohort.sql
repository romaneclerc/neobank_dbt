SELECT 
  a.* 
  , b.country
  , b.generation
  , b.cohort
  , b.user_settings_crypto_unlocked
  , b.num_contacts
  , b.devices_type
  , b.ratio_international
FROM `jovial-rex-384219.dbt_rclerc_user1.engagement_score_by_user` a
LEFT JOIN `dbt_rclerc_user1.user_dash_cohort` b on a.user_id = b.user_id