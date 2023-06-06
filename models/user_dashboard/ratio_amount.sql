WITH transac_type as (
  SELECT
  trans.user_id,
  trans.amount_usd,
  trans.transaction_id,
  usr.country,
  trans.transactions_type,
  trans.ea_merchant_country,
  CASE
    WHEN trans.ea_merchant_country IS NULL THEN 0
    WHEN C.alpha_2_code = usr.country THEN 0
  ELSE
  trans.amount_usd
END
  AS international_amount,
  CASE
    WHEN C.alpha_2_code = usr.country THEN amount_usd
  ELSE
  0
END
  AS national_amount,
FROM
  `neobank.transactions` AS trans
LEFT JOIN
  `neobank.users` AS usr ON usr.user_id = trans.user_id
JOIN
  `neobank.country_code` AS C ON C.alpha_3_code = trans.ea_merchant_country
WHERE
  trans.transactions_state = 'COMPLETED')

SELECT 
  user_id,
  sum(international_amount) as international_amount,
  sum(national_amount) as national_amount,
  sum(amount_usd) as global_amount,
  sum(international_amount)/sum(amount_usd) as ratio_international,
  sum(national_amount)/sum(amount_usd) as ratio_national
FROM transac_type
GROUP BY user_id