select
  user_id
 , CONCAT(extract (year from created_date), "-", extract (month from created_date)) as cohort
from `neobank.users_devices`