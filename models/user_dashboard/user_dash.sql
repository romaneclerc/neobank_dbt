with
    transactions_info as (
        select
            user_id,
            min(date(created_date)) as first_action_date,
            max(date(created_date)) as last_action_date,
            count(transaction_id) as nb_transactions
        from `neobank.transactions_devices`
        where transactions_state = 'COMPLETED'
        group by 1
    )

, cohort as (
    select
    user_id
    , PARSE_DATE("%Y-%m", CONCAT(extract (year from created_date), "-", extract (month from created_date))) AS cohort
    from `neobank.users_devices`
)

select
    u.user_id,
    2023 - u.birth_year as age,
    case
        when u.birth_year <= 1945
        then 'Silent Generation'
        when u.birth_year >= 1946 and u.birth_year <= 1964
        then 'Baby-boomers'
        when u.birth_year >= 1965 and u.birth_year <= 1980
        then 'Generation X'
        when u.birth_year >= 1981 and u.birth_year <= 1996
        then 'Generation Y'
        when u.birth_year >= 1997 and u.birth_year <= 2010
        then 'Generation Z'
        when u.birth_year >= 2011
        then 'Generation Alpha'
    end as generation,
    u.country,
    u.city,
    date(u.created_date) as subscription_date,
    c.cohort,
    t.first_action_date,
    t.last_action_date,
    date_diff(first_action_date, date(u.created_date), day) as days_sub_first,
    date_diff(last_action_date, date(u.created_date), day) as days_sub_last,
    u.user_settings_crypto_unlocked,
    u.plan,
    u.attributes_notifications_marketing_push,
    u.attributes_notifications_marketing_email,
    u.num_contacts,
    u.num_referrals,
    u.num_successful_referrals,
    u.devices_type,
    t.nb_transactions,
    r.ratio_international
from `neobank.users_devices` u
left join transactions_info t on t.user_id = u.user_id
left join `dbt_rclerc_user1.ratio_amount` r on r.user_id = u.user_id
left join cohort c on c.user_id = u.user_id