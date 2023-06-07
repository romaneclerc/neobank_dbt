with
    fixed as (
        select
            *,
            rank() over (partition by user_id order by created_date) as numbered_actions
        from `jovial-rex-384219.neobank.total_timeline`
        order by user_id, created_date
    ),

    shifted_timeline as (
        select
            a.*,
            b.user_id as user_id_1,
            b.created_date as created_date_1,
            b.transactions_type as transactions_type_1,
            b.reason as reason_1
        from fixed a
        left join
            fixed b
            on a.user_id = b.user_id
            and a.numbered_actions = b.numbered_actions + 1
    ),
    all_description as (
        select
            *,
            datetime_diff(
                created_date, created_date_1, hour
            ) as hours_since_last_activity,
            case
                when reason is null and transactions_type is null
                then "creation_of_account"
                when reason_1 is not null and transactions_type is not null
                then "1_transaction_after_notif"
                when reason_1 is null and transactions_type_1 is null and reason is null
                then "transaction_after_creation"
                when
                    reason_1 is null
                    and transactions_type_1 is null
                    and reason is not null
                then "notif_after_creation"
                when reason_1 is null and transactions_type is not null
                then "transaction_after_transaction"
                when reason_1 is not null and reason is not null
                then "consecutive_notif"
                else null
            end as description,
            if
            (
                reason_1 is not null and reason is not null, 1, 0
            ) as consecutive_notification
        from shifted_timeline
        order by user_id, created_date
    )

    ,extract_time as (
        select
            user_id,
            sum(consecutive_notification) * (-1) as sum_consecutive_notification,
            avg(
                if
                (
                    description = "1_transaction_after_notif",
                    datetime_diff(created_date, created_date_1, hour),
                    null
                )
            ) as time_between_transaction_notif
        from all_description
        group by user_id
    )

, notif_class_user as(
    SELECT 
        user_id
        , CASE WHEN sum_consecutive_notification = 0 then 5
                when sum_consecutive_notification = -1 then 1
                when sum_consecutive_notification = -2 then 0
                when sum_consecutive_notification = -3 then -2
                else -5 end as consecutive_notification
    from extract_time)

, plan as (
        select
            user_id,
            case
                when plan = 'STANDARD'
                then 1
                when plan = 'PREMIUM'
                then 2
                when plan = 'METAL'
                then 3
                when plan = 'METAL_FREE'
                then 3
                when plan = 'PREMIUM_OFFER'
                then 2
                when plan = 'PREMIUM_FREE'
                then 2
            end as plan_score
        from `dbt_rclerc_user1.user_dash`
    ),
    receptive_notif as (
        select
            user_id,
            time.time_between_transaction_notif,
            dbt.avg_day_between_transaction * 24 as avg_time_between_transaction,
            (
                (
                    dbt.avg_day_between_transaction * 24
                    - time.time_between_transaction_notif
                )
            ) as difference_in_avg_time,
        from extract_time time
        left join neobank.churn_month_dbt dbt using (user_id)
    )

    , receptive_notif_class as (
        SELECT
            user_id
            , CASE WHEN difference_in_avg_time >= -24 and difference_in_avg_time <= 24 then 1
                when difference_in_avg_time >24 and difference_in_avg_time <= 168 then 3
                when difference_in_avg_time >168 then 5
                when difference_in_avg_time <-24 and difference_in_avg_time >= -168 then -3
                when difference_in_avg_time <-168 then -5
                end as receptive_notif_in_hours
    from receptive_notif
    )

    , nb_country as (
        select
            t.user_id,
            t.transaction_id,
            u.country,
            t.ea_merchant_country,
            case
                when t.ea_merchant_country is null
                then null
                when c.alpha_2_code = u.country
                then 0
                else 1
            end as is_international_country,
        from `neobank.transactions` as t
        left join `neobank.users` as u on u.user_id = t.user_id
        join `neobank.country_code` as c on c.alpha_3_code = t.ea_merchant_country
        where t.transactions_state = 'COMPLETED'
    ),
    nb_country_user as (
        select
            user_id,
            count(
                distinct case
                    when is_international_country = 1 then ea_merchant_country
                end
            ) as nb_international_country
        from nb_country
        group by 1
    )

select
    user_id,
    n.consecutive_notification,
    r.receptive_notif_in_hours,
    u.days_sub_last as life_time_in_days,
    p.plan_score as type_of_card,
    u.nb_transactions,
    nb.nb_international_country
from notif_class_user n
left join neobank.churn_month_dbt dbt using (user_id)
left join receptive_notif_class r using (user_id)
left join `dbt_rclerc_user1.user_dash` u using (user_id)
left join plan p using (user_id)
left join nb_country_user nb using (user_id)