with
    fixed as (
        select
            *,
            rank() over (partition by user_id order by created_date) as numbered_actions
        from `jovial-rex-384219.neobank.total_timeline`
        where reason is null
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
    int_ttt_view as (
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
            end as description
        from shifted_timeline
        order by user_id, created_date
    ),
    int_transaction_quartile as (
        select
            *,
            ntile(5) over (
                partition by user_id order by hours_since_last_activity
            ) as quartile
        from int_ttt_view
    ),
    int_metodo_churn as (
        with
            avg_t_time as (
                select
                    user_id,
                    max(
                        case when quartile = 4 then hours_since_last_activity / 24 end
                    ) as _80th_percentile,
                    avg(datetime_diff(created_date, created_date_1, hour))
                    / 24 as avg_day_between_transaction,
                from int_transaction_quartile
                group by user_id
            ),
            joined as (
                select
                    avge.*,
                    date_diff(
                        '2019-05-16', user.last_action_date, day
                    ) as days_since_last_transaction,
                    date_add(
                        user.last_action_date,
                        interval cast(
                            round(
                                if
                                (
                                    _80th_percentile is null,
                                    avg_day_between_transaction,
                                    _80th_percentile
                                ),
                                0
                            ) as int64
                        ) day
                    ) as churned_date
                from avg_t_time avge
                left join `dbt_rclerc_user1.user_dash` user using (user_id)
            )
        select *
        from joined
    )

select
    churn.*,
    firstt.created_date,
    user.last_action_date,
    case
        when churned_date is null
        then 0
        when churned_date > '2019-05-16'
        then null
        else date_diff(churned_date, date(created_date), month)
    end as month_when_churned,
    c.cohort,
from int_metodo_churn churn
left join `neobank.users` firstt using (user_id)
left join `dbt_rclerc_user1.user_dash` user using (user_id)
left join `dbt_rclerc_user1.user_cohort` c using (user_id)
