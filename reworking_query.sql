{{ config(
          materialized = 'incremental'
        , unique_key= ['customerid','payment_month','channel', 'sub_channel']
    )
}}

with money_movement as (
    select
        customerid,
        payment_month,
        dateadd(month, 1, payment_month) as month_1,
        dateadd(month, 2, payment_month) as month_2,
        dateadd(month, 3, payment_month) as month_3,
        dateadd(month, 4, payment_month) as month_4,
        dateadd(month, 5, payment_month) as month_5,
        dateadd(month, 6, payment_month) as month_6,
        dateadd(month, 7, payment_month) as month_7,
        dateadd(month, 8, payment_month) as month_8,
        dateadd(month, 9, payment_month) as month_9,
        dateadd(month, 10, payment_month) as month_10,
        dateadd(month, 11, payment_month) as month_11,
        channel,
        sub_channel,
        sum(COALESCE(money_moved,0)) as money_moved
    FROM {{ ref('int_bdc_money_movement_ap_ar_adj') }}

    {% if is_incremental() %}
        where payment_month >= (SELECT MAX(payment_month) FROM {{ this }})
    {% endif %}

    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
    order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
)
    select
        m0.customerid,
        m0.payment_month,
        m0.channel,
        m0.sub_channel,
        m0.money_moved as money_moved_current_month,
        case
            when m1.money_moved is null then 0
            else m1.money_moved
        end as money_moved_past_1_month,
        case
            when m2.money_moved is null then 0
            else m2.money_moved
        end as money_moved_past_2_month,
        case
            when m3.money_moved is null then 0
            else m3.money_moved
        end as money_moved_past_3_month,
        case
            when m4.money_moved is null then 0
            else m4.money_moved
            end as money_moved_past_4_month,
        case
            when m5.money_moved is null then 0
            else m5.money_moved
        end as money_moved_past_5_month,
        case
            when m6.money_moved is null then 0
            else m6.money_moved
        end as money_moved_past_6_month,
        case
            when m7.money_moved is null then 0
            else m7.money_moved
        end as money_moved_past_7_month,
        case
            when m8.money_moved is null then 0
            else m8.money_moved
        end as money_moved_past_8_month,
        case
            when m9.money_moved is null then 0
            else m9.money_moved
        end as money_moved_past_9_month,
        case
            when m10.money_moved is null then 0
            else m10.money_moved
        end as money_moved_past_10_month,
        case
            when m11.money_moved is null then 0
            else m11.money_moved
        end as money_moved_past_11_month,
        case when money_moved_current_month=0 then  0 else 1 end +
        case when money_moved_past_1_month =0 then  0 else 1 end +
        case when money_moved_past_2_month =0 then  0 else 1 end +
        case when money_moved_past_3_month =0 then  0 else 1 end +
        case when money_moved_past_4_month =0 then  0 else 1 end +
        case when money_moved_past_5_month =0 then  0 else 1 end
        as paymonth_count_0_to_6,
        case when money_moved_past_6_month =0 then  0 else 1 end +
        case when money_moved_past_7_month =0 then  0 else 1 end +
        case when money_moved_past_8_month =0 then  0 else 1 end +
        case when money_moved_past_9_month =0 then  0 else 1 end +
        case when money_moved_past_10_month =0 then  0 else 1 end +
        case when money_moved_past_11_month =0 then  0 else 1 end
        as paymonth_count_7_to_12

    from money_movement m0
    left join money_movement m1
        on m0.customerid=m1.customerid
               and m0.channel=m1.channel
               and m0.sub_channel=m1.sub_channel
               and m0.payment_month=m1.month_1
    left join money_movement m2
        on m0.customerid=m2.customerid
               and m0.channel=m2.channel
               and m0.sub_channel=m2.sub_channel
               and m0.payment_month=m2.month_2
    left join money_movement m3
        on m0.customerid=m3.customerid
               and m0.channel=m3.channel
               and m0.sub_channel=m3.sub_channel
               and m0.payment_month=m3.month_3
    left join money_movement m4
        on m0.customerid=m4.customerid
               and m0.channel=m4.channel
               and m0.sub_channel=m4.sub_channel
               and m0.payment_month=m4.month_4
    left join money_movement m5
        on m0.customerid=m5.customerid
               and m0.channel=m5.channel
               and m0.sub_channel=m5.sub_channel
               and m0.payment_month=m5.month_5
    left join money_movement m6
        on m0.customerid=m6.customerid
               and m0.channel=m6.channel
               and m0.sub_channel=m6.sub_channel
               and m0.payment_month=m6.month_6
    left join money_movement m7
        on m0.customerid=m7.customerid
               and m0.channel=m7.channel
               and m0.sub_channel=m7.sub_channel
               and m0.payment_month=m7.month_7
    left join money_movement m8
        on m0.customerid=m8.customerid
               and m0.channel=m8.channel
               and m0.sub_channel=m8.sub_channel
               and m0.payment_month=m8.month_8
    left join money_movement m9
        on m0.customerid=m9.customerid
               and m0.channel=m9.channel
               and m0.sub_channel=m9.sub_channel
               and m0.payment_month=m9.month_9
    left join money_movement m10
        on m0.customerid=m10.customerid
               and m0.channel=m10.channel
               and m0.sub_channel=m10.sub_channel
               and m0.payment_month=m10.month_10
    left join money_movement m11
        on m0.customerid=m11.customerid
               and m0.channel=m11.channel
               and m0.sub_channel=m11.sub_channel


{{
    config(
          materialized='incremental',
          incremental_strategy='merge',
          on_schema_change='sync_all_columns',
          unique_key=['customerid','payment_month','channel', 'sub_channel'],
          dist='customerid',
          sort=['customerid','payment_month','channel', 'sub_channel']
    )
}}

WITH
    money_movement_base
AS (
    SELECT
        customerid,
        payment_month,
        channel,
        sub_channel,
        SUM(COALESCE(money_moved,0)) as money_moved
    FROM
        {{ ref('money_movement_ap_ar_adj') }}
    WHERE
        True
        {% if is_incremental() %}
        AND payment_month >= (SELECT MAX(payment_month) FROM {{ this }})
        {% endif %}
    GROUP BY
        1,2,3,4
)
    SELECT
        m0.customerid,
        m0.payment_month,
        m0.channel,
        m0.sub_channel,
        m0.money_moved                                  AS money_moved_current_month,
        COALESCE(m1.money_moved, 0)                     AS money_moved_past_1_month,
        COALESCE(m2.money_moved, 0)                     AS money_moved_past_2_month,
        COALESCE(m3.money_moved, 0)                     AS money_moved_past_3_month,
        COALESCE(m4.money_moved, 0)                     AS money_moved_past_4_month,
        COALESCE(m5.money_moved, 0)                     AS money_moved_past_5_month,
        COALESCE(m6.money_moved, 0)                     AS money_moved_past_6_month,
        COALESCE(m7.money_moved, 0)                     AS money_moved_past_7_month,
        COALESCE(m8.money_moved, 0)                     AS money_moved_past_8_month,
        COALESCE(m9.money_moved, 0)                     AS money_moved_past_9_month,
        COALESCE(m10.money_moved, 0)                    AS money_moved_past_10_month,
        COALESCE(m11.money_moved, 0)                    AS money_moved_past_11_month,
        case when money_moved_current_month=0
            then  0 else 1 end +
        case when money_moved_past_1_month =0
            then  0 else 1 end +
        case when money_moved_past_2_month =0
            then  0 else 1 end +
        case when money_moved_past_3_month =0
            then  0 else 1 end +
        case when money_moved_past_4_month =0
            then  0 else 1 end +
        case when money_moved_past_5_month =0
            then  0 else 1 end
                                                        AS paymonth_count_0_to_6,
        case when money_moved_past_6_month =0
            then  0 else 1 end +
        case when money_moved_past_7_month =0
            then  0 else 1 end +
        case when money_moved_past_8_month =0
            then  0 else 1 end +
        case when money_moved_past_9_month =0
            then  0 else 1 end +
        case when money_moved_past_10_month =0
            then  0 else 1 end +
        case when money_moved_past_11_month =0
            then  0 else 1 end
                                                        AS paymonth_count_7_to_12
FROM
    money_movement_base m0
LEFT JOIN
    money_movement_base m1
    ON m0.customerid = m1.customerid
    AND m0.channel = m1.channel
    AND m0.sub_channel = m1.sub_channel
    AND m0.payment_month = DATEADD(month, 1, m1.payment_month)
LEFT JOIN
    money_movement_base m2
    ON m0.customerid = m2.customerid
    AND m0.channel = m2.channel
    AND m0.sub_channel = m2.sub_channel
    AND m0.payment_month = DATEADD(month, 2, m2.payment_month)
LEFT JOIN
    money_movement_base m3
    ON m0.customerid = m3.customerid
    AND m0.channel = m3.channel
    AND m0.sub_channel = m3.sub_channel
    AND m0.payment_month = DATEADD(month, 3, m3.payment_month)
LEFT JOIN
    money_movement_base m4
    ON m0.customerid = m4.customerid
    AND m0.channel = m4.channel
    AND m0.sub_channel = m4.sub_channel
    AND m0.payment_month = DATEADD(month, 4, m4.payment_month)
LEFT JOIN
    money_movement_base m5
    ON m0.customerid = m5.customerid
    AND m0.channel = m5.channel
    AND m0.sub_channel = m5.sub_channel
    AND m0.payment_month = DATEADD(month, 5, m5.payment_month)
LEFT JOIN
    money_movement_base m6
    ON m0.customerid = m6.customerid
    AND m0.channel = m6.channel
    AND m0.sub_channel = m6.sub_channel
    AND m0.payment_month = DATEADD(month, 6, m6.payment_month)
LEFT JOIN
    money_movement_base m7
    ON m0.customerid = m7.customerid
    AND m0.channel = m7.channel
    AND m0.sub_channel = m7.sub_channel
    AND m0.payment_month = DATEADD(month, 7, m7.payment_month)
LEFT JOIN
    money_movement_base m8
    ON m0.customerid = m8.customerid
    AND m0.channel = m8.channel
    AND m0.sub_channel = m8.sub_channel
    AND m0.payment_month = DATEADD(month, 8, m8.payment_month)
LEFT JOIN
    money_movement_base m9
    ON m0.customerid = m9.customerid
    AND m0.channel = m9.channel
    AND m0.sub_channel = m9.sub_channel
    AND m0.payment_month = DATEADD(month, 9, m9.payment_month)
LEFT JOIN
    money_movement_base m10
    ON m0.customerid = m10.customerid
    AND m0.channel = m10.channel
    AND m0.sub_channel = m10.sub_channel
    AND m0.payment_month = DATEADD(month, 10, m10.payment_month)
LEFT JOIN
    money_movement_base m11
    ON m0.customerid = m11.customerid
    AND m0.channel = m11.channel
    AND m0.sub_channel = m11.sub_channel
    AND m0.payment_month = DATEADD(month, 11, m11.payment_month)






