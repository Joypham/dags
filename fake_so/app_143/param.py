
QUERY_GET_CARD_NO_TRANS = """
    SELECT 
        card_holistic.created as card_created,
        card_holistic.issue_id, 
        card_holistic.app_id, 
        card_holistic."number" as card_number,
        card_holistic.phone_sub,
        card_holistic.card_id
    from ub_holistics.card_holistic
    WHERE
        card_holistic.first_transaction is null
        AND
        card_holistic.app_id = 143
        AND card_holistic.money_topup = 20000
        AND cast(card_holistic.created as date) BETWEEN cast('2022-01-01' as date) and cast('2022-03-31' as date)
        limit 500
"""

QUERY_GET_RANDOM_CART_DETAIL = """
    SELECT 
        card_trans_type,
        transaction_id,
        site_user_id,
        delivery,
        cart_id,
        receiver_id,
        money_ship,
        money_gift,
        money_fee,
        money_coupon,
        cart_detail_money,
        cart_detail_id,
        gift_id,
        category_title,
        brand_title,
        quantity,
        using_time,
        code,
        code_using,
        cart_detail_created_at,
        created_at
    from ub_holistics.cart_detail_holistics
    WHERE 
        cart_detail_money = 20000
        AND app_id = 143
        AND cast(cart_detail_holistics.cart_detail_created_at as date) BETWEEN cast('2022-01-01' as date) and cast('2022-03-31' as date)
    ORDER BY RANDOM()
    limit 500
"""

QUERY_GET_PERCENTILE = """
    SELECT 
    cart_detail_holistics.gift_id,
    max(datediff(second, cast(card_holistic.created as datetime), cast(cart_detail_holistics.cart_detail_created_at as datetime))) as max_second_diff,
    min(datediff(second, cast(card_holistic.created as datetime), cast(cart_detail_holistics.cart_detail_created_at as datetime))) as min_second_diff,
    round(avg(datediff(second, cast(card_holistic.created as datetime), cast(cart_detail_holistics.cart_detail_created_at as datetime))), 0) as mean_second_diff,
    round(median(datediff(second, cast(card_holistic.created as datetime), cast(cart_detail_holistics.cart_detail_created_at as datetime))), 0) as median_second_diff,
    round(percentile_cont(0.25) within group (order by datediff(second, cast(card_holistic.created as datetime), cast(cart_detail_holistics.cart_detail_created_at as datetime))) , 0) as percentile_25,
    round(percentile_cont(0.75) within group (order by datediff(second, cast(card_holistic.created as datetime), cast(cart_detail_holistics.cart_detail_created_at as datetime))) , 0) as percentile_75
    from ub_holistics.card_holistic
    join ub_holistics.cart_detail_holistics on cart_detail_holistics.card_id = card_holistic.card_id
    WHERE
    card_holistic.first_transaction is not null
    and 
    card_holistic.money_topup = 20000
    AND
    card_holistic.app_id = 143
    AND cast(card_holistic.created as date) BETWEEN cast('2022-01-01' as date) and cast('2022-03-31' as date)
    GROUP BY
    cart_detail_holistics.gift_id
"""

QUERY_GET_REAL_DATA = """
    SELECT 
        issue_id,
        app_id,
        card_number,
        phone_sub,
        card_id,
        card_trans_type,
        transaction_id,
        site_user_id,
        delivery,
        cart_id,
        receiver_id,
        money_ship,
        money_gift,
        money_fee,
        money_coupon,
        cart_detail_money,
        cart_detail_id,
        gift_id,
        category_title,
        brand_title,
        quantity,
        using_time,
        code,
        code_using,
        cart_detail_created_at,
        created_at
    from  ub_holistics.cart_detail_holistics
    WHERE
    app_id = 143
"""