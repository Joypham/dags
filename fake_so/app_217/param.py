QUERY_GET_USING_TIME = """
    call ub_holistics.pcd_get_using_time();
"""
QUERY_GET_CONDITION_BY_GIFTDETAIL_ID = """
        with cte as (
        SELECT
            cart_detail.app_id,
            cart_detail.ID as cart_detail_id,
            gift_detail.price,
            tmp_final.using_time,
            case
            when tmp_final.using_time = '1970-01-01 00:00:00' then 'not used'
            when tmp_final.using_time is null then 'unknown'
            else 'used'
            end as using_type,
            gift_detail.id as gift_detail_id,
                    "public".convert_timezone(cart_detail.created) as change_time
        FROM
        urbox.cart_detail
            Join urbox.cart on cart.id = cart_detail.cart_id and cart.status = 2 and cart.pay_status = 2 and cart.delivery <> 4
            join urbox.gift_detail on gift_detail.id = cart_detail.gift_detail_id
            Join #tmp_final tmp_final on tmp_final.cart_detail_id = cart_detail.id
        WHERE
            cart_detail.status = 2
            and
            cart_detail.pay_status = 2
            AND
            app_id = 217
    ),
    cte2 as (
        SELECT
            cte.gift_detail_id,
            COUNT(DISTINCT cte.cart_detail_id) as no_change,
            sum(case when cte.using_type = 'used' then 1 else 0 end ) no_used,
            sum(case when cte.using_type = 'unknown' then 1 else 0 end ) unknown_used
        from cte
        GROUP BY
            cte.gift_detail_id
    ),
	cte3 as (
        SELECT
            gift_detail.id as gift_detail_id,
            gift_detail.title,
            gift_detail.price,
            cte2.no_change,
            cte2.no_used,
            cast(100*(cast(cte2.no_used as FLOAT) / cast(cte2.no_change as FLOAT)) as int) as conversion_rate
        FROM
            cte2
            join urbox.gift_detail on gift_detail.id = cte2.gift_detail_id
            join urbox.gift on gift_detail.gift_id = gift.id
            join urbox.brand on brand.id = gift.brand_id
        -- loại code mua đứt và code không xác định được thời gian sử dụng (có tỷ lệ convert = 100%)
            JOIN urbox.supplier on brand.supplier_id = supplier.id and supplier.crosscheck NOT IN ( 1, 3)
        WHERE
            cte2.unknown_used = '0'
        ORDER BY
            conversion_rate desc 
                ),
    cte4 as (
        SELECT    
            cte.gift_detail_id,
            max(datediff(second, cast(cte.change_time as datetime), cast(cte.using_time as datetime))) as max_second_diff,
            min(datediff(second, cast(cte.change_time as datetime), cast(cte.using_time as datetime))) as min_second_diff,
            round(avg(datediff(second, cast(cte.change_time as datetime), cast(cte.using_time as datetime))), 0) as mean_second_diff,
            round(median(datediff(second, cast(cte.change_time as datetime), cast(cte.using_time as datetime))), 0) as median_second_diff,
            round(percentile_cont(0.25) within group (order by datediff(second, cast(cte.change_time as datetime), cast(cte.using_time as datetime))) , 0) as percentile_25,
            round(percentile_cont(0.75) within group (order by datediff(second, cast(cte.change_time as datetime), cast(cte.using_time as datetime))) , 0) as percentile_75
        from cte
            join urbox.gift_detail on gift_detail.id = cte.gift_detail_id
            join urbox.gift on gift_detail.gift_id = gift.id
            join urbox.brand on brand.id = gift.brand_id
            LEFT JOIN urbox.supplier on brand.supplier_id = supplier.id and supplier.crosscheck in( 1, 3)
        WHERE
            using_type = 'used'
            AND
            -- loại bỏ code mua đứt thời gian sử dụng == đổi
            supplier.id is NULL
        GROUP BY cte.gift_detail_id
        )
    SELECT 
        cte3.*, 
        cte4.max_second_diff,
        cte4.min_second_diff,
        cte4.mean_second_diff,
        cte4.median_second_diff,
        cte4.percentile_25,
        cte4.percentile_75
    from cte3
    LEFT JOIN cte4 on cte4.gift_detail_id = cte3.gift_detail_id
"""
QUERY_GET_CATDETAIL_TO_FAKE_USING_TIME = """
    SELECT 
        cart_redemption.cart_detail_id,
        cart_redemption.gift_detail_id,
        cart_redemption.transaction_time,
        cart_redemption.expired_time
    from ub_holistics.cart_redemption
    WHERE
        app_id = 217
        AND
        using_time = '1970-01-01 00:00:00'
    order by random()
"""

QUERY_GET_USED_RATE = """
        SELECT 
            sum(cart_redemption.money) as changed_amount,
            sum(CASE
                        WHEN cart_redemption.using_time = '1970-01-01' then 0
                        ELSE cart_redemption.money
                    END) as used_amount	
        from ub_holistics.cart_redemption
            join urbox.gift on gift.id = cart_redemption.gift_id and gift.code_type in (1,3,6)
        WHERE
            app_id = 217
            AND
            cast(transaction_time as date) > '2019-12-31'
"""

QUERY_GET_CONVERSION_RATE = """
    SELECT
        gift_detail_id,
        title,
        price,
        no_change,
        no_used,
        conversion_rate
        
    FROM
        fake_so.condition_by_gift_detail_id
    WHERE
        conversion_rate <> 0
    ORDER BY
        conversion_rate DESC

"""

QUERY_GET_LOW_CONVERSION_RATE = """
    SELECT
            gift_detail_id,
            title,
            price,
            no_change,
            no_used,
            CASE 
                WHEN price >= 1000 and no_used <> 0  THEN 3
                WHEN price >= 1000 and no_used = 0  THEN 2
                WHEN price < 1000 and no_used <> 0 THEN 1                                
                ELSE 0.5
            END conversion_rate        
        FROM
            fake_so.condition_by_gift_detail_id 
            WHERE
            conversion_rate = 0
        ORDER BY
            conversion_rate DESC
"""

"""
INSERT into fake_so.using_time_fake (cart_detail_id, using_time)
with cte as (
SELECT 
		cart_redemption.cart_detail_id,
		cart_redemption.gift_detail_id,
		cart_redemption.transaction_time,
		cart_redemption.expired_time,
		cart_redemption.using_time,
		cart_redemption.money,
		ROW_NUMBER ( ) OVER ( PARTITION BY cart_redemption.gift_detail_id ORDER BY random() ) AS row_num
from ub_holistics.cart_redemption
WHERE
		app_id = 217
		AND
		using_time = '1970-01-01 00:00:00'
    ),
		cte2 as (		
		SELECT 
		cte.*, 
		num_of_trans_to_fake.num_of_trans_to_fake,
		condition_by_gift_detail_id.PERCENTILE_25,
		condition_by_gift_detail_id.PERCENTILE_75,		
		condition_by_gift_detail_id.min_second_diff,
		cast(random() * (cast(condition_by_gift_detail_id.PERCENTILE_75 as int) - cast(condition_by_gift_detail_id.PERCENTILE_25 as int)) + cast(condition_by_gift_detail_id.PERCENTILE_25 as int) as int) as second_diff
		from cte
		join fake_so.num_of_trans_to_fake on num_of_trans_to_fake.gift_detail_id = cte.gift_detail_id and cte.row_num <= num_of_trans_to_fake.num_of_trans_to_fake
		LEFT JOIN fake_so.condition_by_gift_detail_id on num_of_trans_to_fake.gift_detail_id = condition_by_gift_detail_id.gift_detail_id
		ORDER BY
		cte.gift_detail_id
		)
		SELECT 
		cte2.cart_detail_id,		
		CASE 
			WHEN dateadd(SECOND,cast(cte2.min_second_diff as int),cte2.transaction_time) > cast(cte2.expired_time as datetime) THEN cast('1970-01-01 00:00:00' as date)
			WHEN dateadd(SECOND,cte2.second_diff,cte2.transaction_time) > cast(cte2.expired_time as datetime) THEN cast(dateadd(SECOND,cast(cte2.min_second_diff as int),cte2.transaction_time) as date)						 
			ELSE cast(dateadd(SECOND,cte2.second_diff,cte2.transaction_time) as date)
		END fake_using_time
		from cte2
"""