# https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-defining-constraints.html
# Chu y:
# Amazon Redshift does not enforce unique, primary-key, and foreign-key constraints, count distinct: not distinct nếu primary key bị duplicate
# https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html
# SELECT DISTINCT query might return duplicate rows if the primary key is not unique

gsheet_id = "1fYIT-6LmBjol8EeLKcDvtiOBQrtPJyoPDmzE-9vcOEk"
sheet_name = "total"
QUERY_GET_CART_DETAIL_FROM_BRAND = """
    WITH merchant_discount_type as (
        SELECT DISTINCT 
                id, 
                type, 
                calculate_type 
        FROM sentry.merchant_discount_test
        WHERE
                merchant_discount_test.calculate_type = '{calculate_type}' AND
                merchant_discount_test.type = 'brand'					
    )   
    SELECT 
            cart_redemption.brand_id AS id,
            merchant_discount_type.type,
            cart_redemption.cart_detail_id,
            cart_redemption.money as cart_detail_money,
    -- 		accumulated_amount theo từng đơn hàng
            sum(cart_redemption.money) over (PARTITION BY 
                    cart_redemption.brand_id,
                    CAST(discount_schedule_by_id.last_discount_period_start AS DATE)
                    ORDER BY CAST(cart_redemption.using_time AS DATETIME) asc rows unbounded preceding) as accumulated_amount_by_cart_detail,
    -- 		accumulated_amount theo brand_id				
            SUM(cart_redemption.money) OVER (PARTITION BY 
                    cart_redemption.brand_id,
                    CAST(discount_schedule_by_id.last_discount_period_start AS DATE),
                    CAST(discount_schedule_by_id.last_discount_period_end AS DATE) ) AS accumulated_amount_by_brand_id,
    -- 	  count cart_detail theo brand_id				
            COUNT(cart_redemption.cart_detail_id) OVER (PARTITION BY 
                    cart_redemption.brand_id,
                    CAST(discount_schedule_by_id.last_discount_period_start AS DATE),
                    CAST(discount_schedule_by_id.last_discount_period_end AS DATE) ) AS count_cart_detail_by_brand_id,                    
            CAST(cart_redemption.using_time AS DATETIME) as using_time,
            CAST(discount_schedule_by_id.last_discount_period_start AS DATE) AS discount_period_start,
            CAST(discount_schedule_by_id.last_discount_period_end AS DATE) AS discount_period_end
    FROM sentry.discount_schedule_by_id	
            JOIN merchant_discount_type ON merchant_discount_type.id = discount_schedule_by_id.id and merchant_discount_type.type = discount_schedule_by_id.type
            JOIN ub_holistics.cart_redemption ON cart_redemption.brand_id = merchant_discount_type.id		
    WHERE
            CAST(cart_redemption.using_time AS DATE) 
            BETWEEN CAST(discount_schedule_by_id.last_discount_period_start AS DATE) AND CAST(discount_schedule_by_id.last_discount_period_end AS DATE)
    ORDER BY cart_redemption.brand_id, CAST(cart_redemption.using_time AS DATE) ASC
""" # noqa
QUERY_GET_CART_DETAIL_FROM_SUPPLIER = """
    WITH merchant_discount_type as (
        SELECT DISTINCT 
            id, 
            type, 
            calculate_type 
        FROM sentry.merchant_discount_test
        WHERE
            merchant_discount_test.calculate_type = '{calculate_type}' AND 
            merchant_discount_test.type = 'supplier' 
              
    )    
    SELECT 
        brand.supplier_id AS id,
        cart_redemption.cart_detail_id,
        cart_redemption.money,
        SUM(cart_redemption.money) OVER (PARTITION BY 
                brand.supplier_id,
                CAST(discount_schedule_by_id.last_discount_period_start AS DATE),
                CAST(discount_schedule_by_id.last_discount_period_end AS DATE) ) AS accumulated_amount,
        CAST(cart_redemption.using_time AS DATE) as using_time,
        CAST(discount_schedule_by_id.last_discount_period_start AS DATE) AS discount_period_start,
        CAST(discount_schedule_by_id.last_discount_period_end AS DATE) AS discount_period_end
    FROM sentry.discount_schedule_by_id	
        JOIN urbox.brand ON brand.supplier_id = discount_schedule_by_id.id
        JOIN ub_holistics.cart_redemption ON cart_redemption.brand_id = brand.id
        JOIN merchant_discount_type ON merchant_discount_type.id = discount_schedule_by_id.id and merchant_discount_type.type = discount_schedule_by_id.type
    WHERE
        CAST(cart_redemption.using_time AS DATE) 
        BETWEEN CAST(discount_schedule_by_id.last_discount_period_start AS DATE) AND CAST(discount_schedule_by_id.last_discount_period_end AS DATE)
    ORDER BY cart_redemption.brand_id, CAST(cart_redemption.using_time AS DATE) ASC
     
"""  # noqa

QUERY_GET_DISCOUNT_INFO = """
    SELECT 
        id, 
        TYPE, 
        money_min, 
        discount_rate 
    FROM sentry.merchant_discount_test
    WHERE
        calculate_type = '{calculate_type}'
    ORDER BY
        merchant_discount_test.id,
        merchant_discount_test.type,
        merchant_discount_test.money_min ASC
"""

QUERY_GET_CART_DETAIL_TYPE_2_BY_GIFT_DETAIL = """
    INSERT INTO ub_rawdata.cart_detail_discount_rate (
    "id", "type", "cart_detail_id", "discount_info", "discount_rate", "calculate_type", "discount_period_start", "discount_period_end"
    )
    
    WITH merchant_discount_type as (
        SELECT DISTINCT 
                        id, 
                        type, 
                        discount_rate,
                        calculate_type
        FROM sentry.merchant_discount_test
        WHERE
                merchant_discount_test.calculate_type = '2'					
    )
    SELECT 
        cart_redemption.gift_detail_id as id,
        merchant_discount_type.type,
        cart_redemption.cart_detail_id,
        '{}' as discount_info,
        merchant_discount_type.discount_rate,
        merchant_discount_type.calculate_type as calculate_type,					
        CAST(discount_schedule_by_id.last_discount_period_start AS DATE) AS discount_period_start,
        CAST(discount_schedule_by_id.last_discount_period_end AS DATE) AS discount_period_end										
    FROM ub_holistics.cart_redemption
    JOIN merchant_discount_type on merchant_discount_type.id = cart_redemption.gift_detail_id AND merchant_discount_type.type = 'gift_detail'
    JOIN sentry.discount_schedule_by_id	 on cart_redemption.gift_detail_id = discount_schedule_by_id.id AND merchant_discount_type.type = 'gift_detail'  AND  CAST(cart_redemption.using_time AS DATE) BETWEEN CAST(discount_schedule_by_id.last_discount_period_start AS DATE) AND CAST(discount_schedule_by_id.last_discount_period_end AS DATE)
    WHERE
    cart_redemption.cart_detail_id not in (SELECT cart_detail_id from ub_rawdata.cart_detail_discount_rate)
"""

QUERY_GET_CART_DETAIL_TYPE_2_BY_BRAND = """
        INSERT INTO ub_rawdata.cart_detail_discount_rate (
        "id", "type", "cart_detail_id", "discount_info", "discount_rate", "calculate_type", "discount_period_start", "discount_period_end"
        )
        WITH merchant_discount_type as (
        SELECT DISTINCT 
                        id, 
                        type, 
                        discount_rate,
                        calculate_type
        FROM sentry.merchant_discount_test
        WHERE
                merchant_discount_test.calculate_type = '2'					
    )
    SELECT 
        cart_redemption.brand_id as id,
        merchant_discount_type.type,
        cart_redemption.cart_detail_id,
        '{}' as discount_info,
        merchant_discount_type.discount_rate,
        merchant_discount_type.calculate_type as calculate_type,					
        CAST(discount_schedule_by_id.last_discount_period_start AS DATE) AS discount_period_start,
        CAST(discount_schedule_by_id.last_discount_period_end AS DATE) AS discount_period_end										
    FROM ub_holistics.cart_redemption
    JOIN merchant_discount_type on merchant_discount_type.id = cart_redemption.brand_id AND merchant_discount_type.type = 'brand'
    JOIN sentry.discount_schedule_by_id	 on cart_redemption.brand_id = discount_schedule_by_id.id AND merchant_discount_type.type = 'brand'  AND  CAST(cart_redemption.using_time AS DATE) BETWEEN CAST(discount_schedule_by_id.last_discount_period_start AS DATE) AND CAST(discount_schedule_by_id.last_discount_period_end AS DATE)
    WHERE
    cart_redemption.cart_detail_id not in (SELECT cart_detail_id from ub_rawdata.cart_detail_discount_rate)
"""

QUERY_SENTRY_TO_UB_RAWDATA = """
    INSERT INTO ub_rawdata.cart_detail_discount_rate (
        "id", "type", "cart_detail_id", "discount_info", "discount_rate", "calculate_type", "discount_period_start", "discount_period_end"
    )
    SELECT 
        "id", "type", "cart_detail_id", "discount_info", "discount_rate", "calculate_type", "discount_period_start", "discount_period_end"
    FROM sentry.cart_detail_discount_rate
    WHERE 
    cart_detail_discount_rate.cart_detail_id not in (SELECT cart_detail_id from ub_rawdata.cart_detail_discount_rate)
"""

