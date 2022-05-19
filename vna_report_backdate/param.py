ORDER_LIST_QUERY = """
    SELECT
        oddt.id AS order_detail_id,
        od.id AS order_id,
        gd.core_gift_detail_id,
        core_gd.title AS core_gift_detail_title,
        oddt.quantity,
        oddt.price AS unit_price,
        oddt.price * oddt.quantity AS amount,
        od.amount AS total_amount,
        od.email,
        od.phone_number,
        CASE
            WHEN od.payment_method = 1 THEN 'vnpay' 
            WHEN od.payment_method = 2 THEN 'adyen' 
            WHEN od.payment_method = 3 THEN 'napas' 
            ELSE'unidentified' 
        END AS payment_method,
        od.payment_transaction_id,
        od.vat_tax_code,
        od.vat_company,
        PUBLIC.convert_timezone(od.created) AS order_created_time
    FROM urevoucher.order_detail oddt
        LEFT JOIN urevoucher.ORDER od ON od.id = oddt.order_id
        LEFT JOIN urevoucher.gift_detail gd ON gd.id = oddt.gift_detail_id
        LEFT JOIN urbox.gift_detail core_gd ON core_gd.id = gd.core_gift_detail_id 
        LEFT JOIN urevoucher.cart_transaction cart_trans ON od.id = cart_trans.order_id
        LEFT JOIN urbox.cart ON cart.id = cart_trans.cart_id
    WHERE
        od.status = 2
        AND od.is_paid = 2
        AND CAST(PUBLIC.convert_timezone(oddt.created) AS date) BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY
        od.id,
        oddt.id
"""

CODE_USAGE_QUERY = """
    SELECT
        gc.code_using,
        oddt.id AS order_detail_id,
        od.id AS order_id,
        core_gd.id AS core_gift_detail_id,
        core_gd.title AS core_gift_detail_title,
        core_gd.price AS unit_price,
        od.amount AS total_amount,
        od.email,
        od.phone_number,
        CASE
            WHEN od.payment_method = 1 THEN 'vnpay' 
            WHEN od.payment_method = 2 THEN 'adyen' 
            WHEN od.payment_method = 3 THEN 'napas' 
            ELSE 'unidentified' 
        END AS payment_method,
        od.payment_transaction_id,
        od.vat_tax_code,
        od.vat_company,
        PUBLIC.convert_timezone(od.created) AS order_created_time,
        PUBLIC.convert_timezone(gc.used) AS code_used_time 
    FROM urbox.gift_code gc
        LEFT JOIN urbox.cart_detail cd ON cd.id = gc.cart_detail_id
        LEFT JOIN urbox.cart ON cd.cart_id = cart.id 
        LEFT JOIN urevoucher.cart_transaction cart_trans ON cart_trans.cart_id = cart.id 
        LEFT JOIN urevoucher.order od ON cart_trans.order_id = od.id
        LEFT JOIN urevoucher.order_detail oddt ON oddt.order_id = od.id AND oddt.core_gift_detail_id = cd.gift_detail_id
        LEFT JOIN urbox.gift_detail core_gd ON core_gd.id = gc.gift_detail_id 
    WHERE
        gc.status = 1 
        AND od.status = 2 -- binh thuong
        AND od.is_got_code = 2 -- khach hang da nhan code
        AND od.is_sent_code = 2 -- he thong da gui code
        AND od.is_paid = 2 -- khach hang da tra tien
        AND cart.status = 2 -- dơn hang binh thuong
        AND gc.used IS NOT NULL 
        AND gc.used > 0 
        AND CAST(PUBLIC.convert_timezone(gc.used) AS date) BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY
        gc.used DESC
    ;
"""

CANCELLED_ORDER_QUERY = """
    SELECT
        oddt.id AS order_detail_id,
        od.ID AS order_id,
        gd.core_gift_detail_id,
        core_gd.title AS core_gift_detail_title,
        oddt.quantity,
        oddt.price AS unit_price,
        oddt.price * oddt.quantity AS amount,
        od.amount AS total_amount,
        od.email,
        od.phone_number,
        CASE
            WHEN od.payment_method = 1 THEN 'vnpay'
            WHEN od.payment_method = 2 THEN 'adyen'
            WHEN od.payment_method = 3 THEN 'napas'
            ELSE'unidentified'
        END AS payment_method,
        od.payment_transaction_id,
        od.vat_tax_code,
        od.vat_company,
        PUBLIC.convert_timezone(od.created) AS order_created_time
    FROM urevoucher.order_detail oddt
        LEFT JOIN urevoucher.ORDER od ON od.id = oddt.order_id
        LEFT JOIN urevoucher.gift_detail gd ON gd.id = oddt.gift_detail_id
        LEFT JOIN urbox.gift_detail core_gd ON core_gd.id = gd.core_gift_detail_id
        LEFT JOIN urevoucher.cart_transaction cart_trans ON od.id = cart_trans.order_id
        LEFT JOIN urbox.cart ON cart.id = cart_trans.cart_id
    WHERE
        od.status = 2 -- binh thuong
        AND cart.status = -2 -- dơn hang binh thuong
    ORDER BY
        od.id,
        oddt.id
"""
