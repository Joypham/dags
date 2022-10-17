query_get_card_diff_money_remain = """
    -- Lấy số dư trên thẻ
    with cte1 as (
    Select id as card_id, money as money_remain, card.version from urbox.card
    WHERE
    status = 2
    -- Loại chương trình loyalty
    -- and card.type in (1,4)
    ),
    cte2 as (
    -- Lấy số tiền topup, 
    select a.card_id,
                    sum(case when a.type = 1 and ((lower(a.note) not like '%hoàn tiền%' and lower(a.note) not like '%hoàn phí%')  or a.note is null ) then a.money else 0 end ) - sum(case when a.type = 404 then a.money else 0 end ) money_topup,
                    sum(case when a.type = 7 then -a.money else 0 end ) chuyen_tien_sang_the_khac,
                    sum(case when a.type = 8 then a.money else 0 end ) nhan_tien_tu_the_khac,
                    sum(case when a.type = 3 or (a.type = 1 and (lower(a.note)  like '%hoàn tiền%' or  lower(a.note) like '%hoàn phí%'))  then a.money else 0 end ) refund,
                    sum(case when a.type in (4, 41, 5, 9, 11, 12) then -a.money else 0 end ) giao_dich_tru_tien_khac,
                    sum(case when a.type in (6, 10) then a.money else 0 end ) giao_dich_cong_tien_khac
    from urbox.card_transaction a
    where 
        a.status = 2 and a.type <> 2
    group by a.card_id),
    
    cte3 as (
    -- Lấy số tiền tiêu theo cart_detail
    SELECT 
    cart_detail.card_id,
    sum(cart_detail.money) as money_comsuming
    from urbox.cart_detail
    join urbox.cart on cart.id = cart_detail.cart_id and cart.status = 2 and cart.pay_status = 2 and cart.delivery <> 4
    WHERE
    cart_detail.status = 2
    AND cart_detail.pay_status = 2
    AND cart_detail.card_id <> 0
    GROUP BY cart_detail.card_id),
    cte4 as (
    SELECT 
    cte1.*,
    cte2.money_topup +
    cte2.refund +
    cte2.chuyen_tien_sang_the_khac +
    cte2.nhan_tien_tu_the_khac +
    cte2.giao_dich_tru_tien_khac +
    cte2.giao_dich_cong_tien_khac +
    - cte3.money_comsuming as calculate_money_remain,
    cte1.money_remain - calculate_money_remain as diff
    from cte3
    join cte1 on cte3.card_id = cte1.card_id
    join cte2 on cte3.card_id = cte2.card_id
    WHERE
    cte1.money_remain <> calculate_money_remain
    ORDER BY 
    diff desc
    )
    SELECT * from cte4
"""

query_by_dup_card_trans = """
    with cte as (
    SELECT id from urbox.card
    WHERE
    card.status = 2 and card.type in (1,4)
    ),
    cte2 as (
    SELECT 
    card_transaction.card_id,
    cast("public".convert_timezone(card_transaction.created) as datetime) as card_transaction_created,
    card_transaction.money,
    COUNT ( DISTINCT cart_id) as so_lan_lap
    from urbox.card_transaction
    WHERE
    status = 2
    and type = 2
    and card_id in (SELECT id from cte)
    -- and cast("public".convert_timezone(card_transaction.created) as datetime) BETWEEN '2022-08-11' and '2022-08-12'
    and cast("public".convert_timezone(card_transaction.created) as datetime) > '2022-01-01'
    GROUP BY
    cast("public".convert_timezone(card_transaction.created) as datetime), card_transaction.money, card_transaction.card_id
    HAVING COUNT(DISTINCT cart_id) > 1
    )
    SELECT 
    cte2.card_id,
    cte2.card_transaction_created as transaction_time,
    cte2.money as money,
    cte2.so_lan_lap,
    card_transaction.cart_id,
    cart_detail.ID AS cart_detail_id,
    CAST ( "public".convert_timezone ( cart_detail.created ) AS datetime ) AS cart_detail_created,
    cart_detail.app_id,
    app.name as app_name,
    CAST ("public".convert_timezone(gift_code.used) as datetime) as gift_code_used_time
    FROM
        cte2
    join urbox.card_transaction on card_transaction.card_id = cte2.card_id and card_transaction.money = cte2.money 
    and card_transaction.status = 2 and cast("public".convert_timezone(card_transaction.created) as datetime) = cte2.card_transaction_created
    LEFT JOIN urbox.cart_detail on cart_detail.cart_id = card_transaction.cart_id and cart_detail.pay_status = 2 and cart_detail.status = 2
    LEFT JOIN urbox.app on app.id = cart_detail.app_id
    LEFT JOIN (
          select cart_detail_id, used, gift_code.code, gift_code.code_using ,row_number() over(partition by cart_detail_id order by active desc ) rn 
          from urbox.gift_code
          where status in (1, 2)
      ) gift_code on cart_detail.id = gift_code.cart_detail_id and gift_code.rn = 1
    ORDER BY 
    cte2.card_id, cte2.card_transaction_created desc
"""

query_by_dup_cart = """
    -- dup giao dich theo cart
with cte as (
SELECT id from urbox.card
WHERE
card.status = 2 and card.type in (1,4)
),
cte2 as (
SELECT 
cart.card_id,
cast("public".convert_timezone(cart.created) as datetime) as cart_created,
cart.money_total,
COUNT ( DISTINCT id) as so_lan_lap
from urbox.cart
WHERE
cart.status = 2
and cart.pay_status = 2
and cart.delivery <> 4
and card_id in (SELECT id from cte)
and cast("public".convert_timezone(cart.created) as datetime) > '2022-01-01'
GROUP BY
cast("public".convert_timezone(cart.created) as datetime), cart.money_total, cart.card_id
HAVING COUNT(DISTINCT id) > 1
)
SELECT 
cte2.card_id,
cte2.cart_created as transaction_time, 
cte2.money_total as money,
cte2.so_lan_lap,
cart.id as cart_id,
cart_detail.ID AS cart_detail_id,
CAST ( "public".convert_timezone ( cart_detail.created ) AS datetime ) AS cart_detail_created,
cart_detail.app_id,
app.name as app_name,
CAST ("public".convert_timezone(gift_code.used) as datetime) as gift_code_used_time

FROM
	cte2
join urbox.cart on cart.card_id = cte2.card_id and cart.money_total = cte2.money_total and cart.status = 2 and cart.pay_status = 2 and cart.delivery <> 4 
and cast("public".convert_timezone(cart.created) as datetime) = cte2.cart_created
LEFT JOIN urbox.cart_detail on cart_detail.cart_id = cart.id and cart_detail.pay_status = 2 and cart_detail.status = 2
LEFT JOIN urbox.app on app.id = cart_detail.app_id
LEFT JOIN (
      select cart_detail_id, used, gift_code.code, gift_code.code_using ,row_number() over(partition by cart_detail_id order by active desc ) rn 
      from urbox.gift_code
      where status in (1, 2)
  ) gift_code on cart_detail.id = gift_code.cart_detail_id and gift_code.rn = 1
ORDER BY 
cte2.card_id, cte2.cart_created desc
"""