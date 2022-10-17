from datetime import date, timedelta, datetime

# receiver=['']
TIKI_ALERT_EMAIL = ["hanh.ph@urbox.vn", "trang.ltt@urbox.vn", "nam.bui@urbox.vn", "anh.htn@urbox.vn",
                    "diep.nn@urbox.vn", "minh.nguyen@urbox.vn", "khoi.nc@urbox.vn"]

ALERT_EMAIL_CONTENT = """
    <p><strong><em>Cảnh b&aacute;o lượng đổi tiki vượt ngưỡng:</em></strong></p>
    <p style="padding-left: 40px;">- <em>Ng&agrave;y giờ</em>: <strong>{time_range}</strong></p>
    <p style="padding-left: 40px;">- <em>Type</em>: {type}</p>
    <p style="padding-left: 40px;">- <em>Mức độ cảnh b&aacute;o</em>:<span style="color: #ff0000;"><strong> {level}</strong></span></p>
    <p style="padding-left: 40px;">&nbsp; + lượng đổi tăng <span style="color: #ff0000;"><strong>{rate_luong_doi}%</strong></span></p>
    <p style="padding-left: 40px;">&nbsp; + lượt đổi tăng <span style="color: #ff0000;"><strong>{rate_luot_doi}%</strong></span> so với 1 tuần gần nhất</p>
    <p style="padding-left: 40px;">- <em>Chi tiết:</em></p>
    <p style="padding-left: 40px;">&nbsp; + Trung b&igrave;nh số lượt đổi trong 1 tuần gần nhất: {avg_so_luot_doi}</p>
    <p style="padding-left: 40px;">&nbsp; + Số lượt đổi giờ qua: <span style="color: #000000;">{so_luot_doi_at_current_hour}</span></p>
    <p style="padding-left: 40px;">&nbsp; + Trung b&igrave;nh lượng đổi trong 1 tuần gần nhất: {avg_luong_doi}</p>
    <p style="padding-left: 40px;">&nbsp; + Lượng đổi giờ qua: <span style="color: #000000;">{luong_doi_at_current_hour}</span></p>
"""


class ClassificationHour:
    '''
        Phân nhóm giờ theo link sau: https://docs.google.com/spreadsheets/d/1xjKfo0L1OLX8pIZuT7bHtuA9_qzAOQSFBs5DAxJxClU/edit#gid=1086511534
    '''
    class_1 = [23, 0]
    class_2 = [1, 2, 3, 4, 5]
    class_3 = [6]
    class_4 = [7, 8]
    class_5 = [9, 10, 11, 12]
    class_6 = [13, 14, 15, 16]
    class_7 = [17]
    class_8 = [18, 19]
    class_9 = [20, 21, 22]


class TimeFromNow:
    now = datetime.now()
    hour_now = now.hour
    day_now = now.day
    date_now = now.date()
    _7day_before = date_now - timedelta(days=7)
    one_hours_before = now - timedelta(hours=1)


class AlertLevel:
    level_negative = (-1000000000, 0)
    level_0 = (0, 50)
    level_1 = (51, 100)
    level_2 = (101, 500)
    level_3 = (501, 1000)
    level_4 = (1001, 2000)
    level_5 = (2001, 100000000)


AVG_REDEMPTION_RECORDS = """
    with cte1 as (
        SELECT 
            EXTRACT ( day FROM CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) ) AS day, 
            EXTRACT ( hour FROM PUBLIC.convert_timezone ( cart_detail.created ) ) AS hour,COUNT(DISTINCT cart_detail.id)
        FROM urbox.cart_detail
            JOIN urbox.gift_code on gift_code.cart_detail_id = cart_detail.id and gift_code.code_using <> ''
            JOIN urbox.gift on gift.id = cart_detail.gift_id
            JOIN urbox.brand on brand.id = gift.brand_id and brand.id = 36
        WHERE 
            cart_detail.pay_status = 2 AND 
            cart_detail.status = 2 AND 
            card_id > 0 AND CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) <= '{start_date}' AND 
            CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) >= '{end_7_date}' AND 
            EXTRACT ( hour FROM PUBLIC.convert_timezone ( cart_detail.created )) in {list_hour}
        GROUP BY 
            EXTRACT ( hour FROM PUBLIC.convert_timezone ( cart_detail.created ) ), EXTRACT ( day FROM CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) )
        ORDER BY 
            EXTRACT ( day FROM CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE )))
    SELECT AVG(count) from cte1
"""

AVG_REDEMPTION = """
    with cte1 as (
        SELECT 
            EXTRACT ( day FROM CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) ) AS day,
            EXTRACT ( hour FROM PUBLIC.convert_timezone ( cart_detail.created ) ) AS hour,
            SUM(cart_detail.money) as total_money
        FROM urbox.cart_detail
            JOIN urbox.gift_code on gift_code.cart_detail_id = cart_detail.id and gift_code.code_using <> ''
            JOIN urbox.gift on gift.id = cart_detail.gift_id
            JOIN urbox.brand on brand.id = gift.brand_id and brand.id = 36
        WHERE 
            cart_detail.pay_status = 2 AND 
            cart_detail.status = 2 AND 
            card_id > 0 AND
            CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) <= '{start_date}' AND
            CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) >= '{end_7_date}' AND 
            EXTRACT ( hour FROM PUBLIC.convert_timezone ( cart_detail.created )) in {list_hour}
        GROUP BY 
            EXTRACT ( hour FROM PUBLIC.convert_timezone ( cart_detail.created ) ), EXTRACT ( day FROM CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) )
        ORDER BY 
            EXTRACT ( day FROM CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE )))
    SELECT AVG(total_money) from cte1
"""

REDEMPTION_AT_CURRENT_HOUR = """
    SELECT 
        COUNT(cart_detail.id) as so_luot_doi, 
        nvl(sum (cart_detail.money),0) as luong_doi
    FROM urbox.cart_detail
        JOIN urbox.gift_code on gift_code.cart_detail_id = cart_detail.id and gift_code.code_using <> ''
        JOIN urbox.gift on gift.id = cart_detail.gift_id
        JOIN urbox.brand on brand.id = gift.brand_id and brand.id = 36
    WHERE 
        cart_detail.pay_status = 2 AND 
        cart_detail.status = 2 AND card_id > 0 AND 
        CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) = '{date_now}' AND 
        EXTRACT ( hour FROM PUBLIC.convert_timezone ( cart_detail.created )) in ('{one_hours_from_now}')
"""

TOP_APP_ALERT = """
    WITH cte AS
    (
    SELECT 
            cart_detail.app_id,
            COUNT(cart_detail.id) as so_luot_doi, 
            nvl(SUM (cart_detail.money),0) as luong_doi
        FROM urbox.cart_detail
            JOIN urbox.gift_code on gift_code.cart_detail_id = cart_detail.id and gift_code.code_using <> ''
            JOIN urbox.gift on gift.id = cart_detail.gift_id
            JOIN urbox.brand on brand.id = gift.brand_id and brand.id = 36
        WHERE 
            cart_detail.pay_status = 2 AND 
            cart_detail.status = 2 AND card_id > 0 AND 
            CAST ( PUBLIC.convert_timezone ( cart_detail.created ) AS DATE ) = '{date_now}' AND 
            EXTRACT ( hour FROM PUBLIC.convert_timezone ( cart_detail.created )) in ('{one_hours_from_now}')
            GROUP BY
                    cart_detail.app_id
    )
    SELECT 
        app."name",
         cte.* 
    FROM cte 
        JOIN urbox.app on app.id = cte.app_id
    ORDER BY luong_doi DESC
"""