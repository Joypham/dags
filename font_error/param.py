QUERY_GET_DATA = """
    SELECT 
        id,
        wallet_id,
        content,
        star,
        version,
        created_at,
        updated_at,
        status
    FROM 
        urbox.mobile_rate
    WHERE
        id not in (SELECT DISTINCT id from ub_rawdata.mobile_rate_tv)

"""

