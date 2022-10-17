QUERY_GET_GIFT_DETAIL_ID = """
    SELECT 
        DISTINCT
            discount_group_id, 
            gift_detail_id 
        FROM urcontract.mapping_discount_group
        WHERE
        discount_group_id IN (	
                        SELECT 
                        discount_group_id
                        FROM urcontract.thresshold_discount_group
                        WHERE 
                        is_step = 1
                        GROUP BY
                        discount_group_id
        );
"""