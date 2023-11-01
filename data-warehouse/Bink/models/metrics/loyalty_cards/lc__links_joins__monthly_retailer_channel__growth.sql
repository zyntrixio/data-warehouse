/*
Created by:         Anand Bhakta
Created date:       2023-09-20
Last modified by:
Last modified date:

Description:
        Convert the lc__links_joins__monthly_retailer_channel table to growth metrics.
Parameters:
    source_object       - lc__links_joins__monthly_retailer_channel
*/

{{convert_to_growth("lc__links_joins__monthly_retailer_channel",
                    ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    [],
                    ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    "DATE" )
}}
