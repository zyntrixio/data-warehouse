/*
Created by:         Anand Bhakta
Created date:       2023-11-09
Last modified by:   Anand Bhakta
Last modified date: 2023-12-18

Description:
    Convert the lc__pll__monthly_retailer table to growth metrics.
Parameters:
    source_object       - lc__pll__monthly_retailer
*/

{{convert_to_growth("lc__pll__monthly_channel_brand_retailer",
                    ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY","CHANNEL"],
                    ["BRAND"],
                    ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY","CHANNEL"],
                    "DATE" )
}}
