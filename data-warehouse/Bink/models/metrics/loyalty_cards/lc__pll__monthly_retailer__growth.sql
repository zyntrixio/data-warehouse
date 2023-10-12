/*
Created by:         Anand Bhakta
Created date:       2023-09-25
Last modified by:
Last modified date:

Description:
    Convert the lc__pll__monthly_retailer table to growth metrics.
Parameters:
    source_object       - lc__pll__monthly_retailer
*/

{{convert_to_growth("lc__pll__monthly_retailer",
                    ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    [],
                    ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    "DATE" )
}}
