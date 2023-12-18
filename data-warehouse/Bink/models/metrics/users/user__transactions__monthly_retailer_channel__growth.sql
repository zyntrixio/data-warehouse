/*
Created by:         Anand Bhakta
Created date:       2023-09-25
Last modified by:
Last modified date:

Description:
    Convert user__transactions__monthly_retailer to growth
Parameters:
    source_object       - user__transactions__monthly_retailer
*/

{{convert_to_growth("user__transactions__monthly_retailer_channel",
                    ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY","CHANNEL"],
                    ["BRAND"],
                    ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY","CHANNEL"],
                    "DATE" )
}}
