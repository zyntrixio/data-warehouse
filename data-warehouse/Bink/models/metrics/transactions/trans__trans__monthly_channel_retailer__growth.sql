/*
Created by:         Anand Bhakta
Created date:       2023-09-25
Last modified by:   Anand Bhakta
Last modified date: 2023-12-18

Description:
    Convert the trans__trans__monthly_retailer table to growth metrics.
Parameters:
    source_object       - trans__trans__monthly_retailer
*/

{{convert_to_growth("trans__trans__monthly_channel_brand_retailer",
                    ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY","CHANNEL"],
                    ["BRAND"],
                    ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY","CHANNEL"],
                    "DATE",
                    "_BRAND" )
}}
