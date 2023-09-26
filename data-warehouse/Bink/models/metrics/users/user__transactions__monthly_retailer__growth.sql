/*
Created by:         Anand Bhakta
Created date:       2023-09-25
Last modified by:
Last modified date:

Description:
    todo
Parameters:
    source_object       - user__transactions__monthly_retailer
*/

{{convert_to_growth("user__transactions__monthly_retailer",
                    ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    [],
                    ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    "DATE" )
}}
