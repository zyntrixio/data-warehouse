/*
Created by:         Anand Bhakta
Created date:       2023-10-31
Last modified by:
Last modified date:

Description:
    todo
Parameters:
    source_object       - voucher__stamps__monthly_retailer
*/

{{convert_to_growth("voucher__stamps__monthly_retailer",
                    ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    [],
                    ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    "DATE" )
}}
