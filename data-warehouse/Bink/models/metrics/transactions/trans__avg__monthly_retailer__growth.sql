/*
Created by:         Anand Bhakta
Created date:       2023-09-25
Last modified by:
Last modified date:

Description:
    Convert the trans__avg__monthly_retailer table to growth metrics.
Parameters:
    source_object       - trans__avg__monthly_retailer
*/

{{convert_to_growth("trans__avg__monthly_retailer",
                    ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    [],
                    ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    "DATE" )
}}
