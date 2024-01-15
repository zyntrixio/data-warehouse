/*
Created by:         Christopher Mithcell
Created date:       2023-10-03
Last modified by:   Christopher Mitchell
Last modified date: 2024-01-15

Description:
    todo
Parameters:
    source_object       - voucher__counts__monthly_retailer
*/

{{convert_to_growth("voucher__counts__monthly_retailer",
                    ["DATE", "LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    [],
                    ["LOYALTY_PLAN_NAME", "LOYALTY_PLAN_COMPANY"],
                    "DATE" )
}}
