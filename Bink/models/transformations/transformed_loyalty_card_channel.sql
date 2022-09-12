
with source as (
    select * from {{ref('fact_loyalty_card_add')}}
)

, get_source_channel as (
    

select 
distinct 
channel,
loyalty_card_id,
LOYALTY_PLAN,
main_answer, 
EVENT_DATE_TIME ,
auth_type,
user_id,
coalesce(main_answer, loyalty_card_id) as main_loyal_id
--row_number()over(partition by loyalty_card_id order by EVENT_DATE_TIME desc ) as add_rank
from source
where event_type  = 'SUCCESS'
--qualify rank = 1

)

select * from get_source_channel