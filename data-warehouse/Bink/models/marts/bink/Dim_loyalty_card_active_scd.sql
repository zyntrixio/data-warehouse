with loyalty_add as (
  select * 

  from {{ref('fact_loyalty_card_add')}} 

)


, loyalty_removed as (
select * 
  from {{ref('fact_loyalty_card_removed')}} 

)


, events as (
select 
a.EVENT_ID add_id ,
a.EVENT_DATE_TIME add_time,
a.LOYALTY_CARD_ID,
a.user_id,
a.CHANNEL,
r.EVENT_ID remove_id ,
r.EVENT_DATE_TIME remove_time,
row_number()over(Partition by  a.channel, a.user_id, a.LOYALTY_CARD_ID  order by datediff('second',a.EVENT_DATE_TIME ,r.EVENT_DATE_TIME )) closest,
datediff('second',a.EVENT_DATE_TIME ,r.EVENT_DATE_TIME )
from loyalty_add  a
left join loyalty_removed r
on a.LOYALTY_CARD_ID = r.LOYALTY_CARD_ID
and a.user_id = r.user_id
and a.CHANNEL = r.CHANNEL
and a.EVENT_DATE_TIME <= r.EVENT_DATE_TIME
where 1 = 1
and a.EVENT_TYPE = 'SUCCESS'
  and date(a.EVENT_DATE_TIME) >= '2022-06-09'
  
  )
  
  , add_time as (
  select min(add_id) add_id,
  min(add_time) add_time,
  LOYALTY_CARD_ID,
  user_id,
  CHANNEL,
  remove_id,
  remove_time,
  case when remove_id is not null then 'TRUE'
        else 'FALSE'
  end as removed
  from events
  where (remove_id is  null or (remove_id is not null and closest = 1))
  --and closest <> 1
  group by 3,4,5,6,7,8


)


select LOYALTY_CARD_ID,
user_id,
  CHANNEL,
  removed,
  add_time as valid_from,
  coalesce(remove_time, current_timestamp()::timestamp_ntz) as valid_to
  from add_time


