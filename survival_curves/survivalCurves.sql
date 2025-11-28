show fields from consolidated_mem_status;
show fields from consolidated_mem_type; -- primarly will show the number of entitlements they ever had
show fields from stack_job2;

select type_clean
from consolidated_mem_type
group by 1 
order by 1;

-- survivorship
-- count # of events per day: any kind of leave or suspension or deactivation
-- for everyday, count # of active subs, # of subs on leave
-- count # of censored data per day (at least on the days of "events"); this could possibly be carried out by a day-o-day join of member data to figure out the precise day they rolled off


/*
cancelled
care giving leave
deactivate
deactivated
disability leave
general leave
initial enrollment
medical leave
parental leave
suspended
technical activation
technical reactivation
winback
*/

-- little study: how many people return after taking leave?
select closer.mt_email, closer.activity, 
(select json_arrayagg(sj2.activity_calc) activity_array from stack_job2 sj2 where sj2.mt_email = closer.mt_email AND sj2.start_dt > closer.start_dt) outer_tactivity_array
from stack_job2 closer
where 1=1 
AND closer.activity_calc LIKE '%leave%';


-- count the total number of accounts that did and did not winback after general leave
WITH all_recs AS (
select closer.mt_email, closer.activity, 
(select json_arrayagg(sj2.activity_calc) activity_array from stack_job2 sj2 where sj2.mt_email = closer.mt_email AND sj2.start_dt > closer.start_dt) activity_array
from stack_job2 closer
where 1=1 
AND closer.activity_calc LIKE '%leave%') 
SELECT SUM(CASE WHEN JSON_CONTAINS(activity_array,'"winback"') = 1 THEN 1 ELSE 0 END) winback_cnt, 
SUM(CASE WHEN JSON_CONTAINS(activity_array,'"winback"') = 1 THEN 0 ELSE 1 END) no_winback
FROM all_recs ;

select mt_email, json_arrayagg(activity_calc) 
from stack_job2
group by 1
limit 20;
