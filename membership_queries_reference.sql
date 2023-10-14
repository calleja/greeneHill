-- word wrap shortcut: shift+cntrl+alt+w
show tables;

show fields from mem_type;
show fields from mem_status;
select * from mem_type order by email, start_dt ASC limit 10;
select * from mem_status order by email, start_dt ASC limit 60;
select type from mem_status group by 1;
/* latest_trial2 = json of latest trial recorded for that email addres (from date of report pull)
 * 
 * 
 * */
 */
show fields from mem_status;

select type from mem_type2 group by 1 order by 1;


drop table mem_type;
drop table mem_status;

select * from mem_type 
where cast(start_dt as date) > date('2023-02-01')
and type_clean not like '%trial%';

select count(*) from mem_type;

-- unique values of type_clean
select type_clean 
from mem_type
group by 1 
order by 1;

-- how do we treat bushwick and park slope members?
WITH others AS (
select * 
from mem_type
where type_clean in ('bushwick','park slope'))
select distinct mem_type.* 
from mem_type
inner join others USING (email)
order by email, start_dt asc;


-- historical trial starts
select date_format(start_dt, '%Y-%m') as month, count(distinct email) as count
from mem_type2
where type_clean like '%trial%'
group by 1
order by 1 desc;

-- historical trial conversions: must account for multiple trials
-- query for any M-O status after a trial 


-- query a json field; the start_dt field in the json doc is isoformat, which must be converted
SELECT JSON_EXTRACT(latest_trial2, '$.start_dt') as json, cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) date_form, 
str_to_date(JSON_EXTRACT(latest_trial2, '$.start_dt'),'%Y-%m-%dT%TZ') 
from mem_type 
limit 5;

select email, start_dt, cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) trial_date, 
CASE 
-- WHEN cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) = 'NULL' THEN 'it is NULL'
WHEN cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) is null THEN 'it is actually null'
ELSE 'undetermined' END AS eval
from mem_type
where email = 'alahoish@gmail.com';

-- all historical conversions: as of the trial start date, and include date of conversion
-- aggregates unique email counts to the trial start date, and allows for duplicate trials when a prospective initiates multiple trials, trial conversions happening BEFORE the latest trial initiation are included; this query excludes park slope and Bushwick members until they're better understood

-- first investigate the validity of the logic: any non-trial "type"
select cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) trial_date, cast(start_dt as date) status_date, 
datediff(cast(start_dt as date), cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date)) as date_difference,
email, type_clean
from mem_type
where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
AND cast(JSON_EXTRACT(latest_trial2, '$.start_dt')as date) is not null;

-- investigate cases of members that converted to fulltime members but seemingly opened up a trial later: there aren't too many: 16 entries involving 5 individuals
WITH all_converts AS (
select cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) trial_date, cast(start_dt as date) status_date, 
datediff(cast(start_dt as date), cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date)) as date_difference,
email, type_clean
from mem_type
where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
AND cast(JSON_EXTRACT(latest_trial2, '$.start_dt')as date) is not null)
select mem_type.* 
from mem_type
inner join all_converts ON mem_type.email = all_converts.email
where all_converts.date_difference < 0
order by mem_type.email, mem_type.start_dt asc 
limit 300;




-- current logic only look at whether a trial was ever initiated and if a non-trial membership was initiated at some point; no condition is introduced to ensure that the full time membership was activated AFTER the trial start date
WITH trial_conversions AS (
select cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) trial_date, cast(start_dt as date) status_date, 
datediff(cast(start_dt as date), cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date)) as date_difference,
email, type_clean
from mem_type
where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
AND cast(JSON_EXTRACT(latest_trial2, '$.start_dt')as date) is not null),
-- choose to include all trials, even in cases of multiple initiated by same email
all_trials AS (
select cast(start_dt as date) status_date, email, type_clean
from mem_type
where type_clean like '%trial%'),
-- aggregate the two trial tables by month, then join on month
conversion_agg AS (
select date_format(trial_date, '%Y-%m') as month, count(distinct email) as converted_cnt
from trial_conversions 
group by 1),
all_agg AS (
select date_format(status_date, '%Y-%m') as month, count(distinct email) as trials_cnt
from all_trials
group by 1)
select all_agg.month, sum(converted_cnt) converted_cnt, sum(trials_cnt) trials_cnt
from conversion_agg
RIGHT JOIN all_agg
ON conversion_agg.month = all_agg.month
group by 1
order by 1 desc;

-- QA thoe above: were there indeed 47 trial sign-ups on March 2023?
select * 
from mem_type
where type_clean like '%trial%'
AND extract(month from start_dt) = 3
AND extract(year from start_dt) = 2023;

-- count all new signups sans-trial

-- join shopping data to mem_type data; later: freq of shopping for trial members, while in-trial, and relate to type conversions

