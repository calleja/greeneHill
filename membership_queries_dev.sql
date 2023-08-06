-- word wrap shortcut: shift+cntrl+alt+w
show tables;

show fields from mem_type;
select * from mem_type limit 5;
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
-- for non-trial entries, calculate the date differential between the latest trial (handy if there are > 1)
-- trial_conversions will contain all trial members that initiated another membership type (non-trial)
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

-- QA the above: were there indeed 47 trial sign-ups on March 2023?
select * 
from mem_type
where type_clean like '%trial%'
AND extract(month from start_dt) = 3
AND extract(year from start_dt) = 2023;

-- count all new signups sans-trial

-- join mem_status to mem_type
select *
from mem_type
LEFT JOIN mem_status ON email

-- join shopping data to mem_type data; later: freq of shopping for trial members, while in-trial, and relate to type conversions

-- attempting to create the table first, then inserting values into it to circumvent the json error received when just trying to create and query at same time
/*INSERT INTO trialShoppingHabits (trial_start_dt, conversions_trial_dt, email, trial_expiration,
ingest_date, max_type_clean, relative_trial_period, final_trial_flag, trips, total_trips, conversion_flag)*/
/*
CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) = 'null' THEN NULL 
ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) as date) END AS latest_trial_start_dt,
*/
DROP TABLE IF EXISTS trialShoppingHabits;
CREATE TABLE trialShoppingHabits AS
WITH prep AS (
SELECT CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) = 'null' THEN NULL 
ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) as date) END AS trial_dt, 
email, type_clean, start_dt, ingest_date, trial_expiration
from mem_type_0730),
conversions AS (
SELECT  trial_dt, datediff(cast(start_dt as date), prep.trial_dt) as date_difference,
email, type_clean
from prep
where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
AND trial_dt is not null), 
stats AS (
SELECT cast(mt.start_dt as date) AS trial_start_dt, conversions.trial_dt as conversions_trial_dt, mt.email, cast(trial_expiration as date) trial_expiration, mt.ingest_date, 
-- correlated subquery to extract max type_clean (excluding trials)
(select max(mini.type_clean) from mem_type_0730 mini where mini.email = mt.email AND mini.type_clean not like '%trial%') max_type_clean,
CASE 
WHEN sl.Activity_Date BETWEEN mt.start_dt AND mt.trial_expiration THEN 'in trial' 
WHEN sl.Activity_Date < mt.start_dt THEN 'pre trial' 
ELSE 'other' 
END AS relative_trial_period, 
-- decipher whether the trial in question is the only one during this period; for purposes of accurate trial conversion
CASE 
WHEN mt.trial_dt = cast(mt.start_dt as date) THEN 'relevant trial' 
ELSE 'trial iteration expected' 
END AS final_trial_flag,
-- attempt to propogate each user's record (in resultset) with the last member type to denote conversion
-- max(conversions.type_clean) converted_type <- reinserting above
count(distinct sl.Activity_Date) trips
from prep mt
LEFT JOIN shop_log sl ON trim(mt.email) = trim(sl.Target_Email)
LEFT JOIN conversions ON trim(mt.email) = trim(conversions.email) AND cast(mt.start_dt as date) = conversions.trial_dt
WHERE 1=1
AND mt.start_dt >= date('2023-02-01')
AND trial_expiration is not null
AND mt.type_clean like '%trial%'
GROUP BY 1,2,3,4,5,6,7,8)
select stats.*, SUM(trips) OVER(PARTITION BY email, trial_start_dt) as total_trips,
CASE 
WHEN final_trial_flag = 'trial iteration expected' THEN 'defer to subsequent trial'
WHEN trial_expiration >= ingest_date THEN 'in trial' 
-- trial had to have expired BEFORE ingest_date
WHEN max_type_clean is NULL AND trial_expiration <= ingest_date  THEN 'did not convert' 
ELSE 'converted' 
END AS conversion_flag 
from stats 
order by email, trial_start_dt asc;

-- time series: plot avg shopping trips while in trial by trial start month [trial_start_dt, trips, conversion flag, relative_trial_period, email]
-- the below provides: week of trial start date, avg trips while in-trial, total trial members, and num of trial members over avg
select date_add(trial_start_dt, interval  -WEEKDAY(trial_start_dt)-1 day) FirstDayOfWeek, avg(trips) avg_trips_while_trial, 
count(distinct email) trial_members, sum(case when tsh.trips > (SELECT avg(tsh_m.trips) from trialShoppingHabits tsh_m WHERE date_add(tsh_m.trial_start_dt, interval  -WEEKDAY(tsh_m.trial_start_dt)-1 day) = date_add(tsh.trial_start_dt, interval  -WEEKDAY(tsh.trial_start_dt)-1 day) AND tsh_m.relative_trial_period = 'in trial') THEN 1 ELSE 0 end) num_over_avg
-- (select max(mini.type_clean) from mem_type mini where mini.email = mt.email AND mini.type_clean not like '%trial%') 
from trialShoppingHabits tsh
where relative_trial_period = 'in trial'
group by 1
order by 1 asc;

-- surface the actual trial members likely to convert: trial members that have shopped since trial has begun and still in-trial
-- partition/craft cohorts from trial start week
select 
date_add(trial_start_dt, interval  -WEEKDAY(trial_start_dt)-1 day) FirstDayOfWeek, (SELECT avg(tsh_m.trips) from trialShoppingHabits tsh_m WHERE date_add(tsh_m.trial_start_dt, interval  -WEEKDAY(tsh_m.trial_start_dt)-1 day) = date_add(tsh.trial_start_dt, interval  -WEEKDAY(tsh.trial_start_dt)-1 day) AND tsh_m.relative_trial_period = 'in trial') avg_for_cohort,
email, 
tsh.trips, -- number of trips taken while in-trial
trial_expiration,
final_trial_flag,
relative_trial_period
from trialShoppingHabits tsh
WHERE tsh.trips > (SELECT avg(tsh_m.trips) from trialShoppingHabits tsh_m WHERE date_add(tsh_m.trial_start_dt, interval  -WEEKDAY(tsh_m.trial_start_dt)-1 day) = date_add(tsh.trial_start_dt, interval  -WEEKDAY(tsh.trial_start_dt)-1 day) AND tsh_m.relative_trial_period = 'in trial')
AND trial_expiration > date('2023-07-15')
AND relative_trial_period = 'in trial'
order by 1 desc, 5 desc;

-- (select max(mini.type_clean) from mem_type mini where mini.email = mt.email AND mini.type_clean not like '%trial%') 

where relative_trial_period = 'in trial'
group by 1
order by 1 asc;

-- potentially missed opportunities? former trial members that did not convert but either shopped a lot during their trial or have shopped since their trial expired (but they never converted)
