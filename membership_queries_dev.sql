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

-- one-off queries to investigate particular persons
select * 
from mem_type_0927
where email IN ('mateotaussig@gmail.com','gabrielagamache@gmail.com') 
order by email;
-- one-off queries to investigate particular persons
select * 
from mem_status_0927
where email IN ('mateotaussig@gmail.com','gabrielagamache@gmail.com','br.weinkle@gmail.com') 
order by email , start_dt asc;

-- PERFORM QA on two mem_type tables
SELECT 
FROM mem_type_0730
LEFT JOIN mem_type_0910;

select count(*)
from mem_type_0730 
where cast(start_dt as date) BETWEEN date('2022-01-01') AND date('2022-12-31');

-- QA cont: count records by membership start date and membership type: search for discrepancies in the numbers
WITH first_tbl AS (
select date_add(start_dt, interval  -WEEKDAY(start_dt)-1 day) week, type, count(*) num
from mem_type_0827
where cast(start_dt as date) BETWEEN date('2023-05-01') AND date('2023-07-01')
group by 1,2), 
sec_tbl AS (
select date_add(start_dt, interval  -WEEKDAY(start_dt)-1 day) week, type, count(*) num
from mem_type_0910
where cast(start_dt as date) BETWEEN date('2023-05-01') AND date('2023-07-01')
group by 1,2)
select ft.week, ft.type, ft.num as cnt_first, st.num as cnt_sec 
from first_tbl ft
INNER JOIN sec_tbl st USING (week,type)
order by 1 asc, 2
limit 20;

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
from mem_type_0925
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

/******************
PLOTTING ACTIVE MEMBERS
Multi-part query, as they each represented a different approach:
1) stack all type and status activity, including the range of duration; then update the lead_date on the "type" row so that it accounts for the next "status" start_dt (as these are calculated independently on the python program)
2) buildin mt_ms, but doesn't stack resultset, so may not be useful
3) stacks like #1 but conditionally selects for 'prevailing' status and types <- could be incoporated into #1, but haven't checked

*******************/

-- #1
-- join mem_status to mem_type; then stack so that each row is a membership event period for ea email
-- UPDATING STATEMENT to replace the "lead_date" for "TYPE" rows
-- the lead and start_dt fields are exclusive to either type or status changes, this is bc I related all status changes to the prevailing type and I need the type range in order to accomplish that; this means I HAVE TO recompute the lead/start date post compilation

-- resorting to creating a table in order to ensure successful compiling
DROP TABLE IF EXISTS stack_job;
CREATE TABLE stack_job AS
WITH mt_ms AS (
SELECT mt.email mt_email, mt.start_dt mt_start_dt, mt.lead_date mt_lead_date, mt.type_clean mt_type_clean, mt.type_raw mt_type_raw, mt.trial_expiration mt_trial_expiration, ms.start_dt ms_start_dt, ms.lead_date ms_lead_date, ms.type_clean ms_type_clean, ms.type_raw ms_type_raw  
from mem_type_0927 mt
LEFT JOIN mem_status_0927 ms ON mt.email = ms.email
-- ensure that status records only populate on same line as the relevant type record
AND ms.start_dt between mt.start_dt AND mt.lead_date
order by mt.email, mt.start_dt asc, ms.start_dt asc),
-- stack the data; mt_type_clean = null WHEN 
-- excludes trial activity and only returns activity related to full member-owners
stacked AS (
SELECT mt_email, mt_start_dt start_dt, mt_lead_date lead_date, mt_type_clean type_clean, null mt_type_clean, mt_type_raw type_raw 
FROM mt_ms
WHERE mt_type_clean IN ('lettuce', 'carrot', 'household', 'avocado')
UNION ALL
SELECT mt_email, ms_start_dt start_dt, ms_lead_date lead_date, ms_type_clean type_clean, mt_type_clean, ms_type_raw type_raw
FROM mt_ms
-- ms_type_clean values related to trial activity are '%trial%', 'cancelled', 'deactivated'
WHERE ms_type_clean not like '%trial%')
select * 
from stacked 
WHERE mt_email IN ('fenailletom@gmail.com','405sarah@gmail.com')
group by 1,2,3,4,5,6 -- must group by in order to account for duplicated "type" rows (LHS)
order by 1,2;

-- prelim table is a helper table to build the new lead date
-- might need to compute a LEAD() first then run a WHERE clause in a second query to exclude accounts that didn't log a status change
WITH prelim AS (
SELECT stack_job.*, LEAD(start_dt) OVER(PARTITION BY mt_email ORDER BY start_dt) date_lead2
FROM stack_job
WHERE mt_email IN ('fenailletom@gmail.com','405sarah@gmail.com')
order by 1,2) 
UPDATE stack_job AS sj2 
-- first apply the inner join, then set values of one column (date_lead) to the other col (date_lead2) ON THE SAME ROW; the alternative would be to write a CASE statement, but then I'd end up with a row with which to deal
-- lead the start_dt of the proceeding status, if one exists and attempt to replace the lead_date field for the type row
INNER JOIN (SELECT * FROM prelim where mt_type_clean IS NULL) x 
ON sj2.mt_email = x.mt_email AND sj2.type_clean = x.type_clean -- ensure that we only update the first 'type' row
SET sj2.lead_date = x.date_lead2 
WHERE sj2.mt_type_clean is NULL AND x.date_lead2 IS NOT NULL;

UPDATE stack_job AS sj1 
INNER JOIN 
-- lead the start_dt of the proceeding status, if one exists and attempt to replace the lead_date field for the type row
(SELECT * FROM prelim where mt_type_clean IS NULL) x ON sj1.mt_email = x.mt_email AND sj1.type_clean = s.type_clean -- ensure that we only update the first 'type' row
SET sj1.lead_date = x.date_lead2;

WITH prelim AS (
SELECT stack_job.*, LEAD(start_dt) OVER(PARTITION BY mt_email ORDER BY start_dt) date_lead2
FROM stack_job
WHERE mt_email IN ('fenailletom@gmail.com','405sarah@gmail.com')
order by 1,2) 
UPDATE stack_job AS sj2 
-- first apply the inner join, then set values of one column (date_lead) to the other col (date_lead2) ON THE SAME ROW; the alternative would be to write a CASE statement, but then I'd end up with a row with which to deal
-- lead the start_dt of the proceeding status, if one exists and attempt to replace the lead_date field for the type row
LEFT JOIN (SELECT * FROM prelim where mt_type_clean IS NULL) x 
ON sj2.mt_email = x.mt_email AND sj2.type_clean = x.type_clean -- ensure that we only update the first 'type' row
SET sj2.lead_date = x.date_lead2 
WHERE sj2.mt_type_clean is NULL AND x.date_lead2 IS NOT NULL;

-- #2
-- measure active members
-- in order to graph the # of members in a timeseries, will need to join the above to the calendar table
-- 'cancelled', 'care giving leave', 'general leave', 'medical leave', 'parental leave' <- marks the suspension of membership: if this occurs, the mt_type_clean is not downgraded, but preserved, so, I have to refer to the ms_start_dt of the suspension
-- two pieces of data will come from 'cancel' lines: 1) the start date of the membership tier and the period of the cancellation/suspension: ostensibly refer to the range on the mt "side" if ms_type_clean is null, if not null
-- mt_ms joins ms data to mt, providing for status changes (ex. winbacks and leave) to be associated to member type; ms_start_date and ms_lead_date contain the dates in which the status is in effect, all of which should be within the range of the over arching membershipt type
-- stacking mt and ms data allows for building a case statement that will allow me to select the prevailing status, as long as every line contains mt_start_dt and mt_lead_dt
WITH mt AS (
SELECT mt.email mt_email, mt.start_dt mt_start_dt, mt.lead_date mt_lead_date, mt.type_clean mt_type_clean, mt.type_raw mt_type_raw, mt.trial_expiration mt_trial_expiration
from mem_type_0927 mt	
WHERE mt.type_clean IN ('lettuce', 'carrot', 'household', 'avocado') 
group by 1,2,3,4,5,6),
-- ms code needs to be tested/QAd
ms AS (
SELECT ms.email ms_email, ms.start_dt ms_start_dt, ms.lead_date ms_lead_date, ms.type_clean ms_type_clean, ms.type_raw ms_type_raw  	
FROM mem_status_0927 ms
WHERE ms.type_clean not like '%trial%'
AND ms.type_clean NOT IN ('cancelled', 'deactivated') 
group by 1,2,3,4,5),
mt_ms AS (
SELECT mt_email, mt_start_dt, mt_lead_date, mt_type_clean, mt_type_raw, mt_trial_expiration, ms_start_dt, ms_lead_date, ms_type_clean, ms_type_raw  
from mt
LEFT JOIN ms ON mt_email = ms_email
-- ensure that status records only populate on same line as the relevant type record
AND ms_start_dt between mt_start_dt AND mt_lead_date) 
select * 
from mt_ms 
order by mt_email, mt_start_dt asc, ms_start_dt asc
limit 200;

-- #3
-- EXPERMINTAL CODE: stacking mt and ms properly; when done properly, I can build a CASE statement that allows me to choose the prevailing status/type (active, inactive) and simultaneously join to the calendar table
-- once a member makes it mt_ms, by applied conditions, they are associated with a membership type and will only duplicate if there is status activity
-- this is missing the initial period of signup, which will need to be UNION'd... after which, the set of "prevailing_" fields will serve as the date ranges to join to the calendar table
select mt_email, mt_type_clean, mt_type_raw,
CASE WHEN ms_start_dt < mt_lead_date THEN ms_type_clean ELSE mt_type_clean END AS prevailing_status,
CASE WHEN ms_start_dt < mt_lead_date THEN ms_start_dt ELSE mt_start_dt END AS prevailing_status_start,
CASE WHEN ms_start_dt < mt_lead_date THEN ms_lead_date ELSE mt_lead_date END AS prevailing_status_end 
from mt_ms 
-- order by mt_email, mt_start_dt asc, ms_start_dt asc
UNION 
select mt_email, mt_type_clean, mt_type_raw,
mt_type_clean prevailing_status,
mt_start_dt prevailing_status_start,
mt_lead_date prevailing_status_end 
from mt_ms 
group by 1,2,3,4,5,6
order by 1, 5 asc 
limit 200;

-- END ACTIVE MEMBER TIME SERIES STUDY

-- weekly snapshot; calendar table
-- this requires adjusting the system variable: https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_cte_max_recursion_depth; https://www.percona.com/blog/introduction-to-mysql-8-0-recursive-common-table-expression-part-2/

SET cte_max_recursion_depth=2000; -- can be executed within dBeaver
CREATE TABLE calendar AS
with recursive cte as (
	-- customize start date here
	select date('2020-01-01') as calendar_date
	union all
	select date_add(calendar_date, interval 1 day) as calendar_date from cte 
	-- customize end date here
	where year(date_add(calendar_date, interval 1 day)) <= year(now())
)
select
calendar_date,
year(calendar_date) as year,
monthname(calendar_date) as month,
day(calendar_date) as day
from cte;

--check range on calendar table
select min(calendar_date), max(calendar_date)
from calendar;


-- ************** THE BELOW REQUIRES THE SHOP LOG TO BE IMPORTED INTO MYSQL DB FIRST************
-- ingest member shopping activity via ingestMemberShopping.ipynb
-- join shopping data ("shop_log") to mem_type data; later: freq of shopping for trial members, while in-trial, and relate to type conversions
-- all recorded/historical shopping is categorized (in/pre/post trial) for each iteration of their trial; ie I don't attempt to segregate shopping activity and assign it to a trial (it's always all considered)
-- trialShoppingHabits = transactional table of trial member shopping activity (exclusively): one record per shopping trip that classifies it as in/pre/out of trial, whether a conversion occurred, total trips while in-trial
DROP TABLE IF EXISTS trialShoppingHabits;
CREATE TABLE trialShoppingHabits AS
WITH prep AS (
SELECT CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) = 'null' THEN NULL 
ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) as date) END AS trial_dt, 
email, type_clean, start_dt, ingest_date, trial_expiration
from mem_type_0927),
-- conversions CTE composed of only non-trial records where there WAS an originating trial for the email account; ie successful conversions
conversions AS (
SELECT  trial_dt, datediff(cast(start_dt as date), prep.trial_dt) as date_difference,
email, type_clean
from prep
where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
AND trial_dt is not null), 
-- stats = aggregation of trips/shopping visits by: email, trial_start_dt, trial_expiration, ingest_date
-- stats will only keep trial-related acitivty; 'conversions_trial_dt' field will denote whether the trial member converted
stats AS (
SELECT cast(mt.start_dt as date) AS trial_start_dt, mt.email, cast(trial_expiration as date) trial_expiration, mt.ingest_date,
CASE 
WHEN sl.Activity_Date BETWEEN mt.start_dt AND mt.trial_expiration THEN 'in trial' 
WHEN sl.Activity_Date < mt.start_dt THEN 'pre trial' 
ELSE 'post trial' 
END AS relative_trial_period, 
-- decipher whether the trial in question is the only one during this period; for purposes of accurate trial conversion
CASE 
WHEN mt.trial_dt = cast(mt.start_dt as date) THEN 'relevant trial' 
ELSE 'trial iteration expected' 
END AS final_trial_flag,
-- propogate the membership type, should they convert (derived from 'conversions' table)
max(conversions.type_clean) mo_type, 
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
GROUP BY 1,2,3,4,5,6)
-- total_trips covers ALL activity related to a trial start date and will include activity occurring before and after trial period
select stats.*, SUM(trips) OVER(PARTITION BY email, trial_start_dt) as total_trips,
CASE 
WHEN final_trial_flag = 'trial iteration expected' THEN 'defer to subsequent trial'
WHEN trial_expiration >= stats.ingest_date THEN 'in trial' 
-- trial had to have expired BEFORE ingest_date
WHEN mo_type is NULL AND trial_expiration <= ingest_date  THEN 'did not convert' 
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
AND mo_type is null
group by 1
order by 1 asc;

-- surface the actual trial members likely to convert: trial members that have shopped since trial has begun and still in-trial
-- partition/craft cohorts from trial start week
select 
date_add(trial_start_dt, interval  -WEEKDAY(trial_start_dt)-1 day) FirstDayOfWeek, (SELECT avg(tsh_m.trips) from trialShoppingHabits tsh_m WHERE date_add(tsh_m.trial_start_dt, interval  -WEEKDAY(tsh_m.trial_start_dt)-1 day) = date_add(tsh.trial_start_dt, interval  -WEEKDAY(tsh.trial_start_dt)-1 day) AND tsh_m.relative_trial_period = 'in trial') avg_for_cohort,
tsh.email, md.first_name, md.last_name,
tsh.trips, -- number of trips taken while in-trial
trial_expiration,
final_trial_flag,
relative_trial_period, phone
from trialShoppingHabits tsh
LEFT JOIN member_directory md ON tsh.email = md.email
WHERE tsh.trips > (SELECT avg(tsh_m.trips) from trialShoppingHabits tsh_m WHERE date_add(tsh_m.trial_start_dt, interval  -WEEKDAY(tsh_m.trial_start_dt)-1 day) = date_add(tsh.trial_start_dt, interval  -WEEKDAY(tsh.trial_start_dt)-1 day) AND tsh_m.relative_trial_period = 'in trial')
AND trial_expiration > date('2023-09-01')
-- ensure to only consider trips while in trial and the trial is the last iteration
AND relative_trial_period = 'in trial'
AND mo_type is null
AND final_trial_flag = 'relevant trial'
order by 1 desc, 5 desc;

-- (select max(mini.type_clean) from mem_type mini where mini.email = mt.email AND mini.type_clean not like '%trial%') 

where relative_trial_period = 'in trial'
group by 1
order by 1 asc;

-- potentially missed opportunities? former trial members that did not convert but either shopped a lot during their trial or have shopped since their trial expired (but they never converted)

/************* Find all member activity, including full membership journey, including those having a historical trial start
TODO: trial expirations don't populate properly here
**************/
WITH mt_ms AS (
SELECT mt.email mt_email, mt.start_dt mt_start_dt, mt.lead_date mt_lead_date, mt.type_clean mt_type_clean, mt.type_raw mt_type_raw, mt.trial_expiration mt_trial_expiration, ms.start_dt ms_start_dt, ms.lead_date ms_lead_date, ms.type_clean ms_type_clean, ms.type_raw ms_type_raw  
from mem_type_0927 mt
LEFT JOIN mem_status_0927 ms ON mt.email = ms.email
-- ensure that status records only populate on same line as the relevant type record
AND ms.start_dt between mt.start_dt AND mt.lead_date
order by mt.email, mt.start_dt asc, ms.start_dt asc),
-- stack the data; mt_type_clean = null WHEN 
-- excludes trial activity and only returns activity related to full member-owners
stacked AS (SELECT mt_email, mt_start_dt start_dt, mt_lead_date lead_date, mt_type_clean type_clean, 
null mt_type_clean, mt_type_raw type_raw 
FROM mt_ms
-- WHERE mt_type_clean IN ('lettuce', 'carrot', 'household', 'avocado')
UNION ALL
SELECT mt_email, ms_start_dt start_dt, ms_lead_date lead_date, ms_type_clean type_clean, mt_type_clean, ms_type_raw type_raw
FROM mt_ms)
-- ms_type_clean values related to trial activity are '%trial%', 'cancelled', 'deactivated'
-- WHERE ms_type_clean not like '%trial%')
select * 
from stacked 
WHERE mt_email = 'alex.b.moss@gmail.com'
-- group by 1,2,3,4,5,6 -- must group by in order to account for duplicated "type" rows (LHS)
order by 1,2;