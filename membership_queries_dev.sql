-- word wrap shortcut for DBeaver: shift+cntrl+alt+w
show tables;

show fields from mem_type0101;
select * from mem_type limit 5;
/* latest_trial2 = json of latest trial recorded for that email addres (from date of report pull)
 * 
 * 
 * */
 */
show fields from mem_status;

-- QA after data ingestion: will want to run this on a legacy table and the newer table
-- count unique emails for each type_clean (member type) where the descriptor contains text "new"
select CAST(EXTRACT(YEAR_MONTH FROM start_dt) AS CHAR) start_dt, type_clean, count(distinct email)
from mem_type_0101 
where type_raw LIKE ('%new%')
group by 1,2 
order by 1 desc, 3 desc;

select CAST(EXTRACT(YEAR_MONTH FROM start_dt) AS CHAR) start_dt, type_clean, count(distinct email)
from mem_type_1204 
where type_raw LIKE ('%new%')
group by 1,2 
order by 1 desc, 3 desc;

-- end ingestion QA

--  ********CONSOLIDATION QUERIES*******

-- create "type" table 
-- reference table for the variable instantiation statement = most recent report ingest
SET @initial_dt = (SELECT min(start_dt) FROM membership.mem_type_0217);
DROP TABLE IF EXISTS consolidated_mem_type; /* won't be able to do this going forward: consolidated_mem_type will be the persisted table */
CREATE TEMPORARY TABLE consolidated_mem_type AS
WITH consolidated AS (
-- TODO legacy table; to be updated later	
select * 
from membership.mem_type_1112 
where start_dt < @initial_dt 
UNION 
-- most recent mem_type report ingest
select * 
from membership.mem_type_0217)
-- accounting for duplicates
select type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, max(ingest_date) ingest_date 
from consolidated
group by 1,2,3,4,5,6,7,8,9;

-- create "status" table; the table reference for the latter table will need to be revised
SET @initial_dt = (SELECT min(start_dt) FROM membership.mem_status_0217);
DROP TABLE IF EXISTS consolidated_mem_status;
CREATE TABLE consolidated_mem_status AS
-- consolidate the legacy table with the most recent table
WITH consolidated AS (
select * 
-- legacy table will eventually be replaced with the legacy ver of consolidated_mem_status
from membership.mem_status_1112 
where start_dt < @initial_dt 
UNION 
select *
from membership.mem_status_0217)
-- accounting for duplicates
select type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, max(ingest_date) ingest_date 
from consolidated
group by 1,2,3,4,5,6,7;

-- delete & insertion approach: includes some QA before deleting the old version
/*
STEPS
1) create initial temporary table to store pre-existing consolidated tables
2) delete records from temp table that meet date criteria
3) insert records: new table 
4) create 2nd temp table that is free of dupes
5) run QA
6) replace prod table w/QAd 2nd temp table

*/
-- count duplicates in legacy table first 
SELECT type, type_raw,start_dt,lead_date, datetimerange,
type_clean,email, trial_expiration, latest_trial2, count(*)
FROM consolidated_mem_type
group by 1,2,3,4,5,6,7,8,9
having count(*) > 1;

-- STEP 1
DROP TABLE consolidated_mem_type_temp; -- if exists
-- consolidated_mem_type is the legacy prod table
CREATE TEMPORARY TABLE consolidated_mem_type_temp LIKE consolidated_mem_type;
INSERT INTO consolidated_mem_type_temp SELECT * FROM consolidated_mem_type;

-- STEP 2: DELETE RECORDS MEETING CRITERIA FROM TEMPORARY TABLE VERSION
-- replace w/most recent report download
SET @initial_dt = (SELECT min(start_dt) FROM membership.mem_type_0217);
DELETE FROM consolidated_mem_type_temp WHERE start_dt > @initial_dt;

-- STEP 3: insert new records into first temp table
-- make sure to account for 'ingest_date' field
INSERT INTO consolidated_mem_type_temp
select type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, max(ingest_date) ingest_date
-- new table of data
from membership.mem_type_0217 
GROUP BY 1,2,3,4,5,6,7,8,9;

-- STEP 4 - DELETE DUPES: requires making ANOTHER temp table; this NEW table ("consolidated_mem_type_temp2") is the new de-duped membership table, and is the PROD version going forward
CREATE TABLE consolidated_mem_type_temp2 LIKE consolidated_mem_type_temp;

INSERT INTO consolidated_mem_type_temp2
WITH row_num_table AS (
SELECT c_temp.*, row_number() OVER(PARTITION BY type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2 order by ingest_date asc) row_num
FROM consolidated_mem_type_temp c_temp)
SELECT type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2,ingest_date
FROM row_num_table 
WHERE row_num = 1;

-- STEP 5: QA
-- QA #1: count duplicates in new table (all fields save for ingest_date); 
-- DESIRED RESULTSET: blank table (no records)
SELECT type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, ingest_date, count(*) 
FROM consolidated_mem_type_temp2
group by 1,2,3,4,5,6,7,8,9,10
having count(*) > 1;

-- QA #2: establish a sense of # of records that should have been deleted and be added to new version of consolidated_mem_type
SET @initial_dt = (SELECT min(start_dt) FROM membership.mem_type_0217);
SET @prevcount = (SELECT COUNT(*) FROM membership.mem_type_0217 WHERE start_dt < @initial_dt); 
SET @newcount = (SELECT COUNT(*) FROM consolidated_mem_type2 WHERE start_dt > @initial_dt);

-- conditional logic: check whether record count of consolidated_mem_type_temp2 ~ equals the sum(prevcount, newcount); if this passes, proceed to STEP 6

-- 9,677 records
SELECT count(*)
FROM consolidated_mem_type;

-- does this count make sense?
SELECT count(*)
FROM consolidated_mem_type_temp2;

-- STEP 6: clean up tables and establish the new prod version of "consolidated_mem_type"
DROP TABLE consolidated_mem_type;
DROP TABLE consolidated_mem_type_temp;
CREATE TABLE consolidated_mem_type SELECT * FROM consolidated_mem_type_temp2;

/* TODO NEXT STEP: run stack_job table creation query */

-- QA the consolidation 
select count(*) 
from consolidated_staged;

select count(*) 
from membership.mem_type_1112;

select count(*) 
from membership.mem_type_0217;
-- END CONSOLIDATION QUERY


-- mem_type.type_clean = {park slope, trial, lettuce, 6 mo trial, carrot, apple, household, avocado, bushwick}

-- one-off queries to investigate particular persons
select * 
from consolidated_mem_type
where email IN ('lilien.sophie@gmail.com') 
order by start_dt;
-- one-off queries to investigate particular persons

select * 
from consolidated_mem_status
where email IN ('amelia.h.clark@gmail.com') 
order by email , start_dt asc;

select * 
from stack_job2 sj 
where mt_email IN ('lilien.sophie@gmail.com') 
order by mt_email , start_dt asc;

/*mem_type_1112,mem_status_1112
consolidated_mem_status,consolidated_mem_type*/
DROP TABLE consolidated_mem_type;
DROP TABLE IF EXISTS consolidated_mem_type;
DROP TABLE IF EXISTS consolidated_mem_status;
CREATE TABLE consolidated_mem_type SELECT * FROM mem_type_1112;
CREATE TABLE consolidated_status_type SELECT * FROM mem_status_1112;


SELECT mt.email mt_email, mt.start_dt mt_start_dt, mt.lead_date mt_lead_date, mt.type_clean mt_type_clean, mt.type_raw mt_type_raw, mt.trial_expiration mt_trial_expiration, ms.start_dt ms_start_dt, ms.lead_date ms_lead_date, ms.type_clean ms_type_clean, ms.type_raw ms_type_raw  
from mem_type_0927 mt
LEFT JOIN mem_status_0927 ms ON mt.email = ms.email
AND ms.start_dt between mt.start_dt AND mt.lead_date
where 1=1 
AND mt.email = 
AND 
order by mt.email, mt.start_dt asc, ms.start_dt asc;

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
from consolidated_mem_type
where type_clean like '%trial%'
group by 1
order by 1 desc;

-- historical membership starts
select date_format(start_dt, '%Y-%m') as month, count(distinct email) as count
from consolidated_mem_type
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


/* TRIAL CONVERSION SUCCESS */

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

-- prod version of ipynb file; this is a historical performance analysis
WITH trial_conversions AS (
-- returns the last trial (can be > 1)
select cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) trial_date, cast(start_dt as date) status_date, 
datediff(cast(start_dt as date), cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date)) as date_difference,
email, type_clean
from mem_type_0130
where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
AND cast(JSON_EXTRACT(latest_trial2, '$.start_dt')as date) is not null),
-- return all trials as recognized on all other fields other than latest_trial2
all_trials AS (
select cast(start_dt as date) status_date, email, type_clean
from mem_type_0130
where type_clean like '%trial%'),
conversion_agg AS (
select date_format(trial_date, '%%Y-%%m') as month, count(distinct email) as converted_cnt
from trial_conversions 
group by 1),
all_agg AS (
select date_format(status_date, '%%Y-%%m') as month, count(distinct email) as trials_cnt
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

-- TODO: compare the stats from the trial conversion sql on the same period across two sets of tables. One suspicious case: 

/******************
PLOTTING ACTIVE MEMBERS (***trial activity is removed****)
Multi-part query, as they each represented a different approach:
1) stack all type and status activity, including the range of duration; then update the lead_date on the "type" row so that it accounts for the next "status" start_dt (as these are calculated independently on the python program)
2) buildin mt_ms, but doesn't stack resultset, so may not be useful
3) stacks like #1 but conditionally selects for 'prevailing' status and types <- could be incoporated into #1, but haven't checked

*******************/

-- #1
-- join mem_status to mem_type, which is the only way to associate prevailing membership type (mem_typeXXX.type_clean) to mem_status activity; then stack so that each row is a membership event period for ea email (records having null values for mt_type_clean are mem_type original records; it's mem_status that will have non-null values)
-- UPDATING STATEMENT to replace the "lead_date" for "TYPE" rows
-- the lead and start_dt fields are exclusive to either type or status changes, this is bc I related all status changes to the prevailing type and I need the type range in order to accomplish that; this means I HAVE TO recompute the lead/start date post compilation

DROP TABLE IF EXISTS stack_job;
CREATE TABLE stack_job AS
WITH mt_ms AS (
SELECT mt.email mt_email, mt.start_dt mt_start_dt, mt.lead_date mt_lead_date, mt.type_clean mt_type_clean, mt.type_raw mt_type_raw, mt.trial_expiration mt_trial_expiration, 
-- add a cancel flag, which is necessary for cases where there are no mem_status records (typically observed when members' sign up date < 2019)
CASE WHEN mt.type_raw LIKE '%Cancelled%' THEN 'Y' ELSE 'N' END mt_cancel_flag,
ms.start_dt ms_start_dt, ms.lead_date ms_lead_date, ms.type_clean ms_type_clean, ms.type_raw ms_type_raw  
from consolidated_mem_type mt
-- from mem_type_1204 mt (older version of one-off)
LEFT JOIN consolidated_mem_status ms ON mt.email = ms.email
-- LEFT JOIN mem_status_1204 ms ON mt.email = ms.email (older version of one-off)
-- ensure that status records only populate on same line as the relevant type record
AND ms.start_dt between mt.start_dt AND mt.lead_date
order by mt.email, mt.start_dt asc, ms.start_dt asc),
-- stack the data; mt_type_clean = null signals records from original mt_type (part of the UNION)
-- excludes trial activity and only returns activity related to full member-owners
stacked AS (
SELECT mt_email, mt_start_dt start_dt, mt_lead_date lead_date, mt_type_clean activity, mt_type_clean mem_type, mt_type_raw type_raw, mt_cancel_flag
FROM mt_ms
WHERE mt_type_clean IN ('lettuce', 'carrot', 'household', 'avocado','apple')
UNION ALL
SELECT mt_email, ms_start_dt start_dt, ms_lead_date lead_date, ms_type_clean activity, mt_type_clean mem_type, ms_type_raw type_raw, NULL mt_cancel_flag
FROM mt_ms
-- ms_type_clean values related to trial activity are '%trial%', 'cancelled', 'deactivated'
WHERE ms_type_clean not like '%trial%' 
AND lower(mt_type_clean) NOT LIKE '%trial')
select stacked.*, CASE WHEN activity = mem_type THEN 'initial enrollment' ELSE activity END AS activity_calc, 
-- experimental text 
TRIM(regexp_substr(type_raw,'(?<=Status:).*$')) AS text_status_indicator
from stacked 
WHERE 1=1 
-- AND mt_email IN ('fenailletom@gmail.com','405sarah@gmail.com')
group by 1,2,3,4,5,6,7 -- must group by in order to account for duplicated "type" rows (LHS)
order by 1,2;


-- #1b: UPDATE stack_job table (aka overwrite)
-- PURPOSE: replace the original lead_date with (1-start_dt) of the subsequent row (partitioned by email) in order to enhance accuracy of date ranges
-- prelim table is a helper table to build the new lead date
-- might need to compute a LEAD() first then run a WHERE clause in a second query to exclude accounts that didn't log a status change
-- what happens within mysql during an UPDATE statement: https://itnext.io/what-happens-during-a-mysql-update-statement-7aafbb1ecc01
WITH prelim AS (
SELECT stack_job.*, LEAD(date_sub(start_dt, interval 1 day)) OVER(PARTITION BY mt_email ORDER BY start_dt) date_lead2
FROM stack_job
-- WHERE mt_email IN ('fenailletom@gmail.com','405sarah@gmail.com')
order by 1,2) 
UPDATE stack_job AS sj2 
-- first apply the inner join, then set values of one column (date_lead) to the other col (date_lead2) ON THE SAME ROW; the alternative would be to write a CASE statement, but then I'd end up with a row with which to deal
-- lead the start_dt of the proceeding status, if one exists and attempt to replace the lead_date field for the type row
INNER JOIN (SELECT * FROM prelim where activity_calc = 'initial enrollment') x 
ON sj2.mt_email = x.mt_email AND sj2.activity = x.activity -- ensure that we only update the first 'type' row
SET sj2.lead_date = x.date_lead2 
WHERE sj2.activity_calc = 'initial enrollment' AND x.date_lead2 IS NOT NULL;

-- set row numbers: this will come in handy 
DROP TABLE stack_jobII;
CREATE TABLE stack_jobII AS
SELECT sj.*, ROW_NUMBER() OVER(PARTITION BY mt_email ORDER BY start_dt asc) AS row_num, 
-- WIP: this will be useful; finish this logic (11/13)
COUNT(*) OVER(PARTITION BY mt_email) total_rows 
FROM stack_job sj;

-- UPDATE lead_date on the last record for each email to curren date; lead_date in the case of the final row by email is hard coded to pipeline run date on the .ipynb file, and can be stale... but ultimately, that was the last run date, and most precise

/* values text_status_indicator; these will need to be recorded (overwite) in the activity_calc field
Cancelled
Deactivated
General Leave
Expired
*/

-- UPDATE "activity_calc" in cases where there is only one record for the member (total_rows = 1)
WITH prelim AS (
SELECT stack_jobII.*, 
-- regex to exclude all text prior to "_"
CASE WHEN TRIM(LOWER(regexp_substr(text_status_indicator,'^.*(?=(_))'))) IN ('cancelled', 'deactivated', 'general leave', 'expired') 
THEN 'deactivate' 
ELSE activity_calc END AS activity_calc_alt
FROM stack_jobII
-- WHERE mt_email IN ('fenailletom@gmail.com','405sarah@gmail.com')
order by 1,2) 
UPDATE stack_jobII AS sj2 
-- first apply the inner join, then set values of one column (date_lead) to the other col (date_lead2) ON THE SAME ROW; the alternative would be to write a CASE statement, but then I'd end up with a row with which to deal
-- lead the start_dt of the proceeding status, if one exists and attempt to replace the lead_date field for the type row
INNER JOIN (SELECT * FROM prelim where activity_calc = 'initial enrollment') x 
ON sj2.mt_email = x.mt_email AND sj2.activity = x.activity -- ensure that we only update the first 'type' row
SET sj2.activity_calc = x.activity_calc_alt 
WHERE sj2.activity_calc = 'initial enrollment' 
-- apply update on cases where there is a single activity record 
AND sj2.row_num = 1 
AND sj2.total_rows = 1;


/************************
* number of unique emails contained
*
**************************/
SELECT count(distinct mt_email)
FROM stack_jobII;

-- compare to the intake group
SELECT count(distinct email)
FROM mem_type_1112 
-- WHERE lower(type_clean) NOT LIKE '%trial%'
WHERE lower(type_clean) IN ('lettuce','carrot','apple','avocado','household');
-- AND lower(type_clean) NOT IN ('park slope','bushwick');

-- who's missing from stack_jobII that was initially intaken 
select email FROM mem_type_1112 
WHERE email not in (SELECT mt_email FROM stack_jobII where lower(type) NOT like '%trial%' group by 1) 
AND lower(type_clean) IN ('lettuce','carrot','apple','avocado','household')
limit 10;

-- QA: ensure that the stack job table has the proper date alignment from the UPDATE statement (UPDATE statement persisted)
SELECT mt_email, start_dt, activity, activity_calc, text_status_indicator, row_num, total_rows 
FROM stack_jobII
order by mt_email, start_dt asc 
limit 20;

-- QA: review distribution
SELECT activity, activity_calc, text_status_indicator, count(mt_email) cnt
FROM stack_jobII
group by 1,2,3 
order by 1,2,3;

-- QA: review problematic cases where for whatever reason 'initial enrollment' isn't properly overwritten on the UPDATE statement (cases where text_status_indicator contains 'Cancelled', 'Deactivated', 'General Leave'); reference: problematic_activityCalc.ods
-- KIM appearance of 'initial enrollment' is OK when total_rows > 1
-- unsure if these are truly problematic: unknown whether the "cancel" indication in the text_status_indicator is accurate or if present after a retroactive process (ie the account eventually cancels, and this event is applied retroactively to the text_status_indicator from account inception)
SELECT *
FROM stack_jobII
WHERE (text_status_indicator like '%Cancelled%' OR text_status_indicator like '%Deactivated%')
AND activity_calc = 'initial enrollment' 
AND total_rows > 1 
order by mt_email, start_dt asc;

-- individual cases:
SELECT *
FROM stack_jobII
WHERE mt_email IN ('achomet@gmail.com', 'amandabfriedman@gmail.com', 'anmuessig@gmail.com', 'bcgilman@gmail.com', 'irunwithscissorz@hotmail.com')
ORDER BY mt_email, start_dt asc;

-- QA: ensure that stored procedures properly bring all the lead_date forward to the appropriate date
SELECT lead_date, count(*)
FROM consolidated_mem_status_temp2
GROUP BY 1
ORDER BY 1 DESC;

SELECT lead_date, count(*)
FROM consolidated_mem_type_temp2
GROUP BY 1
ORDER BY 1 DESC;

-- #2
-- measure active members
-- in order to graph the # of members in a timeseries, will need to join the above to the calendar table
-- 'cancelled', 'care giving leave', 'general leave', 'medical leave', 'parental leave' <- marks the suspension of membership: if this occurs, the mt_type_clean is not downgraded, but preserved, so, I have to refer to the ms_start_dt of the suspension
-- two pieces of data will come from 'cancel' lines: 1) the start date of the membership tier and the period of the cancellation/suspension: ostensibly refer to the range on the mt "side" if ms_type_clean is null, if not null
-- mt_ms joins ms (status) data to mt (type), providing for status changes (ex. winbacks and leave) to be associated to member type; ms_start_date and ms_lead_date contain the dates in which the status is in effect, all of which should be within the range of the over-arching membership type
-- stacking mt and ms data allows for building a case statement that will allow me to select the prevailing status, as long as every line contains mt_start_dt and mt_lead_dt

-- displays the types of activity_calc
/*initial enrollment - active
cancelled
general leave
winback - active
deactivated
technical activation - active
parental leave
technical reactivation - active
medical leave
care giving leave
technical re-activation - active  
('winback','initial enrollment','technical activation','technical reactivation','technical re-activation')
('deactivated','parental leave','medical leave','care giving leave')
*/

select activity_calc 
from stack_job 
group by 1;

-- exploratory code demonstrates proof that the JOIN works
SELECT *
FROM stack_job sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
WHERE mt_email = '16defazioe@gmail.com' 
order by calendar_date asc;

-- #3 aggregation: counting the number of active accounts on a daily basis
-- this MIGHT be what is ultimately fed into the ipynb file for visualization
-- the below ONLY includes FULL member-owners
SELECT calendar_date, 
SUM(CASE WHEN activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation') THEN 1 ELSE 0 END) AS active_cnts, 
SUM(CASE WHEN activity_calc IN ('parental leave','medical leave','care giving leave') THEN 1 ELSE 0 END) AS temporary_inactivte, 
SUM(CASE WHEN activity_calc IN ('cancelled','deactivated','deactive','suspended') THEN 1 ELSE 0 END) revoked,
SUM(CASE WHEN activity_calc IN ('general leave') THEN 1 ELSE 0 END) general_leave, 
SUM(CASE WHEN activity_calc IN ('winback') THEN 1 ELSE 0 END) winbacks
FROM stack_job2 sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
GROUP BY 1 
ORDER BY 1 desc;

-- review records of those accounts that are not considered 'active' as of a certain date to ensure accuracy
/* I need to review three use cases: 
1) those accounts that are found in the db tables AND the CIVI dataset, yet are NOT flagged as active in my logic; 2) accounts appearing in CIVI that are missing entirely from my db; 
3) accounts flagged as active in the db but missing from CIVI
*/
-- the below accounts are NOT showing as active as of 12/1 when there is a case that they should be... this highlights the problem of cases where total_rows = 1 
-- the query below returns all accounts that SHOULD NOT be considered active ao 12/1 according to the logic to-date
SELECT * 
FROM stack_job2 sj
WHERE mt_email NOT IN (
SELECT sj.mt_email 
FROM stack_job2 sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
WHERE activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation')
AND cal.calendar_date = DATE('2023-11-01') 
group by 1) 
order by mt_email, start_dt asc 
limit 500;

-- mis-stated cases: the emails below do not carry an "active"-type activity_calc value, but are shown to be active per CIVI (active member report)
WITH sj AS (
SELECT sj.* 
from stack_jobII sj
WHERE mt_email IN (
SELECT sj.mt_email 
FROM stack_jobII sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
WHERE activity_calc NOT IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation')
AND cal.calendar_date = DATE('2023-11-01') 
group by 1))
SELECT sj.mt_email, sj.start_dt, sj.lead_date, sj.type_raw, sj.activity, sj.mem_type, sj.activity_calc, sj.row_num, sj.total_rows, ar.Email current_email  
FROM sj
INNER JOIN active_roster ar ON trim(lower(sj.mt_email)) = trim(lower(ar.Email))
group by 1,2,3,4,5,6,7,8,9,10
order by sj.mt_email, sj.start_dt asc;

-- QA the surge of new members observed between 10/15/2024 AND 12/01/2024
SELECT *
FROM stack_job2
WHERE start_dt BETWEEN date('2024-10-15') AND date('2024-12-01')
ORDER BY mt_email, start_dt
LIMIT 20;

-- missing cases: these emails are 
-- "active_roster" is the CIVI table (imported into db by way of 'ingestMemberShopping.ipynb'
WITH total_set AS (
SELECT sj.mt_email, sj.start_dt, sj.lead_date, sj.type_raw, sj.activity, sj.mem_type, sj.activity_calc, sj.row_num, sj.total_rows, ar.Email current_email  
FROM stack_job2 sj
RIGHT JOIN active_roster ar ON trim(lower(sj.mt_email)) = trim(lower(ar.Email))
group by 1,2,3,4,5,6,7,8,9,10)
SELECT *
from total_set
-- this ensures that I'm considering the most recent cases
WHERE row_num = total_rows
-- group by 1 
order by mt_email, lead_date asc;

-- return the whole universe of active accounts (per the db logic AND CIVI)
-- "active_roster" is the CIVI table (imported into db by way of 'ingestMemberShopping.ipynb' - "Current Membership Detailed Report" <- as labeled in CIVI Reports)
WITH total_set AS (
SELECT sj.mt_email, sj.start_dt, sj.lead_date, sj.type_raw, sj.activity, sj.mem_type, sj.activity_calc, sj.row_num, sj.total_rows, ar.Email current_email  
FROM stack_jobII sj
RIGHT JOIN active_roster ar ON trim(lower(sj.mt_email)) = trim(lower(ar.Email)) AND row_num = total_rows
group by 1,2,3,4,5,6,7,8,9,10)
SELECT *
from total_set
-- this ensures that I'm considering the most recent cases
-- group by 1 
order by mt_email, lead_date asc;

-- more QA of active roster: 


-- investigate cases of accounts not indicated as ACTIVE in stack_jobII
SELECT *
FROM stack_jobII sj
WHERE mt_email IN ('oba1620@gmail.com','kevin@keogh.dev','aiisshiki@gmail.com','jmullen006@gmail.com',
'akiko.ichikawa@gmail.com','cj@cirr.us','chrisgo22@gmail.com','varybl@gmail.com','ethan.rts@gmail.com') 
order by mt_email, lead_date asc;


-- investigate some emails from the above and determine if they were rightfully excluded from 'active' count as 12-1-2023
SELECT *
from consolidated_mem_status_temp2
WHERE email IN ('1elsamaki@gmail.com') 
order by email;

SELECT *
from consolidated_mem_type_temp2
WHERE email IN ('1elsamaki@gmail.com') 
order by email;

SELECT *
from mem_status_1204
WHERE email IN ('aubreygrowsfood@gmail.com','cbm389@nyu.edu','luvness06@yahoo.com') 
order by email;


-- QA: exposes how "initial enrollment" is extremely inflated; fix this in the UPDATE statement above
-- reveals the "mix" of members: those on initial enrollment, those wonback, etc
SELECT activity_calc, count(*) cnt
FROM stack_job2 sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
WHERE cal.calendar_date = date('2023-11-05')
AND activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation')
GROUP BY 1 
ORDER BY 1 desc;

-- #3b (legacy, candidate for removal)
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

-- ACCOUNT FLOWS TABLE
/* 
BUCKETS: 1) starting membership, 2) Leave, 3) Return from Leave, 4) new signups, 5) trial converters
this may require two snapshots: last day of previous month and last day of current month
- trick is to handle same-month reversals (ex. signup then suspend)


NOTE: In order for an account's activity to be reflected in the Accounts Flow Table, they must have been categorized as an ACTIVE member the previous month <- if that is not the case, then their inclusion will mess up the NET amounts... this may indicate that I'll need to vet each member at the member/email level and not at the "monthly" aggregate level as done here
*/

-- the below query returns a two row table of category/account values/freq by date ("current" and "prev")
-- ****MUST BE RUN AS A SCRIPT IN DBEAVER*****
SET @current = date('2024-05-31');
SET @prev = date('2024-04-30');

SELECT @current AS date,
SUM(CASE WHEN activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation') THEN 1 ELSE 0 END) AS active_cnts, 
SUM(CASE WHEN activity_calc IN ('parental leave','medical leave','care giving leave') THEN 1 ELSE 0 END) AS temporary_inactivte, 
SUM(CASE WHEN activity_calc IN ('cancelled','deactivated','deactive','suspended') THEN 1 ELSE 0 END) revoked,
SUM(CASE WHEN activity_calc IN ('general leave') THEN 1 ELSE 0 END) general_leave, 
SUM(CASE WHEN activity_calc IN ('winback') THEN 1 ELSE 0 END) winbacks
FROM stack_job2 sj
WHERE @current between start_dt AND lead_date
UNION
SELECT @prev AS date, 
SUM(CASE WHEN activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation') THEN 1 ELSE 0 END) AS active_cnts, 
SUM(CASE WHEN activity_calc IN ('parental leave','medical leave','care giving leave') THEN 1 ELSE 0 END) AS temporary_inactivte, 
SUM(CASE WHEN activity_calc IN ('cancelled','deactivated','deactive','suspended') THEN 1 ELSE 0 END) revoked,
SUM(CASE WHEN activity_calc IN ('general leave') THEN 1 ELSE 0 END) general_leave, 
SUM(CASE WHEN activity_calc IN ('winback') THEN 1 ELSE 0 END) winbacks
FROM stack_job2 sj
WHERE @prev between start_dt AND lead_date;


SET @current_mo = date('2024-05-31')
SET @prev_mo = date('2024-04-30')

-- merge prev month and current month, then capture ea scenario in a CASE statement
WITH may AS
(SELECT CASE 
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%leave%' THEN 'membership_leave'
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%suspend%' THEN 'membership_suspend'
-- careful w/"activation" bc this will include reactivations
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%activation%' THEN 'membership_activation'
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%deactivate%' THEN 'membership_deactivate'
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%enrollment%' THEN 'membership_enrollment'
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%winback%' THEN 'membership_winback' 
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%cancell%' THEN 'membership_cancel'
WHEN mem_type = 'carrot' THEN 'trial activity'
END AS bucket,
COUNT(mt_email) may_email_cnt
FROM stack_job2
WHERE date('2024-05-31') BETWEEN start_dt AND lead_date
GROUP BY 1 
ORDER BY 2 DESC),
april AS
(SELECT CASE 
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%leave%' THEN 'membership_leave'
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%suspend%' THEN 'membership_suspend'
-- careful w/"activation" bc this will include reactivations
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%activation%' THEN 'membership_activation'
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%deactivate%' THEN 'membership_deactivate'
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%enrollment%' THEN 'membership_enrollment'
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%winback%' THEN 'membership_winback' 
WHEN mem_type IN('apple','avocado','household','lettuce','carrot') AND activity_calc LIKE '%cancell%' THEN 'membership_cancel'
WHEN mem_type = 'carrot' THEN 'trial activity'
END AS bucket,
COUNT(mt_email) april_email_cnt
FROM stack_job2
WHERE date('2024-04-30') BETWEEN start_dt AND lead_date
GROUP BY 1 
ORDER BY 2 DESC)
SELECT may.bucket, may.may_email_cnt, april.april_email_cnt
FROM may
INNER JOIN april
ON may.bucket = april.bucket;

-- ACCOUNT FLOW VER.II: snapshot approach - the "status" of EACH member at a point in time is compared to the status at a previous/later point in time. Important to handle cases of newly appearing members (weren't in the table the t-1 period)
-- NOTE I: stack_job2 excludes trial activity, so I'll need to bring that in via joining to another table
WITH curr AS (
select mt_email curr_mt_email, mem_type curr_mem_type, activity_calc curr_activity_calc, activity curr_activity 
from stack_job2 
WHERE date('2024-05-31') between start_dt AND lead_date
ORDER BY mt_email),
prev AS (
select mt_email prev_mt_email, mem_type prev_mem_type, activity_calc prev_activity_calc, activity prev_activity 
from stack_job2 
WHERE date('2024-04-31') between start_dt AND lead_date
ORDER BY mt_email), 
trial AS (
select email trial_email, type_clean trial_type_clean, trial_expiration, latest_trial2 
from consolidated_mem_type
WHERE type_clean = JSON_EXTRACT(latest_trial2, '$.type_clean')
AND type_clean LIKE '%trial%'
order by email)
SELECT *
FROM curr
LEFT JOIN prev ON curr_mt_email = prev_mt_email
LEFT JOIN trial ON curr_mt_email = trial_email
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY curr_mt_email
limit 1000;

-- relevant account balances
WITH curr AS (
select mt_email curr_mt_email, mem_type curr_mem_type, activity_calc curr_activity_calc, activity curr_activity 
from stack_job2 
WHERE date('2024-05-31') between start_dt AND lead_date
ORDER BY mt_email),
prev AS (
select mt_email prev_mt_email, mem_type prev_mem_type, activity_calc prev_activity_calc, activity prev_activity 
from stack_job2 
WHERE date('2024-04-30') between start_dt AND lead_date
ORDER BY mt_email),
final_tbl AS (
SELECT date('2024-05-31') current_month, curr_activity_calc, prev_activity_calc, count(distinct curr_mt_email) unq_email
FROM curr
LEFT JOIN prev ON curr_mt_email = prev_mt_email
-- WHERE curr_mem_type <> prev_mem_type
GROUP BY 1,2,3),
agg_tbl AS (
SELECT current_month, 
CASE 
-- leave
WHEN curr_activity_calc = 'general leave' AND prev_activity_calc =	'general leave' THEN unq_email
WHEN curr_activity_calc = 'medical leave' AND prev_activity_calc = 'medical leave' THEN unq_email
WHEN curr_activity_calc = 'parental leave'	AND prev_activity_calc = 'parental leave' THEN unq_email
WHEN curr_activity_calc = 'parental leave' AND prev_activity_calc = 'technical activation' THEN unq_email
WHEN curr_activity_calc = 'general leave' AND prev_activity_calc = 'technical activation' THEN unq_email
WHEN curr_activity_calc = 'care giving leave' AND prev_activity_calc = 'care giving leave' THEN unq_email
WHEN curr_activity_calc = 'general leave' AND prev_activity_calc = 'winback' THEN unq_email
WHEN curr_activity_calc = 'general leave' AND prev_activity_calc =	'initial enrollment' THEN unq_email
WHEN curr_activity_calc = 'general leave' AND prev_activity_calc =	'technical reactivation' THEN unq_email
WHEN curr_activity_calc = 'medical leave' AND prev_activity_calc = 'winback' THEN unq_email 
ELSE NULL 
END AS leave_balance,

-- active
CASE WHEN curr_activity_calc = 'initial enrollment' AND prev_activity_calc = 'initial enrollment' THEN unq_email
WHEN curr_activity_calc = 'technical activation' AND prev_activity_calc = 'technical activation' THEN unq_email
WHEN curr_activity_calc = 'technical reactivation' AND prev_activity_calc = 'technical reactivation' THEN unq_email 
WHEN curr_activity_calc = 'initial enrollment' AND prev_activity_calc =	NULL THEN unq_email 
WHEN curr_activity_calc = 'winback' AND prev_activity_calc = NULL THEN unq_email
ELSE NULL
END AS active_balance,

-- winbacks
CASE WHEN curr_activity_calc = 'winback' AND prev_activity_calc =  'winback' THEN unq_email 
WHEN curr_activity_calc = 'winback' AND prev_activity_calc = 'cancelled' THEN unq_email
WHEN curr_activity_calc = 'winback'	AND prev_activity_calc = 'medical leave' THEN unq_email
WHEN curr_activity_calc = 'winback'	AND prev_activity_calc = 'general leave' THEN unq_email
WHEN curr_activity_calc = 'technical reactivation' AND prev_activity_calc = 'winback' THEN unq_email
WHEN curr_activity_calc = 'winback'	AND prev_activity_calc = 'deactivated' THEN unq_email
WHEN curr_activity_calc = 'winback' AND prev_activity_calc = 'suspended' THEN unq_email
ELSE NULL
END AS winback_balance,

-- suspended/cancellation
CASE WHEN curr_activity_calc = 'cancelled' AND prev_activity_calc = 'technical activation' THEN unq_email
WHEN curr_activity_calc = 'cancelled' AND prev_activity_calc = 'winback' THEN unq_email
WHEN curr_activity_calc = 'suspended' AND prev_activity_calc = 'suspended' THEN unq_email
WHEN curr_activity_calc = 'deactivated'	AND prev_activity_calc = 'initial enrollment' THEN unq_email 
WHEN curr_activity_calc = 'suspended' AND prev_activity_calc = 'winback' THEN unq_email
WHEN curr_activity_calc = 'cancelled' AND prev_activity_calc =	'initial enrollment' THEN unq_email
ELSE NULL
END AS suspended_balance,

-- re-activation
CASE WHEN curr_activity_calc = 'technical activation' AND prev_activity_calc = 'parental leave' THEN unq_email 
WHEN curr_activity_calc = 'technical activation' AND prev_activity_calc = 'general leave' THEN unq_email 
WHEN curr_activity_calc = 'technical activation' AND prev_activity_calc = 'cancelled' THEN unq_email
ELSE NULL
END AS reactivated_balance
FROM final_tbl) 
-- TALLY ALL CATEGORIES (leave, active, winbacks)
SELECT current_month,
SUM(leave_balance) leave_balance,
SUM(IFNULL(active_balance,0) + IFNULL(winback_balance,0) + IFNULL(reactivated_balance,0)) active_balance, 
SUM(suspended_balance) suspended_balance
FROM agg_tbl 
GROUP BY 1;

-- ACCOUNT FLOWS PROD TABLE (.IPYNB FILE)
-- the below code snippet produces the combination of month t and month t-1 "activity calcs" aka the status combinations for a hard-coded pair of months; these counts (unique emails) are ultimately multiplied by the "categorical" matrix that assigns flows to Account Flow categories
-- prod version recorded in account_flows.ipynb
WITH curr AS (
select mt_email curr_mt_email, mem_type curr_mem_type, activity_calc curr_activity_calc, activity curr_activity 
from stack_job2 
WHERE date('2024-05-31') between start_dt AND lead_date 
ORDER BY mt_email), 
prev AS (
select mt_email prev_mt_email, mem_type prev_mem_type, activity_calc prev_activity_calc, activity prev_activity 
from stack_job2 
WHERE date('2024-04-30') between start_dt AND lead_date 
ORDER BY mt_email), 
final_tbl AS (
SELECT date('2024-05-31') current_month, curr_activity_calc, prev_activity_calc, count(distinct curr_mt_email) unq_email 
FROM curr 
LEFT JOIN prev ON curr_mt_email = prev_mt_email 
GROUP BY 1,2,3) 
SELECT * 
FROM final_tbl;

"WITH curr AS (
select mt_email curr_mt_email, mem_type curr_mem_type, activity_calc curr_activity_calc, activity curr_activity 
from stack_job2 
WHERE date('"+tup_dates[1]+"') between start_dt AND lead_date 
ORDER BY mt_email), 
prev AS (
select mt_email prev_mt_email, mem_type prev_mem_type, activity_calc prev_activity_calc, activity prev_activity 
from stack_job2 
WHERE date('"+tup_dates[0]+"') between start_dt AND lead_date 
ORDER BY mt_email), 
final_tbl AS (
SELECT date('"+tup_dates[1]+"') current_month, curr_activity_calc, prev_activity_calc, count(distinct curr_mt_email) unq_email 
FROM curr 
LEFT JOIN prev ON curr_mt_email = prev_mt_email 
GROUP BY 1,2,3) 
SELECT * FROM final_tbl"

-- APPROXIMATE A CHURN RATE FOR PSFC LOAN
SET @current_dt = date('2024-06-01');
SET @t_30 = DATE_SUB(@current_dt, INTERVAL 30 DAY);
-- on all rows of stack_job2 I can access activity and mem_type; dates are in timsteamp
WITH curr AS (
select mt_email curr_mt_email, mem_type curr_mem_type, activity_calc curr_activity_calc, activity curr_activity 
from stack_job2 
WHERE @current_dt between start_dt AND lead_date 
ORDER BY mt_email), 
prev AS (
select mt_email prev_mt_email, mem_type prev_mem_type, activity_calc prev_activity_calc, activity prev_activity 
from stack_job2 
WHERE @t_30 between start_dt AND lead_date 
ORDER BY mt_email),
-- join the two above to determine the designations: pairs of t0/t-30
assigned_status AS (
SELECT *, @current_dt current_dt,
CASE WHEN curr_activity_calc IN ('technical activation', 'technical reactivation', 'winback','initial enrollment') 
AND prev_activity_calc IN ('technical activation', 'technical reactivation', 'winback','initial enrollment')
THEN "active throughout"
WHEN curr_activity_calc IN ('cancelled', 'care giving leave', 'deactivate', 'deactivated', 'disability leave', 'general leave', 'medical leave',
'parental leave', 'suspended') 
AND prev_activity_calc IN ('technical activation', 'technical reactivation', 'winback','initial enrollment')
THEN "churners"
WHEN curr_activity_calc IS NULL
AND prev_activity_calc IN ('technical activation', 'technical reactivation', 'winback','initial enrollment')
THEN "churners"
ELSE 'irrelevant'
END AS mom_active_status
FROM curr 
LEFT JOIN prev ON curr_mt_email = prev_mt_email 
ORDER BY curr_mt_email) 
SELECT current_dt, mom_active_status , count(*) cnt
FROM assigned_status
GROUP BY 1,2;

/*
-- inactive activity_calc --
cancelled
care giving leave
deactivate
deactivated
disability leave
general leave
medical leave
parental leave
suspended

-- active activity_calc types --
technical activation
technical reactivation
winback
initial enrollment*/

-- PSFC loan application membership question: How many members have been members in good standing for 6 months or longer? 
-- ao 10/1/2024 calc the day diff between any "active" status members and lead_date (if the activity_calc of the previous type was also active, then drill down one )
-- alternative #2: take 2 snapshots of their member type and status
-- alternative #3: group 
WITH activity_array AS (
SELECT mt_email, JSON_ARRAYAGG(activity_calc) activity_calc_array
FROM stack_job2
WHERE DATE(lead_date) > DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
GROUP BY 1)
SELECT *
FROM activity_array
-- can't contain cancelled, general leave, deactivate, medical leave, parental leave, suspended, care giving leave
WHERE NOT JSON_CONTAINS(activity_calc_array,'"cancelled"')
AND NOT JSON_CONTAINS(activity_calc_array,'"general leave"') 
AND NOT JSON_CONTAINS(activity_calc_array,'"deactivate"')
AND NOT JSON_CONTAINS(activity_calc_array,'"medical leave"')
AND NOT JSON_CONTAINS(activity_calc_array,'"suspended"')
AND NOT JSON_CONTAINS(activity_calc_array,'"parental leave"')
AND NOT JSON_CONTAINS(activity_calc_array,'"care giving leave"')
AND NOT JSON_CONTAINS(activity_calc_array,'"deactivated"') 
AND mt_email NOT LIKE '%test%';

-- PSFC loan question cont. What is the average rate of enrollment of new members over the last six months?  members /month Comment end  
-- rolling 6 month
-- will require expanding the stack_job2 table on calendar dates
/*AVG(val) OVER (PARTITION BY subject ORDER BY time
                        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) */
WITH agg_counts AS (
-- aggregate on start_dt, which is a timestamp originally	
SELECT DATE(start_dt) start_dt, COUNT(DISTINCT sj.mt_email) new_signups
FROM stack_job2 sj 
WHERE TRIM(LOWER(sj.activity_calc)) = 'initial enrollment'
GROUP BY 1 
order by 1 desc), 
raw_data AS (
SELECT cal.calendar_date, SUM(new_signups) new_signups
FROM calendar cal 
LEFT JOIN agg_counts ON cal.calendar_date = agg_counts.start_dt 
WHERE cal.calendar_date BETWEEN date_sub(current_date(), interval 180 day) AND DATE('2024-12-01') -- as of the last 180 days
GROUP BY 1
ORDER BY 1 desc), 
running_signups AS (
SELECT calendar_date, sum(new_signups) OVER(ORDER BY calendar_date ASC ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) rolling_signups
FROM raw_data 
ORDER BY 1 DESC 
LIMIT 100)
SELECT AVG(rolling_signups) 
FROM running_signups;

-- INVESTIGATE HOW BEST TO IDENTIFY AND JOIN TRIAL DATA (ORIGINALLY TAKEN FROM 'stored_procedure_trial_shopping.sql')
WITH prep AS (
SELECT 
CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) = 'null' THEN NULL 
ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) as date) 
END AS trial_dt, 
email, type_clean, start_dt, ingest_date, trial_expiration
from consolidated_mem_type), -- last working code pointed to table mem_type_0101
-- "conversions" CTE composed only of records where member is no longer in-trial, and where there WAS an originating trial for the email account; ie successful conversions
conversions AS (
SELECT  trial_dt, datediff(cast(start_dt as date), prep.trial_dt) as date_difference,
email, type_clean
from prep
where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
-- qualifier where there was a trial on record for the email
AND trial_dt is not null)

/* trial data wish list:
i. trial length from that which they converted
ii. trial history (whether they initiated one prior to their last)
iii. gaps btwn their trial iterations and/or trial expiration and final signup
*/
-- TRIAL ANALYSIS: LOOK AT MULTIPLE TRIAL STARTS (EITHER ROLLOVERS OR REPEATS)
WITH multiple_trials AS (
select email, count(*) trials_cnt
from consolidated_mem_type
where type_clean like '%trial%'
AND start_dt > DATE('2019-06-01')
group by 1
HAVING count(*) > 1
order by 2 desc),
raw_data AS
(SELECT email, start_dt, type_raw, type_clean, lead_date, cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) last_trial_start
FROM consolidated_mem_type
where type_clean like '%trial%' 
AND email IN (SELECT email FROM multiple_trials GROUP BY 1)
ORDER BY 1,2),
-- consolidate type clean values
consolidated_arrays AS (
SELECT email, JSON_ARRAYAGG(type_clean) type_clean_array, max(start_dt) max_start_date
FROM raw_data 
GROUP BY 1)
SELECT YEAR(max_start_date) year, count(distinct email) 
FROM consolidated_arrays
GROUP BY 1
ORDER BY 1;

-- RSTUDIO code which likely double counts multi-trials, which isn't the end of the world; the 
select 
date_format(start_dt, '%Y-%m') as month, 
type_clean,  
CASE WHEN date(start_dt) <> cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) THEN 'rollover' ELSE 'initial' END AS trial_seq, 
count(distinct email) as count
from consolidated_mem_type
where type_clean like '%trial%'
AND start_dt > DATE('2019-06-01')
group by 1,2,3
order by 1 desc,2;


-- the above trial analysis query should match with the below:
WITH raw AS (
select 
YEAR(start_dt) year, 
type_clean,  
-- this logic is questionable: make sure I'm not double counting; and be sure that I'm counting from the last trial, and not the first (which coincides with the logic from the query above)
CASE WHEN date(start_dt) <> cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) THEN 'rollover' ELSE 'initial' END AS trial_seq, 
count(distinct email) as count
from consolidated_mem_type
where type_clean like '%trial%'
AND start_dt > DATE('2019-06-01')
group by 1,2,3
order by 1 desc,2)
SELECT year, sum(count)
FROM raw 
WHERE trial_seq = 'rollover' 
GROUP BY 1
ORDER BY 1 DESC;

-- PROD VERSION OF TRIAL TIMESERIES
WITH setup AS (
SELECT email, start_dt, type_raw, type_clean, lead_date, cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) last_trial_start, 
ROW_NUMBER() OVER(PARTITION BY email ORDER BY start_dt) row_num
FROM consolidated_mem_type
where type_clean like '%trial%' 
ORDER BY email, start_dt asc)
SELECT date_format(start_dt,'%Y-%m') as month, type_clean,
CASE WHEN row_num = 2 THEN 'rollover' ELSE 'initial' END AS trial_seq, count(distinct email) cnt
FROM setup
GROUP BY 1,2,3 
ORDER BY 1;

-- proposal table: email, latest trial metadata, # of total trials, count of trial by trial length, conversion date, date diff btwn last two trials, date diff btwn last trial and conversion date

CREATE TABLE trial_meta_all AS
WITH all_trials AS (SELECT *
FROM consolidated_mem_type
WHERE type LIKE '%trial%'
ORDER BY email, start_dt), 
-- get counts by trial rounds
counts AS (
SELECT email, type_clean, count(*) trial_len_cnt_email, max(start_dt) AS last_trial_start
FROM all_trials 
GROUP BY 1,2),
-- one row per email and trial type (requires a self-JOIN)
six_month AS (
SELECT email, type_clean, trial_len_cnt_email six_mo_cnt, last_trial_start
FROM counts
WHERE type_clean = '6 mo trial'),
two_month AS (
SELECT email, type_clean, trial_len_cnt_email two_mo_cnt, last_trial_start
FROM counts
WHERE type_clean = '2 mo trial'),
final_tbl AS (
-- mysql doesn't offer OUTER joins, so have to build this janky one
SELECT six_month.email, 
CASE WHEN six_mo_cnt IS NULL THEN 0 ELSE six_mo_cnt END AS six_mo_cnt, 
CASE WHEN two_mo_cnt IS NULL THEN 0 ELSE two_mo_cnt END AS two_mo_cnt, six_month.last_trial_start last_trial_start_6, two_month.last_trial_start last_trial_start_2, ABS(DATEDIFF(six_month.last_trial_start, two_month.last_trial_start)) date_diff
FROM six_month
LEFT JOIN two_month ON six_month.email = two_month.email
UNION ALL
SELECT two_month.email, 
CASE WHEN six_mo_cnt IS NULL THEN 0 ELSE six_mo_cnt END AS six_mo_cnt, 
CASE WHEN two_mo_cnt IS NULL THEN 0 ELSE two_mo_cnt END AS two_mo_cnt, six_month.last_trial_start last_trial_start_6, two_month.last_trial_start last_trial_start_2, ABS(DATEDIFF(six_month.last_trial_start, two_month.last_trial_start)) date_diff
FROM six_month
RIGHT JOIN two_month ON six_month.email = two_month.email
GROUP BY 1,2,3,4,5 
ORDER BY six_mo_cnt ASC)
SELECT *, 
-- build latest trial type field
CASE 
WHEN last_trial_start_6 > last_trial_start_2 THEN '6 month'
WHEN last_trial_start_2 > last_trial_start_6 THEN '2 month'
WHEN last_trial_start_2 IS NULL AND last_trial_start_6 IS NOT NULL THEN '6 month' 
WHEN last_trial_start_6 IS NULL AND last_trial_start_2 IS NOT NULL THEN '2 month'
END AS last_trial_type,
-- record the last trial start date
CASE 
WHEN last_trial_start_6 > last_trial_start_2 THEN DATE_ADD(last_trial_start_6, INTERVAL 6 MONTH)
WHEN last_trial_start_2 > last_trial_start_6 THEN DATE_ADD(last_trial_start_2, INTERVAL 2 MONTH)
WHEN last_trial_start_2 IS NULL AND last_trial_start_6 IS NOT NULL THEN DATE_ADD(last_trial_start_6, INTERVAL 6 MONTH) 
WHEN last_trial_start_6 IS NULL AND last_trial_start_2 IS NOT NULL THEN DATE_ADD(last_trial_start_2, INTERVAL 2 MONTH)
END AS last_expiration_date
FROM final_tbl;

-- can I create a JSON object that stores the freq count by trial type?

-- prod table: current_month, leave_balance, active_balance, suspended_balance
INSERT INTO acct_flows (current_month, leave_balance, active_balance, suspended_balance)
VALUES (value1, value2, value3, ...); 


-- weekly snapshot; calendar table
-- this requires adjusting the system variable: https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_cte_max_recursion_depth; https://www.percona.com/blog/introduction-to-mysql-8-0-recursive-common-table-expression-part-2/

SET cte_max_recursion_depth=2000; -- can be executed within dBeaver
DROP TABLE IF EXISTS calendar;
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


-- check date range of shop_log: this range dictates the range to be applied to trialShoppingHabits
SELECT MIN(Activity_Date) min_dt, MAX(Activity_Date) max_dt
from shop_log;

-- ************** THE BELOW REQUIRES THE SHOP LOG TO BE IMPORTED INTO MYSQL DB FIRST************
-- ingest member shopping activity via ingestMemberShopping.ipynb
-- join shopping data ("shop_log") to mem_type data; later: freq of shopping for trial members, while in-trial, and relate to type conversions
-- all recorded/historical shopping is categorized (in/pre/post trial) for each iteration of their trial; ie I don't attempt to segregate shopping activity and assign it to a trial (it's always all considered)
-- trialShoppingHabits = transactional table of trial member shopping activity (exclusively): one record per shopping trip that classifies it as in/pre/out of trial, whether a conversion occurred, total trips while in-trial
-- if this version doesn't work, consult trialShoppingHabitsDiagnostics.sql
DROP TABLE IF EXISTS trialShoppingHabits;

/* 
DECLARE min_dt DATE;
DECLARE max_dt DATE;
*/
set @min_dt := (SELECT MIN(Activity_Date) from shop_log);
set @max_dt := (SELECT MAX(Activity_Date) from shop_log);

CREATE TABLE trialShoppingHabits AS
WITH prep AS (
SELECT CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) = 'null' THEN NULL 
ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) as date) END AS trial_dt, 
email, type_clean, start_dt, ingest_date, trial_expiration
from mem_type_0101), -- last working code pointed to table mem_type_0101
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
-- sl.Activity_Date shopping_date, -- (have to exclude or I don't get proper aggregation below)
CASE 
WHEN sl.Activity_Date BETWEEN mt.start_dt AND mt.trial_expiration THEN 'in trial' 
WHEN sl.Activity_Date < mt.start_dt THEN 'pre trial' 
WHEN sl.Activity_Date > mt.start_dt THEN 'post trial' 
ELSE  'n/a'
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
AND mt.start_dt BETWEEN @min_dt AND @max_dt
AND trial_expiration is not null
AND mt.type_clean like '%trial%'
GROUP BY 1,2,3,4,5,6)
select * 
from stats 
order by email;

-- time series: plot avg shopping trips while in trial by trial start month [trial_start_dt, trips, conversion flag, relative_trial_period, email]
-- the below provides: week of trial start date, avg trips while in-trial, total trial members, and num of trial members over avg
WITH overall AS (
select STR_TO_DATE(CONCAT(yearweek(trial_start_dt),' Sunday'), '%X%V %W') FirstDayOfWeek, 
avg(trips) avg_trips_while_trial, 
count(distinct email) trial_members
from trialShoppingHabits tsh
where relative_trial_period IN ('in trial','n/a')
-- AND mo_type is null (NOTE unnecessary to limit the averages to non-converters)
group by 1), 
above_avg AS (
select STR_TO_DATE(CONCAT(yearweek(trial_start_dt),' Sunday'), '%X%V %W') FirstDayOfWeek, count(distinct email) above_avg_trial_members
from trialShoppingHabits tsh
WHERE tsh.relative_trial_period IN ('in trial','n/a')
AND tsh.mo_type is null
-- AND tsh.trial_start_dt between date('2023-12-03') AND date('2023-12-09')
AND tsh.trips > (SELECT avg(tsh_m.trips) 
from trialShoppingHabits tsh_m 
WHERE STR_TO_DATE(CONCAT(yearweek(tsh_m.trial_start_dt),' Sunday'), '%X%V %W') = STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W') 
AND tsh_m.relative_trial_period IN ('in trial','n/a')) 
GROUP BY 1) 
SELECT overall.*, above_avg.above_avg_trial_members
FROM overall 
LEFT JOIN above_avg ON overall.FirstDayOfWeek = above_avg.FirstDayOfWeek;


-- surface the actual trial members likely to convert: trial members that have shopped since trial has begun and still in-trial
-- partition/craft cohorts from trial start week
-- member_directory is a table located in the db that is imported via its own pipeline: 'ingestMembershipContactInfo.ipynb'
-- latest code should found in trial_shopping.sql
WITH trips_data AS (
SELECT STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W') FirstDayOfWeek, tsh.email, tsh.trips, tsh.trial_expiration, (SELECT avg(tsh_m.trips) ave
from trialShoppingHabits2 tsh_m
WHERE tsh_m.relative_trial_period IN ('in trial','n/a')
AND tsh_m.final_trial_flag = 'relevant trial'
AND STR_TO_DATE(CONCAT(yearweek(tsh_m.trial_start_dt),' Sunday'), '%X%V %W') = STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W')) overall
FROM trialShoppingHabits2 tsh 
where tsh.relative_trial_period = 'in trial' 
AND tsh.final_trial_flag = 'relevant trial'
AND tsh.mo_type IS NULL) 
SELECT td.*, md.*
FROM trips_data td 
LEFT JOIN member_directorys md ON td.email = md.email 
-- select for only those trial members that have # trips greater than avg for their cohort
WHERE trips > overall 
AND trips > 1 
AND trial_expiration BETWEEN date_sub(curdate(), interval 35 day) AND date_add(curdate(), interval 10 day)
order by firstDayOfWeek asc;


-- QA: 12/31/2023 only has an average of 1, which doesn't make sense
select STR_TO_DATE(CONCAT(yearweek(tsh_m.trial_start_dt),' Sunday'), '%X%V %W') FirstDayOfWeek, AVG(trips) avg_trips
from trialShoppingHabits2 tsh_m
where trial_start_dt between date('2023-12-31') AND date('2023-01-07');
AND tsh_m.relative_trial_period IN ('in trial','n/a')
AND tsh_m.final_trial_flag = 'relevant trial'
group by 1 
order by 1 asc;


/******** TRIAL JOURNEYS, SUCCESSFUL TRIAL JOURNEYS, TRIAL MEMBER TENDENCIES, ETC***********
- source table: consolidated_mem_type 
field 'type_clean': values "6 mo trial" and "trial"
field 'latest_trial2': comes from the 'append_last_trial' function of civiActivityReport.ipynb; this is a convenience field... field records the latest trial a/o start date, this json field contains keys trial start date and trial type
- source table: consolidated_mem_status

investigate cases:
i. multiple trials
ii. a 2-month trial converting to a 6 mo
*/

-- QUERY FOR SANKEY DIAGRAM OF TRIAL JOURNEY where each branch of the diagram is a mem_type step
-- ea "step"/round returns pieces of metadata of the mem_type round
-- result set expectation: leftmost columnset
-- first step after the trial is return ea distinct mem_type along with the start_dt, then return ea pair of mem_type and mem_status along w/mem_status start_dt; this will eventually allow me to understand the typical journey, tenure and success/failure outcomes
-- first select for members that ever underwent a trial
WITH trial_email AS (
SELECT email 
from consolidated_mem_type mt	
WHERE 1=1
AND lower(type_clean) like '%trial%'
group by 1),
-- select ALL activity for members that every registered a trial
grouping2 AS (
SELECT mt.email mt_email, mt.start_dt mt_start_dt, mt.type_clean mt_type_clean
from consolidated_mem_type mt 
WHERE mt.email IN (SELECT email from trial_email)
group by 1,2,3
order by 1,3 asc), 
-- assign a row_num to ea mem_type in order to identify first, second, thirs, etc rounds of
iteration AS (
SELECT *, row_number() over(PARTITION BY mt_email order by mt_start_dt asc) row_num
FROM grouping2),
-- in order to join ea iteration, build a join with hard-coded round numbers going 4-deep at first
consolidate AS (
SELECT * FROM 
(SELECT * from iteration where row_num = 1) row_one
LEFT JOIN (SELECT mt_email mt_email2, mt_start_dt mt_start_dt2, mt_type_clean mt_type_clean2 from iteration where row_num = 2) row_two
ON row_one.mt_email = row_two.mt_email2
LEFT JOIN (SELECT mt_email mt_email3, mt_start_dt mt_start_dt3, mt_type_clean mt_type_clean3 from iteration where row_num = 3) row_three 
ON row_one.mt_email = row_three.mt_email3) 
-- aggregate by journey; this will reveal some non-sequitors, but those are in minority
select mt_type_clean, mt_type_clean2, mt_type_clean3, count(*)
from consolidate 
group by 1,2,3 
order by 1,2,4 desc;

/******** END TRIAL JOURNEYS ***********/

/*Number of members on Leave, # of winbacks, time-to-winback */
SELECT calendar_date, SUM(CASE WHEN activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation') THEN 1 ELSE 0 END) AS active_cnts, SUM(CASE WHEN activity_calc IN ('deactivated','parental leave','medical leave','care giving leave') THEN 1 ELSE 0 END) AS inactivte_cnts, SUM(CASE WHEN activity_calc IN ('winback','technical activation','technical re-activation') THEN 1 ELSE 0 END) AS winbacks
FROM stack_jobII sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
GROUP BY 1 
ORDER BY 1 desc;

-- tracking winbacks: applying a second validation on winbacks by leveraging a lookback via window funct
SELECT *,
-- "leaves_to_date" won't necessarily coincide w/ activity_calc = "winback", bc that logic also includes reactivations
SUM(CASE WHEN sj.activity_calc LIKE '%leave%' THEN 1 ELSE 0 END) OVER(PARTITION BY sj.mt_email ORDER BY sj.start_dt ASC) AS leaves_to_date
FROM stack_jobII sj 
ORDER BY mt_email, start_dt asc
LIMIT 40;


-- best working model; but counts between 'winbacks_window' & 'winbacks_orig' are unequal needs to be investigated
WITH sjII_new AS (
SELECT *,
-- "leaves_to_date" won't necessarily coincide w/ activity_calc = "winback", bc that logic also includes reactivations
SUM(CASE WHEN sj.activity_calc LIKE '%leave%' THEN 1 ELSE 0 END) OVER(PARTITION BY sj.mt_email ORDER BY sj.start_dt ASC) AS leaves_to_date
FROM stack_jobII sj)
SELECT calendar_date, 
SUM(CASE WHEN activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation') THEN 1 ELSE 0 END) AS active_cnts, 
SUM(CASE WHEN activity_calc IN ('deactivated','parental leave','medical leave','care giving leave') THEN 1 ELSE 0 END) AS inactivte_cnts, 
SUM(CASE WHEN leaves_to_date > 0 AND activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation') THEN 1 ELSE 0 END) AS winbacks_window,
SUM(CASE WHEN activity_calc IN ('winback','technical activation','technical re-activation') THEN 1 ELSE 0 END) AS winbacks_orig
FROM sjII_new sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
GROUP BY 1 
order by 1 asc;