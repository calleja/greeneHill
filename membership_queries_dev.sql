-- word wrap shortcut: shift+cntrl+alt+w
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
from mem_type_0927
where email IN ('mateotaussig@gmail.com','gabrielagamache@gmail.com') 
order by email;
-- one-off queries to investigate particular persons
select * 
from mem_status_0927
where email IN ('mateotaussig@gmail.com','gabrielagamache@gmail.com','br.weinkle@gmail.com') 
order by email , start_dt asc;
-- curated tables
SELECT * 
FROM stack_jobII
where mt_email IN('aubreygrowsfood@gmail.com');


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
from mem_type_0101
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

-- prod version of ipynb file
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
-- this resultset looks off, will need to QA
SELECT calendar_date, SUM(CASE WHEN activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation') THEN 1 ELSE 0 END) AS active_cnts, SUM(CASE WHEN activity_calc IN ('deactivated','parental leave','medical leave','care giving leave') THEN 1 ELSE 0 END) AS inactivte_cnts
FROM stack_jobII sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
GROUP BY 1 
ORDER BY 1 desc;

-- review records of those accounts that are not considered 'active' as of a certain date to ensure accuracy
-- I need to review three use cases: 1) those accounts that are found in the db tables AND the CIVI dataset, yet are NOT flagged as active in my logic; 2) accounts appearing in CIVI that are missing entirely from my db; 3) accounts flagged as active in the db but missing from CIVI
-- the below accounts are NOT showing as active as of 12/1 when there is a case that they should be... this highlights the problem of cases where total_rows = 1 
-- the query below returns all accounts that SHOULD NOT be considered active ao 12/1 according to the logic to-date
SELECT * 
FROM stack_jobII sj
WHERE mt_email NOT IN (
SELECT sj.mt_email 
FROM stack_jobII sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
WHERE activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation')
AND cal.calendar_date = DATE('2023-11-01') 
group by 1) 
order by mt_email, start_dt asc 
limit 500;

-- misstated cases: the emails below do not carry an "active"-type activity_calc value, but are shown to be active per CIVI (active member report)
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

-- missing cases: these emails are 
-- "active_roster" is the CIVI table (imported into db by way of 'ingestMemberShopping.ipynb'
WITH total_set AS (
SELECT sj.mt_email, sj.start_dt, sj.lead_date, sj.type_raw, sj.activity, sj.mem_type, sj.activity_calc, sj.row_num, sj.total_rows, ar.Email current_email  
FROM stack_jobII sj
RIGHT JOIN active_roster ar ON trim(lower(sj.mt_email)) = trim(lower(ar.Email))
group by 1,2,3,4,5,6,7,8,9,10)
SELECT *
from total_set
-- this ensures that I'm considering the most recent cases
WHERE row_num = total_rows
-- group by 1 
order by mt_email, lead_date asc;

-- return the whole universe of active accounts (per the db logic AND CIVI)
-- "active_roster" is the CIVI table (imported into db by way of 'ingestMemberShopping.ipynb')
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

-- investigate cases of accounts not indicated as ACTIVE in stack_jobII
SELECT *
FROM stack_jobII sj
WHERE mt_email IN ('oba1620@gmail.com','kevin@keogh.dev','aiisshiki@gmail.com','jmullen006@gmail.com',
'akiko.ichikawa@gmail.com','cj@cirr.us','chrisgo22@gmail.com','varybl@gmail.com','ethan.rts@gmail.com') 
order by mt_email, lead_date asc;


SELECT *
FROM stack_jobII sj
WHERE mt_email IN ('oba1620@gmail.com','kevin@keogh.dev','aiisshiki@gmail.com','jmullen006@gmail.com',
'akiko.ichikawa@gmail.com','cj@cirr.us','chrisgo22@gmail.com','varybl@gmail.com','ethan.rts@gmail.com') 
order by mt_email, lead_date asc;



-- investigate some emails from the above and determine if they were rightfully excluded from 'active' count as 12-1-2023
SELECT *
from mem_type_1204
WHERE email IN ('aubreygrowsfood@gmail.com','cbm389@nyu.edu','luvness06@yahoo.com') 
order by email;

SELECT *
from mem_status_1204
WHERE email IN ('aubreygrowsfood@gmail.com','cbm389@nyu.edu','luvness06@yahoo.com') 
order by email;


-- QA: exposes how "initial enrollment" is extremely inflated; fix this in the UPDATE statement above
SELECT activity_calc, count(*) cnt
FROM stack_job sj
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
from mem_type_0101),
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
WITH trips_data AS (
SELECT STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W') FirstDayOfWeek, tsh.email, tsh.trips, tsh.trial_expiration, (SELECT avg(tsh_m.trips) ave
from trialShoppingHabits tsh_m
WHERE tsh_m.relative_trial_period IN ('in trial','n/a')
AND tsh_m.final_trial_flag = 'relevant trial'
AND STR_TO_DATE(CONCAT(yearweek(tsh_m.trial_start_dt),' Sunday'), '%X%V %W') = STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W')) overall
FROM trialShoppingHabits tsh 
where tsh.relative_trial_period = 'in trial' 
AND tsh.final_trial_flag = 'relevant trial'
AND tsh.mo_type IS NULL) 
SELECT td.*, md.*
FROM trips_data td 
LEFT JOIN member_directory md ON td.email = md.email 
-- select for only those trial members that have # trips greater than avg for their cohort
WHERE trips > overall 
AND trips > 1 
AND trial_expiration < date_add(curdate(), interval 14 day) 
order by firstDayOfWeek asc;

-- potentially missed opportunities? former trial members that did not convert but either shopped a lot during their trial or have shopped since their trial expired (but they never converted)

/************* Find all member activity, including full membership journey, including those having a historical trial start
TODO: trial expirations don't populate properly here
**************/
DROP TABLE IF EXISTS mem_journey;
CREATE TABLE mem_journey AS
WITH mt_ms AS (
SELECT mt.email mt_email, mt.start_dt mt_start_dt, mt.lead_date mt_lead_date, mt.type_clean mt_type_clean, mt.type_raw mt_type_raw, ms.start_dt ms_start_dt, ms.lead_date ms_lead_date, ms.type_clean ms_type_clean, ms.type_raw ms_type_raw  
from mem_type_0927 mt
LEFT JOIN mem_status_0927 ms ON mt.email = ms.email
-- ensure that status records only populate on same line as the relevant type record
AND ms.start_dt between mt.start_dt AND mt.lead_date 
AND ms.type_clean not like '%trial%'
WHERE mt.type_clean IN ('lettuce', 'carrot', 'household', 'avocado','apple')), 
-- stack the data: to ensure that the initial membership (trial or other) is allocated its own row, this way I can edit the in-force date range, as this will be impacted by the first record on mem_status
-- grouping here will remove the dupes, and can be used in place of a WHERE clause to ensure single selection
-- stacking: select the mem_type portion of each row and stack on top of the mem_status portion of each row; grouping here is essential for the mem_type portion, in order to drop duplicates
stacked AS (
SELECT mt_email, mt_start_dt start_dt, mt_lead_date lead_date, mt_type_clean, mt_type_clean type_clean, mt_type_raw type_raw
FROM mt_ms
-- WHERE mt_type_clean IN ('lettuce', 'carrot', 'household', 'avocado','apple')
-- where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
GROUP BY 1,2,3,4,5,6	
UNION ALL
SELECT mt_email, ms_start_dt start_dt, ms_lead_date lead_date, mt_type_clean, ms_type_clean type_clean, ms_type_raw type_raw
FROM mt_ms
-- ms_type_clean values related to trial activity are '%trial%', 'cancelled', 'deactivated'
-- WHERE ms_type_clean not like '%trial%')
GROUP BY 1,2,3,4,5,6), 
lead_step AS (
select stacked.*, LEAD(start_dt) OVER(PARTITION BY mt_email order by start_dt asc) lead_dt_2
from stacked 
WHERE type_clean IS NOT NULL) -- rows where the member-owner never had a status change
SELECT lead_step.*, CASE WHEN lead_dt_2 is not null THEN date_sub(lead_dt_2, interval 1 day) ELSE lead_date END AS end_dt
FROM lead_step 
order by 1,2 asc;

-- check the unique values of type_clean
SELECT type_clean 
from mem_journey
group by 1;

/*
GROUPINGS
active: lettuce, carrot, apple, winback, household, technical reactivation, avocado, technical re-activation, technical	activation
deactivated: cancelled, deactivated, general leave, parental leave, medical	leave
*/

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