/*
identified issue where converted member-owners are missing their trial flags because the civiActivityReport.ipynb didn't catch them because the trial started BEFORE the start date of the CIVI extract

I'll need to identify/isolate trials, select the latest one, then store that in the JSON field of any subsequent 'type' records

 Fields in consolidated_mem_type: type, type_raw,start_dt, datetimerange, type_clean, email, trial_expiration, latest_trial2, ingest_date,lead_date

 example of creating a JSON object in mysql:
 SELECT JSON_OBJECT('id', id, 'name', name, 'age', age) AS json_result
FROM users;
*/

-- identify latest trials for all trial records by email
SELECT outer_t.email, outer_t.start_dt, outer_t.type_clean, outer_t.trial_expiration
, JSON_OBJECT('start_dt',outer_t.start_dt,'type_clean',outer_t.type_clean) AS latest_trial_new 
from consolidated_mem_type outer_t
WHERE lower(outer_t.type_clean) LIKE '%trial%'
AND outer_t.trial_expiration = 
(SELECT max(inner_t.trial_expiration) FROM consolidated_mem_type inner_t
WHERE lower(inner_t.type_clean) LIKE '%trial%' 
AND inner_t.email = outer_t.email)
-- GROUP BY 1,2,3 
limit 20;


-- CREATE STORED PROCEDURE FROM THE BELOW
DELIMITER //
CREATE PROCEDURE update_latest_trial()
BEGIN
-- First, create a temporary table with the latest trial info 
CREATE TEMPORARY TABLE temp_trial_list AS
SELECT outer_t.email, outer_t.start_dt, outer_t.type_clean, outer_t.trial_expiration,
       JSON_OBJECT('start_dt', outer_t.start_dt, 'type_clean', outer_t.type_clean) AS latest_trial_new
FROM consolidated_mem_type outer_t
WHERE LOWER(outer_t.type_clean) LIKE '%trial%'
AND outer_t.trial_expiration = (
    SELECT MAX(inner_t.trial_expiration)
    FROM consolidated_mem_type inner_t
    WHERE LOWER(inner_t.type_clean) LIKE '%trial%' 
    AND inner_t.email = outer_t.email
    GROUP BY inner_t.email);

-- Then, perform the update using the temporary table: but only update the latest_trial2 field for records that are not trials
UPDATE consolidated_mem_type AS mt_prod
INNER JOIN temp_trial_list tl ON mt_prod.email = tl.email
SET mt_prod.latest_trial2 = tl.latest_trial_new
WHERE mt_prod.type_clean NOT LIKE '%trial%';

-- Clean up
DROP TEMPORARY TABLE IF EXISTS temp_trial_list;
END //
DELIMITER ;

-- END STORED PROCEDURE

-- quality check
select *
from consolidated_mem_type
where email = 'tanya.marquardt@gmail.com';

-- here is an overwrite statement for reference (to delete asap)
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


-- ensure the code above can be queried by the code below
-- some starter code from the trial stored procedure
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
AND trial_dt is not null), 
-- "stats" ONLY contains members on trial at time of shopping activity (where prep.type_clean = "trial")
-- "stats" CTE is aggregation of trips/shopping visits by: email, trial_start_dt, trial_expiration, ingest_date
-- "stats" will only keep trial-related activity; 'conversions_trial_dt' field will denote whether the trial member converted
-- track the latest trial via "final_trial_flag" = 'relevant trial'
-- for each trial record (regardless if latest), categorize shopping visits as either pre or post trial, and count no. of visits
stats AS (
SELECT cast(mt.start_dt as date) AS trial_start_dt, mt.email, cast(trial_expiration as date) trial_expiration, mt.ingest_date, mt.type_clean orig_type_clean,
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
ELSE 'trial iteration expected' -- apriori evidence of an upcoming subsequent trial
END AS final_trial_flag,
-- propogate the membership type, should they convert (derived from 'conversions' table); for QA/conditional purposes later (assume that if a non-NULL is present, that will be returned)
-- attempt to propogate each user's record (in resultset) with the last member type to denote conversion
max(conversions.type_clean) mo_type, 
count(distinct sl.Activity_Date) trips
from prep mt -- prep = entire universe of membership
LEFT JOIN shop_log sl ON trim(mt.email) = trim(sl.Target_Email)
LEFT JOIN conversions ON trim(mt.email) = trim(conversions.email) AND cast(mt.start_dt as date) = conversions.trial_dt
WHERE 1=1
AND mt.start_dt BETWEEN @min_dt AND @max_dt
AND mt.trial_expiration is not null
AND mt.type_clean like '%trial%'
GROUP BY 1,2,3,4,5,6,7)
select * 
from stats 
order by email;



select email trial_email, type_clean trial_type_clean, trial_expiration, latest_trial2 
from consolidated_mem_type
WHERE type_clean = JSON_EXTRACT(latest_trial2, '$.type_clean')
AND type_clean LIKE '%trial%'
order by email;


SELECT 
CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) = 'null' THEN NULL 
ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) as date) 
END AS trial_dt, 
email, type_clean, start_dt, ingest_date, trial_expiration
from consolidated_mem_type