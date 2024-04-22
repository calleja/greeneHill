-- #1
-- join mem_status to mem_type, which is the only way to associate prevailing membership type (mem_typeXXX.type_clean) to mem_status activity; then stack so that each row is a membership event period for ea email (records having null values for mt_type_clean are mem_type original records; it's mem_status that will have non-null values)
-- UPDATING STATEMENT to replace the "lead_date" for "TYPE" rows
-- the lead and start_dt fields are exclusive to either type or status changes, this is bc I related all status changes to the prevailing type and I need the type range in order to accomplish that; this means I HAVE TO recompute the lead/start date post compilation

DROP PROCEDURE IF EXISTS stackjob_creations;

DELIMITER //
        
CREATE PROCEDURE stackjob_creations()

BEGIN 

DROP TABLE IF EXISTS stack_job2;

-- TODO: mt_cancel_flag is causing problems as it is later referenced and is included in a group by, and interpreted as a unique field. EXAMPLE: WHERE mt_email = '1elsamaki@gmail.com'

CREATE TABLE stack_job2 AS
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
AND lower(mt_type_clean) NOT LIKE '%trial'), 
-- penultimo introduced in order to make space for the window functions below 'row_num' and 'total_rows'
penultimo AS (
select stacked.*, CASE WHEN activity = mem_type THEN 'initial enrollment' ELSE activity END AS activity_calc, 
-- experimental text 
TRIM(regexp_substr(type_raw,'(?<=Status:).*$')) AS text_status_indicator
from stacked 
WHERE 1=1 
-- AND mt_email IN ('fenailletom@gmail.com','405sarah@gmail.com')
group by 1,2,3,4,5,6,7 -- must group by in order to account for duplicated "type" rows (LHS)
order by 1,2)
select *, ROW_NUMBER() OVER(PARTITION BY mt_email ORDER BY start_dt asc) AS row_num, 
COUNT(*) OVER(PARTITION BY mt_email) total_rows 
from penultimo;


-- #1b: UPDATE stack_job table (aka overwrite)
-- PURPOSE: replace the original lead_date with (1-start_dt) of the subsequent row (partitioned by email) in order to enhance accuracy of date ranges
-- prelim table is a helper table to build the new lead date
-- might need to compute a LEAD() first then run a WHERE clause in a second query to exclude accounts that didn't log a status change
-- what happens within mysql during an UPDATE statement: https://itnext.io/what-happens-during-a-mysql-update-statement-7aafbb1ecc01
WITH prelim AS (
SELECT stack_job2.*, LEAD(date_sub(start_dt, interval 1 day)) OVER(PARTITION BY mt_email ORDER BY start_dt) date_lead2
FROM stack_job2
-- WHERE mt_email IN ('fenailletom@gmail.com','405sarah@gmail.com')
order by 1,2) 
UPDATE stack_job2 AS sj2 
-- first apply the inner join, then set values of one column (date_lead) to the other col (date_lead2) ON THE SAME ROW; the alternative would be to write a CASE statement, but then I'd end up with a row with which to deal
-- lead the start_dt of the proceeding status, if one exists and attempt to replace the lead_date field for the type row
INNER JOIN (SELECT * FROM prelim where activity_calc = 'initial enrollment') x 
ON sj2.mt_email = x.mt_email AND sj2.activity = x.activity -- ensure that we only update the first 'type' row
SET sj2.lead_date = x.date_lead2 
WHERE sj2.activity_calc = 'initial enrollment' AND x.date_lead2 IS NOT NULL;

-- UPDATE lead_date on the last record for each email to curren date; lead_date in the case of the final row by email is hard coded to pipeline run date on the .ipynb file, and can be stale... but ultimately, that was the last run date, and most precise

/* values text_status_indicator; these will need to be recorded (overwite) in the activity_calc field
Cancelled
Deactivated
General Leave
Expired
*/

-- UPDATE "activity_calc" in cases where there is only one record for the member (total_rows = 1)
WITH prelim AS (
SELECT stack_job2.*, 
-- regex to exclude all text prior to "_"
CASE WHEN TRIM(LOWER(regexp_substr(text_status_indicator,'^.*(?=(_))'))) IN ('cancelled', 'deactivated', 'general leave', 'expired') 
THEN 'deactivate' 
ELSE activity_calc END AS activity_calc_alt
FROM stack_job2
-- WHERE mt_email IN ('fenailletom@gmail.com','405sarah@gmail.com')
order by 1,2) 
UPDATE stack_job2 AS sj2 
-- first apply the inner join, then set values of one column (date_lead) to the other col (date_lead2) ON THE SAME ROW; the alternative would be to write a CASE statement, but then I'd end up with a row with which to deal
-- lead the start_dt of the proceeding status, if one exists and attempt to replace the lead_date field for the type row
INNER JOIN (SELECT * FROM prelim where activity_calc = 'initial enrollment') x 
ON sj2.mt_email = x.mt_email AND sj2.activity = x.activity -- ensure that we only update the first 'type' row
SET sj2.activity_calc = x.activity_calc_alt 
WHERE sj2.activity_calc = 'initial enrollment' 
-- apply update on cases where there is a single activity record 
AND sj2.row_num = 1 
AND sj2.total_rows = 1;

END //
DELIMITER ;