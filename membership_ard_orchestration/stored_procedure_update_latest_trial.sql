/*
This stored procedure is an updater to the consdolidated_mem_type table. The "update_latest_trial" stored procedure solves for the use case where converted member-owners were missing their trial flags in cases where trial events/records were not included on the same CIVI extract. That field would be NULL/empty in those cases. Furthermore: civiActivityReport.ipynb writes the "latest_trial2" field - and it's scope is limited to the date range of the CIVI Activity Report - hence if the trial and the membership activation record don't coappear, "latest_trial2" may not contain accurate trial data (as stated earlier: in most cases, it will be null). 

Goals and procedure steps: I'll need to identify/isolate trials, select the latest one, then store that in the JSON field of any subsequent 'type' records

 Fields in consolidated_mem_type: type, type_raw,start_dt, datetimerange, type_clean, email, trial_expiration, latest_trial2, ingest_date,lead_date

 example of creating a JSON object in mysql:
 SELECT JSON_OBJECT('id', id, 'name', name, 'age', age) AS json_result
FROM users;
*/

-- TODO place a WHERE clause whereby only mem_type records with null latest_trial2 values are updated; alternatively, where latest_trial2 values don't equal to the true latest trial; the current logic updates ALL records in consolidated_mem_type, which is unnecessary and takes long

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