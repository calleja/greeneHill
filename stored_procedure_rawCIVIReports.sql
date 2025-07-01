/*
Stored procedure accomplishes the following:
1) copy contents of current prod table "consolidated_mem_type" into a temp table and remove from it all records that appear AFTER the earliest "start_dt" of the new import 
2) insert all contents of new import table ("type") into the temp table
3) bring forward the lead date of the last record
4) check for and remove duplicates (requires making a second TEMP table)
5) overwrite and reasses lead_date field


expected table names: consolidated_mem_type, (during test) mem_type_0217

run the stored procedure and ensure that it's stored on the server so that I can call it from python
TODO: replace the hard-coded table names below
*/
DROP PROCEDURE IF EXISTS type_table_create;

DELIMITER //

CREATE PROCEDURE type_table_create()

BEGIN
-- STEP 1
DROP TABLE IF EXISTS consolidated_civi_temp; -- if exists
-- consolidated_mem_type is the legacy prod table. It follows the same schema as mem_type_MMDD
CREATE TABLE consolidated_civi_temp LIKE consolidated_civi; -- table consolidated_mem_type_temp will need to be deleted
INSERT INTO consolidated_civi_temp SELECT * FROM consolidated_civi;

-- STEP 2: DELETE RECORDS MEETING CRITERIA FROM TEMPORARY TABLE VERSION
-- replace w/most recent report download
-- mem_type_new_import was created in orchestration.ipynb and is an exact copy of the latest import file ex. 'mem_type_0722'
SET @initial_dt = (SELECT min(start_dt) FROM memerbship_ard.mem_type_new_import);
DELETE FROM consolidated_mem_type_temp WHERE start_dt >= @initial_dt;

-- STEP 3: insert new records into first temp table
-- make sure to account for 'ingest_date' field bc it could be duplicated in certain circumstances
INSERT INTO consolidated_mem_type_temp
select type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, max(ingest_date) ingest_date
-- new table of data
from memerbship_ard.mem_type_new_import 
GROUP BY 1,2,3,4,5,6,7,8,9;

-- STEP 4
-- first segment of the logic overwrites the lead_date field in order to refresh it by "bringing it forward" to the "report date"/ingest_date of the newest data import. Rationale: lead_date is designated at report run date in the civiActivityReport.ipynb file, but this has to be brought forward each time the script is run. Only the final record of each member's activity should be brought forward; the earlier (lead_date) ones should be preserved. After the records are accurately brought forward it's appropriate to run the de-dupe script; 
-- TODO: this procedure must also be copied to the "status" stored procedure
-- STEP 4a (date-forwarding segment): project a row number onto ea record of ea member's activity
-- i. declare a variable used for UPDATING the lead_date
-- ii. records where row_num = max row number for the group are candidates for an UPDATE
SET @max_lead_date = (SELECT max(ingest_date) FROM memerbship_ard.mem_type_new_import);

WITH row_ver AS (
-- changed the ORDER BY clause to start_date from lead_date because start_date is more reliable
select *, ROW_NUMBER() OVER(PARTITION BY email ORDER BY start_dt asc) row_num, 
COUNT(start_dt) OVER(PARTITION BY email) total_rows
from consolidated_mem_type_temp),
new_one AS 
(SELECT *, 
-- ** DATE EXTENSION LOGIC**: create 'new_date' field that I will use to replace the lead_date for the "last" record for ea email
CASE 
-- max_lead_date is a table-wide variable NOT an email-specific variable, which should be OK
WHEN row_num = total_rows THEN @max_lead_date 
ELSE lead_date END AS new_date
FROM row_ver)
UPDATE consolidated_mem_type_temp x 
INNER JOIN new_one ON x.email = new_one.email AND x.lead_date = new_one.lead_date 
SET x.lead_date = new_one.new_date;

-- run the de-dupe process on the newly updated 'consolidated_mem_type_temp' table
-- STEP 4b
-- de-dupe method: for each unique set of values for a subset of rows, we select the LATEST entry (by way of "ingest_date") 
-- field names (10) of consolidated_mem_type: type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, ingest_date
-- QA: ensure that the number of unique rows (as measured by the proper subset) equals the number of rows by email
-- expect that # of rows of consolidated_mem_type_temp2 =< consolidated_mem_type_temp
-- consolidated_mem_type_temp2 is renamed to consolidated_mem_type in the orchestration.ipynb procedure
-- HOT FIX: a new de-dupe method that selects for the max ingest_date on combinations of email, type, type_raw, start_dt, type_clean, trial_expiration, latest_trial2
DROP TABLE IF EXISTS consolidated_mem_type_temp2;
CREATE TABLE consolidated_mem_type_temp2 AS
WITH row_num_table AS (
-- SELECT c_temp.*,
SELECT type, type_raw, start_dt, datetimerange, type_clean, email, trial_expiration, latest_trial2, ingest_date, lead_date,
-- left out of PARTITION BY clause: 'lead_date' and 'ingest_date'
-- the value of the row_num is that I can reference it later when I attempt to preserve the latest lead_date (all others should be overwritten in the 'stored_procedure_create_tables_stack_job.sql)
row_number() OVER(PARTITION BY type, type_raw, start_dt, datetimerange, type_clean, email, trial_expiration, latest_trial2 order by ingest_date desc) row_num
FROM consolidated_mem_type_temp c_temp)
-- select for row with the latest
SELECT *
FROM row_num_table 
WHERE ingest_date = 
(SELECT max(ingest_date) 
from consolidated_mem_type_temp inner_c 
WHERE inner_c.email = row_num_table.email
AND inner_c.type = row_num_table.type  
AND inner_c.type_raw = row_num_table.type_raw
AND inner_c.start_dt = row_num_table.start_dt
AND inner_c.type_clean = row_num_table.type_clean
-- trial_expiration to be removed because it carries NULL values, which will negate the JOIN
-- AND inner_c.trial_expiration = row_num_table.trial_expiration 
AND inner_c.latest_trial2 = row_num_table.latest_trial2);

-- HANDLE CASES where multiple type or status entries are made on the same day; solution: select the latest

-- re-run lead_date logic for quality assurance
WITH prelim AS (
SELECT temp2.*, LEAD(date_sub(start_dt, interval 1 day)) OVER(PARTITION BY email ORDER BY start_dt) date_lead2
FROM consolidated_mem_type_temp2 temp2
order by 1,2) 
UPDATE consolidated_mem_type_temp2 AS status2 
-- first apply the inner join, then set values of one column (date_lead) to the other col (date_lead2) ON THE SAME ROW; the alternative would be to write a CASE statement, but then I'd end up with a row with which to deal
-- lead the start_dt of the proceeding status, if one exists and attempt to replace the lead_date field for the type row
INNER JOIN prelim x 
ON status2.email = x.email AND status2.start_dt = x.start_dt AND status2.type_clean = x.type_clean
SET status2.lead_date = x.date_lead2 
WHERE x.date_lead2 IS NOT NULL;

-- QA options: table length of 2nd temp table should be > records of pre-existing prod table and have a 'start_dt' range spanning beginning of legacy prod to end of latest table ingest

-- LEGACY CODE TO BE DEPRECATED
/*
INSERT INTO consolidated_mem_type_temp2
WITH row_num_table AS (
SELECT c_temp.*, 
-- left out of PARTITION BY clause: 'lead_date' and 'ingest_date'
row_number() OVER(PARTITION BY type, type_raw, start_dt, datetimerange, type_clean, email, trial_expiration, latest_trial2 order by ingest_date desc) row_num
FROM consolidated_mem_type_temp c_temp)
SELECT type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, ingest_date
FROM row_num_table 
WHERE row_num = 1;
*/
END //
DELIMITER ;