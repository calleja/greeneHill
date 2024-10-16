/*
Stored procedure accomplishes the following:
1) copy contents of current prod table "consolidated_mem_type" into a temp table and remove all records that appear in the new import (as of "start_dt" field)
2) insert all contents of new import table ("type") into the temp table
3) check for and remove duplicates (requires making a second TEMP table)

expected table names: consolidated_mem_type, (during test) mem_type_0217

run the stored procedure and ensure that it's stored on the server so that I can call it from python
TODO: replace the hard-coded table names below
*/
DROP PROCEDURE IF EXISTS type_table_create;

DELIMITER //

CREATE PROCEDURE type_table_create()

BEGIN
-- STEP 1
DROP TABLE IF EXISTS consolidated_mem_type_temp; -- if exists
-- consolidated_mem_type is the legacy prod table. It follows the same schema as mem_type_MMDD
CREATE TABLE consolidated_mem_type_temp LIKE consolidated_mem_type; -- table consolidated_mem_type_temp will need to be deleted
INSERT INTO consolidated_mem_type_temp SELECT * FROM consolidated_mem_type;

-- STEP 2: DELETE RECORDS MEETING CRITERIA FROM TEMPORARY TABLE VERSION
-- replace w/most recent report download
-- mem_type_new_import was created in orchestration.ipynb and is an exact copy of the latest import file ex. 'mem_type_0722'
SET @initial_dt = (SELECT min(start_dt) FROM membership.mem_type_new_import);
DELETE FROM consolidated_mem_type_temp WHERE start_dt >= @initial_dt;

-- STEP 3: insert new records into first temp table
-- make sure to account for 'ingest_date' field bc it could be duplicated in certain circumstances
INSERT INTO consolidated_mem_type_temp
select type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, max(ingest_date) ingest_date
-- new table of data
from membership.mem_type_new_import 
GROUP BY 1,2,3,4,5,6,7,8,9;

-- STEP 4 - DELETE DUPES: requires making ANOTHER temp table; this NEW table ("consolidated_mem_type_temp2") is the new de-duped membership table, and is the PROD version going forward
-- first segment of the logic overwrites the lead_date field in order to refresh it by "bringing it forward" to the "report date"/ingest_date of the newest data import. Rationale: lead_date is designated at report run date in the .ipynb file, but this has to be brought forward each time the script is run. Only the final record of each member's activity should be brought forward; the earlier (lead_date) ones should be preserved. After the records are accurately brought forward it's appropriate to run the de-dupe script; 
-- TODO: this procedure must also be copied to the "status" stored procedure
-- STEP 4a (date-forwarding segment): project a row number onto ea record of ea member's activity
-- i. declare a variable used for UPDATING the lead_date
-- ii. records where row_num = max row number for the group are candidates for an UPDATE
SET @max_lead_date = (SELECT max(ingest_date) FROM membership.mem_type_new_import);

WITH row_ver AS (
-- changed the ORDER BY clause to start_date from lead_date because start_date is more reliable
select *, ROW_NUMBER() OVER(PARTITION BY email ORDER BY start_date asc) row_num, 
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
DROP TABLE IF EXISTS consolidated_mem_type_temp2;
CREATE TABLE consolidated_mem_type_temp2 LIKE consolidated_mem_type_temp;

-- STEP 4b
-- de-dupe method: for each unique set of values for a subset of rows, we select the LATEST entry (by way of "ingest_date") 
-- field names (10) of consolidated_mem_type: type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, ingest_date
-- QA: ensure that the number of unique rows (as measured by the proper subset) equals the number of rows by email
-- expect that # of rows of consolidated_mem_type_temp2 =< consolidated_mem_type_temp
-- consolidated_mem_type_temp2 is renamed to consolidated_mem_type in the orchestration.ipynb procedure

-- HOT FIX: a new de-dupe method that selects for the max ingest_date on combinations of email, type, type_raw, start_dt, type_clean, trial_expiration, latest_trial2
INSERT INTO consolidated_mem_type_temp2
WITH row_num_table AS (
SELECT c_temp.*, 
-- left out of PARTITION BY clause: 'lead_date' and 'ingest_date'
-- the value of the row_num is that I can reference it later when I attempt to preserve the latest lead_date (all others should be overwritten in the 'stored_procedure_create_tables_stack_job.sql)
row_number() OVER(PARTITION BY type, type_raw, start_dt, datetimerange, type_clean, email, trial_expiration, latest_trial2 order by ingest_date desc) row_num
FROM consolidated_mem_type c_temp)
-- select for row with the latest
SELECT *
FROM row_num_table 
WHERE ingest_date = 
(SELECT max(ingest_date) 
from consolidated_mem_type inner_c 
WHERE row_num_table.email = inner_c.email 
AND row_num_table.type = inner_c.type 
AND row_num_table.type_raw = inner_c.type_raw 
AND row_num_table.start_dt = inner_c.start_dt 
AND row_num_table.type_clean = inner_c.type_clean
AND row_num_table.trial_expiration = inner_c.trial_expiration
AND row_num_table.latest_trial2 = inner_c.latest_trial2)

-- QA options: table length of 2nd temp table should be > records of pre-existing prod table and have a 'start_dt' range spanning beginning of legacy prod to end of latest table ingest

-- LEGACY CODE TO BE DEPRECATED
INSERT INTO consolidated_mem_type_temp2
WITH row_num_table AS (
SELECT c_temp.*, 
-- left out of PARTITION BY clause: 'lead_date' and 'ingest_date'
row_number() OVER(PARTITION BY type, type_raw, start_dt, datetimerange, type_clean, email, trial_expiration, latest_trial2 order by ingest_date desc) row_num
FROM consolidated_mem_type_temp c_temp)
SELECT type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, ingest_date
FROM row_num_table 
WHERE row_num = 1;

END //
DELIMITER ;