/*
Stored procedure accomplishes the following:
1) copy contents of current prod table "consolidated_mem_status" into a temp table and remove all records that appear in the new import (as of "start_dt" field) <- FAILURE: if a 'lead_date' of a legacy import is after a new import, there is no adjustment and one:many records can exist for an email as of a particular date and inflate AccountFlow and Active Account records
2) insert all contents of new import table ("type") into the temp table
3) check for and remove duplicates (requires making a second TEMP table)

expected table names: consolidated_mem_type, (during test) mem_type_0217

run the stored procedure and ensure that it's stored on the server so that I can call it from python
TODO: replace the hard-coded table names below ("mem_type_0217")
*/
DROP PROCEDURE IF EXISTS status_table_create;

DELIMITER //

CREATE PROCEDURE status_table_create()

BEGIN
-- STEP 1
DROP TABLE IF EXISTS consolidated_mem_status_temp; -- if exists
-- consolidated_mem_status is the legacy prod table
CREATE TABLE consolidated_mem_status_temp LIKE consolidated_mem_status;
INSERT INTO consolidated_mem_status_temp SELECT * FROM consolidated_mem_status;

-- STEP 2: DELETE RECORDS MEETING CRITERIA FROM TEMPORARY TABLE VERSION
-- replace w/most recent report download
SET @initial_dt = (SELECT min(start_dt) FROM membership.mem_status_new_import);
DELETE FROM consolidated_mem_status_temp WHERE start_dt >= @initial_dt;

-- STEP 3: insert new records into first temp table
-- make sure to account for 'ingest_date' field
INSERT INTO consolidated_mem_status_temp
select type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, max(ingest_date) ingest_date
-- new table of data
from membership.mem_status_new_import 
GROUP BY 1,2,3,4,5,6,7;


-- STEP 4 - DELETE DUPES: requires making ANOTHER temp table; this NEW table ("consolidated_mem_status_temp2") is the new de-duped membership table, and is the PROD version going forward
-- TODO: re-write the lead_date field in order to refresh it by "bringing it forward" to the report date of the newest data import. Rationale: lead_date is designated at report run date in the .ipynb file, but this has to be brought forward each time. Only the final record of each member's activity should be brought forward; the earlier ones should be preserved. After the records are accurately brought forward is it appropriate to run the de-dupe script; NOTE: this procedure must also be copied to the "status" stored procedure
-- *** PROPOSAL *** - re-calc the lead_dt AFTER new records are consolidated with old records, which requires ignoring or overwriting all lead_dt records
-- STEP 4a: project a row number onto ea record of ea member's activity
-- variable used for UPDATING the lead_date
-- STEP 4b: records where row_num = max row number for the group are candidates for an UPDATE
SET @max_lead_date = (SELECT max(ingest_date) FROM membership.mem_status_new_import);

WITH row_ver AS (
select *, ROW_NUMBER() OVER(PARTITION BY email ORDER BY lead_date asc) row_num, 
COUNT(start_dt) OVER(PARTITION BY email) total_rows
from consolidated_mem_status_temp),
new_one AS 
(SELECT *, 
-- create 'new_date' field that I will use to replace the lead_date for the "last" record for ea email
CASE 
WHEN row_num = total_rows THEN @max_lead_date 
ELSE lead_date END AS new_date
FROM row_ver)
UPDATE consolidated_mem_status_temp x 
INNER JOIN new_one ON x.email = new_one.email AND x.lead_date = new_one.lead_date 
SET x.lead_date = new_one.new_date;

-- new logic introduced/tested first on 'stored_procedure_create_type_tables.sql'
DROP TABLE IF EXISTS consolidated_mem_status_temp2;
CREATE TABLE consolidated_mem_status_temp2 AS
WITH row_num_table AS (
-- SELECT c_temp.*,
SELECT type, type_raw, start_dt, datetimerange, type_clean, email, ingest_date, lead_date,
-- left out of PARTITION BY clause: 'lead_date' and 'ingest_date'
-- the value of the row_num is that I can reference it later when I attempt to preserve the latest lead_date (all others should be overwritten in the 'stored_procedure_create_tables_stack_job.sql)
row_number() OVER(PARTITION BY type, type_raw, start_dt, datetimerange, type_clean, email order by ingest_date desc) row_num
FROM consolidated_mem_status_temp c_temp)
-- select for row with the latest
SELECT *
FROM row_num_table 
WHERE ingest_date = 
(SELECT max(ingest_date) 
from consolidated_mem_status_temp inner_c 
WHERE inner_c.email = row_num_table.email
AND inner_c.type = row_num_table.type  
AND inner_c.type_raw = row_num_table.type_raw
AND inner_c.start_dt = row_num_table.start_dt
AND inner_c.type_clean = row_num_table.type_clean);

-- re-run lead_date logic for quality assurance
WITH prelim AS (
SELECT temp2.*, LEAD(date_sub(start_dt, interval 1 day)) OVER(PARTITION BY email ORDER BY start_dt) date_lead2
FROM consolidated_mem_status_temp2 temp2
order by 1,2) 
UPDATE consolidated_mem_status_temp2 AS status2 
-- first apply the inner join, then set values of one column (date_lead) to the other col (date_lead2) ON THE SAME ROW; the alternative would be to write a CASE statement, but then I'd end up with a row with which to deal
-- lead the start_dt of the proceeding status, if one exists and attempt to replace the lead_date field for the type row
INNER JOIN prelim x 
ON status2.email = x.email AND status2.start_dt = x.start_dt AND status2.type_clean = x.type_clean
SET status2.lead_date = x.date_lead2 
WHERE x.date_lead2 IS NOT NULL;

/* legacy de-dupe code:
DROP TABLE IF EXISTS consolidated_mem_status_temp2;
CREATE TABLE consolidated_mem_status_temp2 LIKE consolidated_mem_status_temp;

-- de-dupe method: assign row numbers via window function and select for the latest import by way of "ingest_date"
INSERT INTO consolidated_mem_status_temp2
WITH row_num_table AS (
SELECT c_temp.*, row_number() OVER(PARTITION BY type, type_raw, start_dt, lead_date, datetimerange, type_clean, email order by ingest_date desc) row_num
FROM consolidated_mem_status_temp c_temp)
SELECT type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, ingest_date
FROM row_num_table 
WHERE row_num = 1;
*/

END //
DELIMITER ;

-- CALL status_table_create();