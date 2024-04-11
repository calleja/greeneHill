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
-- consolidated_mem_type is the legacy prod table
CREATE TEMPORARY TABLE consolidated_mem_type_temp LIKE consolidated_mem_type;
INSERT INTO consolidated_mem_type_temp SELECT * FROM consolidated_mem_type;

-- STEP 2: DELETE RECORDS MEETING CRITERIA FROM TEMPORARY TABLE VERSION
-- replace w/most recent report download
SET @initial_dt = (SELECT min(start_dt) FROM membership.mem_type_new_import);
DELETE FROM consolidated_mem_type_temp WHERE start_dt > @initial_dt;

-- STEP 3: insert new records into first temp table
-- make sure to account for 'ingest_date' field
INSERT INTO consolidated_mem_type_temp
select type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, max(ingest_date) ingest_date
-- new table of data
from membership.mem_type_new_import 
GROUP BY 1,2,3,4,5,6,7,8,9;

-- STEP 4 - DELETE DUPES: requires making ANOTHER temp table; this NEW table ("consolidated_mem_type_temp2") is the new de-duped membership table, and is the PROD version going forward
DROP TABLE IF EXISTS consolidated_mem_type_temp2;
CREATE TABLE consolidated_mem_type_temp2 LIKE consolidated_mem_type_temp;

-- de-dupe method: assign row numbers via window function and select for the latest import by way of "ingest_date"
INSERT INTO consolidated_mem_type_temp2
WITH row_num_table AS (
SELECT c_temp.*, row_number() OVER(PARTITION BY type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2 order by ingest_date desc) row_num
FROM consolidated_mem_type_temp c_temp)
SELECT type, type_raw, start_dt, lead_date, datetimerange, type_clean, email, trial_expiration, latest_trial2, ingest_date
FROM row_num_table 
WHERE row_num = 1;

-- QA options: table length of 2nd temp table should be > records of pre-existing prod table and have a 'start_dt' range spanning beginning of legacy prod to end of latest table ingest
-- these can be done in python

SET @legacy_cnt = (select count(*) from consolidated_mem_type_temp2);
SET @incremental_cnt = (select count(*) from consolidated_mem_type);

END //
DELIMITER ;

CALL type_table_create();