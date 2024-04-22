-- ************** THE BELOW REQUIRES THE SHOP LOG TO BE IMPORTED INTO MYSQL DB FIRST************
-- ingest member shopping activity via ingestMemberShopping.ipynb
-- join shopping data ("shop_log") to mem_type data; later: freq of shopping for trial members, while in-trial, and relate to type conversions
-- all recorded/historical shopping is categorized (in/pre/post trial) for each iteration of their trial (ie I don't attempt to segregate shopping activity and assign it to a trial - it's always all considered)
-- trialShoppingHabits = transactional table of trial member shopping activity (exclusively): one record per shopping trip that classifies it as in/pre/out of trial, whether a conversion occurred, total trips while in-trial
-- if this version doesn't work, consult trialShoppingHabitsDiagnostics.sql for extensive QA
DROP TABLE IF EXISTS trialShoppingHabits2;

/* 
DECLARE min_dt DATE;
DECLARE max_dt DATE;
*/
set @min_dt := (SELECT MIN(Activity_Date) from shop_log);
set @max_dt := (SELECT MAX(Activity_Date) from shop_log);

/*
columns =
trial start_dt
email 
trial_expiration
ingest_date: will be uniform throughout table
relative_trial_period: in-trial, pre-trial, post-trial and n/a (catch-all)
final_trial_flag: capture whether there were multiple trial (ex. 2mo rollovers to 6mo)
mo_type: membership type, which will only be non-null if they converted
trips: aggregation (count of trips) by all the fields above
*/
CREATE TABLE trialShoppingHabits2 AS
-- prep appends a trial_dt field referencing the existing(?); membership start dates filtered at the "stats" CTE step
WITH prep AS (
SELECT 
CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) = 'null' THEN NULL 
ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) as date) 
END AS trial_dt, 
email, type_clean, start_dt, ingest_date, trial_expiration
from consolidated_mem_type_temp2), -- last working code pointed to table mem_type_0101
-- conversions CTE composed of only non-trial records where there WAS an originating trial for the email account; ie successful conversions
conversions AS (
SELECT  trial_dt, datediff(cast(start_dt as date), prep.trial_dt) as date_difference,
email, type_clean
from prep
where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
AND trial_dt is not null), 
-- stats CTE is aggregation of trips/shopping visits by: email, trial_start_dt, trial_expiration, ingest_date
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
ELSE 'trial iteration expected' -- apriori evidence of an upcoming subsequent trial
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
AND mt.trial_expiration is not null
AND mt.type_clean like '%trial%'
GROUP BY 1,2,3,4,5,6)
select * 
from stats 
order by email;


-- surface the actual trial members likely to convert: trial members that have shopped since trial has begun and still in-trial
-- partition/craft cohorts from trial start week
-- member_directory is a table located in the db that is imported via its own pipeline: 'ingestMembershipContactInfo.ipynb'
-- ideally this is moved to a python script where I can simulataneously write output to csv
WITH trips_data AS (
SELECT STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W') FirstDayOfWeek, tsh.email, tsh.trips, tsh.trial_expiration, (SELECT avg(tsh_m.trips) ave
from trialShoppingHabits2 tsh_m
WHERE tsh_m.relative_trial_period IN ('in trial','n/a')
AND tsh_m.final_trial_flag = 'relevant trial'
AND STR_TO_DATE(CONCAT(yearweek(tsh_m.trial_start_dt),' Sunday'), '%X%V %W') = STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W')) cohort_avg
FROM trialShoppingHabits2 tsh 
where tsh.relative_trial_period = 'in trial' 
AND tsh.final_trial_flag = 'relevant trial'
AND tsh.mo_type IS NULL) 
SELECT td.*, md.contact_name, md.first_name, md.last_name, md.phone
FROM trips_data td 
LEFT JOIN member_directory2 md ON td.email = md.email 
-- select for only those trial members that have # trips greater than avg for their cohort
WHERE trips > cohort_avg 
AND trips > 1 
AND trial_expiration BETWEEN date_sub(curdate(), interval 40 day) AND date_add(curdate(), interval 10 day)
order by FirstDayOfWeek asc;

--  QA: in order to QA the trial member's journey and ensure that they did not convert, review their history in consolidated_mem_type_temp2
-- 'names' is a CTE crafted from the last query above
select mt.*
from consolidated_mem_type_temp2 mt
INNER JOIN names ON mt.email=names.email 
order by email, start_dt asc;