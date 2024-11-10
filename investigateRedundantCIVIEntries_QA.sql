-- HANDLE CASES where multiple type or status entries are made on the same day; solution: select the latest, which is accomplished on civiActivityReport_selectBestTrans.ipynb

-- TODO 10/21/2024: cases of multiple changes on the same day messes up the lead_date logic. Email sent to August and Renee on 10/21/24
-- query to  surface members with multiple records on the same day:
WITH multiples AS (
SELECT mt_email, start_dt, count(*)
FROM stack_job2
GROUP BY mt_email, start_dt
HAVING COUNT(*) > 1)
SELECT sj.mt_email, sj.start_dt, sj.activity, sj.mem_type, sj.type_raw 
FROM stack_job2 sj
INNER JOIN multiples m ON sj.mt_email = m.mt_email AND sj.start_dt = m.start_dt
order by 2 desc,1,3;


-- surfaces 82 rows of entries made at the same timestamp
WITH multiples AS (
SELECT email, start_dt, count(*)
FROM consolidated_mem_status
GROUP BY email, start_dt
HAVING COUNT(*) > 1)
SELECT sj.*, raw.Target_Name_act , raw.Source_Email_act, raw.Target_Email_act, raw.Subject_act, raw.Activity_Date_DT_act 
FROM consolidated_mem_status sj
INNER JOIN multiples m ON sj.email = m.email AND sj.start_dt = m.start_dt
LEFT JOIN consolidated_rawActivityReport raw ON raw.email_grouping = m.email AND raw.Activity_Date_DT_act  = m.start_dt
order by email, start_dt;

-- look at dupes occuring just within the app; this returns more than the INNER JOIN version above
WITH multiples AS (
SELECT email, start_dt, count(*)
FROM consolidated_mem_status
GROUP BY email, start_dt
HAVING COUNT(*) > 1)
SELECT sj.*
FROM consolidated_mem_status sj
WHERE email = 'amanda.brianna@me.com'
order by email, start_dt;

-- another stab at joining consolidated_mem_status to consolidated_rawActivityReport
-- PRODUCTION CODE: THE PROPER WAY TO JOIN BOTH consolidated_mem_status to the raw Activity reports
WITH multiples AS (
SELECT email, start_dt, count(*)
FROM consolidated_mem_status
GROUP BY email, start_dt
HAVING COUNT(*) > 1)
SELECT sj.*, raw.Target_Name_act , raw.Source_Email_act, raw.Target_Email_act, raw.Subject_act, raw.Activity_Date_DT_act 
FROM consolidated_mem_status sj
INNER JOIN multiples m ON sj.email = m.email AND sj.start_dt = m.start_dt
LEFT JOIN consolidated_rawActivityReport raw ON sj.email = raw.email_grouping AND sj.start_dt = raw.Activity_Date_DT_act  AND regexp_substr(sj.type_raw,'^.*(?=(\_))') = raw.Subject_act
order by email, start_dt;

-- can't join on email and Activity_Date_DT_act alone bc that will return a 1:many. I need to also include type_raw field (which will map to Subject_act), the only catch is that type_raw was affixed with a suffix in the form of "_" and an integer <- that needs to be removed
-- practicing regex
SELECT type_raw, regexp_substr(sj.type_raw,'^.*(?=(\_))')
FROM consolidated_mem_status sj 
limit 10;


-- email and start_dt at the exact same timestamp: 48 entries returned
WITH multiples AS (
SELECT email, start_dt, count(*)
FROM consolidated_mem_status
GROUP BY email, start_dt
HAVING COUNT(*) > 1)
SELECT sj.*, raw.Target_Name_act , raw.Source_Email_act, raw.Target_Email_act, raw.Subject_act, raw.Activity_Date_DT_act 
FROM consolidated_mem_status sj
LEFT JOIN consolidated_rawActivityReport raw ON sj.email = raw.email_grouping AND sj.start_dt = raw.Activity_Date_DT_act  AND trim(regexp_substr(sj.type_raw,'^.*(?=(\_))')) = trim(raw.Subject_act)
INNER JOIN multiples m ON sj.email = m.email AND sj.start_dt = m.start_dt
order by email, start_dt;

-- EXPAND THE PURVIEW TO CALENDAR DAY FROM TIMESTAMP
WITH multiples AS (
SELECT email, date(start_dt), count(*)
FROM consolidated_mem_status
GROUP BY email, date(start_dt)
HAVING COUNT(*) > 1)
SELECT sj.*, raw.Target_Name_act , raw.Source_Email_act, raw.Target_Email_act, raw.Subject_act, raw.Activity_Date_DT_act 
FROM consolidated_mem_status sj
LEFT JOIN consolidated_rawActivityReport raw ON sj.email = raw.email_grouping AND sj.start_dt = raw.Activity_Date_DT_act  AND regexp_substr(sj.type_raw,'^.*(?=(\_))') = raw.Subject_act
INNER JOIN multiples m ON sj.email = m.email AND date(sj.start_dt) = date(m.start_dt)
order by email, start_dt;