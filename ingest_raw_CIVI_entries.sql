/*
A QA hub for Membership committee entries in CIVI, capturing:
- multiple entries made on the same day
- duplicates entries made by different members of Membership

*/


-- TODO 10/21/2024: cases of multiple changes on the same day messes up the lead_date logic. Email sent to August and Renee on 10/21/24
-- query to  surface members with multiple records on the same day:
WITH multiples AS (
SELECT mt_email, start_dt, count(*) -- NOTE: this query groups on start_dt, a precise timestamp, other options are to aggregate on a longer timespan like calendar day
FROM stack_job2
GROUP BY mt_email, start_dt
HAVING COUNT(*) > 1)
SELECT sj.mt_email, sj.start_dt, sj.activity, sj.mem_type, sj.type_raw 
FROM stack_job2 sj
INNER JOIN multiples m ON sj.mt_email = m.mt_email AND sj.start_dt = m.start_dt
order by 2 desc,1,3;

-- consolidate_mem_type: multiple entries on the same day. Is it safe to choose the latest one?


SELECT email, start_dt, count(*)
FROM consolidated_mem_type 
GROUP BY 1,2
HAVING COUNT(*) > 1
ORDER BY 1,2;