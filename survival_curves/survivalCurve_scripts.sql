-- SHIFT+ALT+Y <- toggle word wrap

show tables;

select * 
from membership_ard.stack_job2 
limit 10;

-- investigate whether start_dt or lead_date should proceed
SELECT SUM(CASE WHEN start_dt > lead_date THEN 1 else 0 end) lead_earlier,
SUM(CASE WHEN start_dt < lead_date THEN 1 else 0 end) start_earlier
from membership_ard.stack_job2 ;

-- for cases where lead_dt < start_date what is the distribution of the days difference? BY AND LARGE, IT'S 0 or 1
SELECT DATEDIFF(start_dt,lead_date), count(*)
from membership_ard.stack_job2
WHERE start_dt > lead_date
GROUP BY 1
ORDER BY 1 DESC;

-- take note of 'central brooklyn' & 'bushwick' activity as those accounts don't count, so if a member transferred from one of those from a full member, that should be considered a censored activity
select * 
from membership_ard.stack_job2
where mem_type = 'central brooklyn'
order by mt_email, lead_date;


select mem_type from membership_ard.stack_job2 
group by 1 
order by 1;

-- lead_dt is the final reading date for the "activity_calc"; however, there are 275 cases where lead_dt is ahead of start_dt
-- in order to determine a censored event from a query, would likely seek for the max lead_date for an individual... let's see if there is any variance to that. the db is very normalized and so all max lead_date may be the same
-- results here suggest vast majority of accounts have the same lead_dt; there is a small cluster of 134 in 1/1/2019
select max_lead_dt, count(*) from (
select max(date(lead_date)) max_lead_dt
from stack_job2
group by mt_email
order by 1) tabla
GROUP BY 1 
ORDER BY 1;


-- investigate some records from 1/1/2019; this are behaving oddly: the start_dt > lead_date
-- count # of emails having multiple records
DROP TABLE IF EXISTS lead_dt_errors;

CREATE TABLE lead_dt_errors AS
WITH tbl AS (SELECT sj2.mt_email, count(*) cnt
FROM stack_job2 sj2
WHERE sj2.mt_email IN (
    SELECT mt_email
    FROM stack_job2
    GROUP BY mt_email
    HAVING MAX(DATE(lead_date)) = '2019-01-01')
AND sj2.lead_date < sj2.start_dt
GROUP BY 1
ORDER BY 2 desc)
select * from tbl;

show tables;
use membership_ard;
select * from lead_dt_errors limit 10;
select * from calendar limit 10;



/*
 * USE CASES
 * 1) if only one record, bring lead_date forward to the latest lead_date in the table
 * 2) 2 or more records: determine if they are dupes, then delete one
 * 3) if > 1 record, do any of them set a lead_date > start_dt
 */

select * FROM stack_job2 limit 5;
-- USE CASE #3
SELECT email, sum(CASE WHEN lead_date > start_dt THEN 1 ELSE 0 END) cnt
FROM consolidated_mem_type
WHERE email IN (
    SELECT mt_email 
    FROM lead_dt_errors where  cnt >1 ) 
GROUP BY 1 
order by 2 desc;

-- look at select cases where start_dt > lead_dt
/* ann.chiaverini@gmail.com: only one record in mem_type (none in mem_status) for some reason, the lead_date is set to the day before start_dt
 * 'anmuessig@gmail.com': multiple signups on same day (lettuce & carrot); no records in mem_status
 * ajohnmadani@gmail.com
 * ajohnmadani@gmail.com
 * malnorba@gmail.com
 * laurenlarocca1@gmail.com
 * 
 * those with 2 or more (14 total)
 * anmuessig@gmail.com
achomet@gmail.com
camillegb@gmail.com
gcint5@runbox.com
jayhilda@gmail.com
julieseamon@gmail.com
kfranklin@nyls.edu
malnorba@gmail.com - case of two records: one is actually correct (having lead_date set to a date > start_dt)
 */
select *
from consolidated_mem_status
where email = 'achomet@gmail.com';

select *
from consolidated_mem_type
where email = 'julieseamon@gmail.com'
order by start_dt;

select *
from stack_job2 sj 
where mt_email = 'julieseamon@gmail.com';


select activity_calc, text_status_indicator, count(*) 
from stack_job2 sj
where lower(sj.text_status_indicator) like '%deactivate%' OR lower(sj.text_status_indicator) like '%cancel%'
group by 1,2
order by 3 desc;