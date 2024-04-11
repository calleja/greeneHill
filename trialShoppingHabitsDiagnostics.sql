/* most trial members are falling off the final table; theories: only shopping trial members are being captured, but I'd like them all to appear
- some trial members are being depicted with 9 visits in one day; I wonder if that is incorrect (ex. abaurley@gmail.com on 10/31 shopped 10 times)
- trace the journey from 'prep' down to the end of trialShoppingHabits and find the step at which I lose a chunk of trial members
- recently added: shop_log.Activity_Date, so I can verify that the shopping date coincides with the trial status (ex. 'in trial')
- check the calc on 'total trips' as it's possible that trips taken outside of trial window are being captured
*/

set @min_dt := (SELECT MIN(Activity_Date) from shop_log);
set @max_dt := (SELECT MAX(Activity_Date) from shop_log);

CREATE TABLE trialShoppingHabits AS
WITH prep AS (
SELECT CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) = 'null' THEN NULL 
ELSE CAST(JSON_UNQUOTE(JSON_EXTRACT(latest_trial2, '$.start_dt[0]')) as date) END AS trial_dt, 
email, type_clean, start_dt, ingest_date, trial_expiration
from mem_type_0101),
-- conversions CTE composed of only non-trial records where there WAS an originating trial for the email account; ie successful conversions
conversions AS (
SELECT  trial_dt, datediff(cast(start_dt as date), prep.trial_dt) as date_difference,
email, type_clean
from prep
where (type_clean not like '%trial%' AND type_clean not like '%bushwick%' AND type_clean not like '%park%')
AND trial_dt is not null), 
-- stats = aggregation of trips/shopping visits by: email, trial_start_dt, trial_expiration, ingest_date
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
ELSE 'trial iteration expected' 
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
AND trial_expiration is not null
AND mt.type_clean like '%trial%'
GROUP BY 1,2,3,4,5,6)
select * 
from stats 
order by email;


select STR_TO_DATE(CONCAT(yearweek(trial_start_dt),' Sunday'), '%X%V %W') FirstDayOfWeek, 
avg(trips) avg_trips_while_trial, 
count(distinct email) trial_members, 
sum(case when tsh.trips > 
(SELECT avg(tsh_m.trips) from trialShoppingHabits tsh_m WHERE STR_TO_DATE(CONCAT(yearweek(trial_start_dt),' Sunday'), '%X%V %W') = STR_TO_DATE(CONCAT(yearweek(trial_start_dt),' Sunday'), '%X%V %W') AND tsh_m.relative_trial_period = 'in trial') THEN 1 ELSE 0 end) num_over_avg
-- (select max(mini.type_clean) from mem_type mini where mini.email = mt.email AND mini.type_clean not like '%trial%') 
from trialShoppingHabits tsh
where relative_trial_period IN ('in trial','n/a')
AND mo_type is null
group by 1
order by 1 asc;


-- close but doesn't work
select tsh.*, STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W') FirstDayOfWeek,  secondT.*
from trialShoppingHabits tsh, 
(SELECT avg(tsh_m.trips) 
from trialShoppingHabits tsh_m 
WHERE STR_TO_DATE(CONCAT(yearweek(tsh_m.trial_start_dt),' Sunday'), '%X%V %W') = STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W') 
AND tsh_m.relative_trial_period IN ('in trial','n/a')) secondT
where tsh.relative_trial_period IN ('in trial','n/a')
AND tsh.mo_type is null
AND tsh.trial_start_dt between date('2023-12-03') AND date('2023-12-09')
order by 1 asc;

-- THIS WORKS!!!!!! Will need to place this into its own CTE
select tsh.*, STR_TO_DATE(CONCAT(yearweek(trial_start_dt),' Sunday'), '%X%V %W') FirstDayOfWeek
from trialShoppingHabits tsh
WHERE tsh.relative_trial_period IN ('in trial','n/a')
AND tsh.mo_type is null
AND tsh.trial_start_dt between date('2023-12-03') AND date('2023-12-09')
AND tsh.trips > (SELECT avg(tsh_m.trips) 
from trialShoppingHabits tsh_m 
WHERE STR_TO_DATE(CONCAT(yearweek(tsh_m.trial_start_dt),' Sunday'), '%X%V %W') = STR_TO_DATE(CONCAT(yearweek(tsh.trial_start_dt),' Sunday'), '%X%V %W') 
AND tsh_m.relative_trial_period IN ('in trial','n/a'));

