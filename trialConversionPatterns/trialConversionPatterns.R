library(RMySQL)
library(dplyr)
library(readxl)
library(fpp3)
library(DBI)
library(zoo)
library(stringr)

#connection attributes
#connection object
con <- dbConnect(
  RMySQL::MySQL(),
  dbname = "membership",
  host = "172.17.0.2",
  port = 3306, # Default MySQL port
  user = "root",
  password = "salmon01"
)

#test connection object
if (!is.null(con)) {
  cat("Connection successful!\n")
}


#query returns one line per email/member that provides metadata around trial starts, types (if applicable) and membership starts; ONLY members that started a membership are considered, and all are included (ie not conditional on trial activity)
query = "
WITH setup AS (
SELECT email, start_dt, type_raw, type_clean, lead_date,
CASE
WHEN type_clean = '2 mo trial' THEN 1
WHEN type_clean = '6 mo trial' THEN 2
ELSE NULL
END AS trial_proxy,
cast(JSON_EXTRACT(latest_trial2, '$.start_dt') as date) last_trial_start, ROW_NUMBER() OVER(PARTITION BY email ORDER BY start_dt) row_num
FROM consolidated_mem_type
where type_clean like '%trial%'
ORDER BY email, start_dt asc),
-- and which had trial rollovers (2 and 6 month trials)
trial_agg AS (SELECT email, max(row_num) tot_trials, max(start_dt) last_trial_start, max(trial_proxy) AS trial_proxy
FROM setup
GROUP BY 1
ORDER BY 1),
mem_starts AS (
SELECT sj.mt_email, sj.start_dt, sj.activity_calc, sj.mem_type
FROM stack_job2 sj
WHERE sj.activity_calc = 'initial enrollment'
GROUP BY 1,2,3,4),
-- LEFT JOIN
joined_data AS (
SELECT ms.*, trial_agg.tot_trials, trial_agg.last_trial_start, trial_agg.trial_proxy
FROM mem_starts ms
LEFT JOIN trial_agg
ON ms.mt_email = trial_agg.email
ORDER BY 1)
SELECT mt_email, start_dt AS full_mem_start_dt, activity_calc, mem_type,
CASE WHEN tot_trials IS NULL THEN 'no trial' ELSE tot_trials END AS tot_trials,
CASE
WHEN trial_proxy = 1 THEN '2 mo trial'
WHEN trial_proxy = 2 THEN '6 mo trial'
ELSE 'no trial'
END AS trial_type,
last_trial_start
FROM joined_data 
WHERE start_dt > DATE('2020-01-01')"

#send query to db server
result <- dbGetQuery(con, query)

#close connection
dbDisconnect(con)

str(result)

#group membership start dates by month-year and count the 3 "trial type" variants: [never started a trial, 2 mo trial, 6 month trial]; this will ignore conversions and multi-trials (for now)
result |>
  mutate(date_obj = as.Date(full_mem_start_dt, "%Y-%m-%d %H:%M:%S")) |>
  mutate(mem_start_month = zoo::as.yearmon(full_mem_start_dt)) |>
  #select(date_obj, mem_start_month) |>
  group_by(mem_start_month, trial_type) |>
  summarize(count = n()) -> df.member

head(df.member)

#create a "proportion" df: this will calculate the prop of new members that ever originated a trial compared with ALL new members; purpose: line plot overlay of prop by month, which requires a separate df of the context/scope difference with df.member
trial_prop <- df.member |>
  mutate(trial_present = stringr::str_detect(trial_type,'mo trial')) |>
  group_by(mem_start_month) |>
  summarize(prop = sum(count[trial_present])/sum(count))
  

#simple stacked barplot
df.member |> 
  ggplot(aes(x = mem_start_month, group = trial_type, fill = trial_type)) +
  geom_bar(aes(y=count), stat ="identity", position="stack")
  
#overlaid plots
ggplot() +
  geom_bar(data = df.member, aes(x = mem_start_month, y = count, fill = trial_type), stat ="identity", position="stack") +
  geom_line(data = trial_prop, aes(x = mem_start_month, y = prop*40)) +
  # Custom the Y scales:
  scale_y_continuous(
    # Features of the first axis
    name = "member count",
    # Add a second axis and specify its features
    sec.axis = sec_axis( trans=~.*1/40, name="proportion having trial", labels = scales::percent)
  ) +
  labs(title = "All membership signups broken out by trial types and overlaid with proportion of signups that also started a trial", x = "membership start month")

#plot 
trial_prop |>
  mutate(mem_start_month = yearmonth(mem_start_month)) |>
  as_tsibble(index=mem_start_month) -> ts.prop

#no discernable seasonality apparent from plotting the proportion on a seasonal plot
ts.prop |>
  gg_season(prop, labels = 'right') +
  labs(title = 'Seasonal plot of trial proportion of membership signups', y = 'proportion of signups that had a trial')

#going about it a different way: what is the conversion success rate on 2-mo and 6-mo trial members (what is the proportion of conversion success)
#calculate success on 2-mo only (exclude those that rolled over to a 6 mo), 6-mo only, extended trial (signed up for 6 mo after a 2 mo)