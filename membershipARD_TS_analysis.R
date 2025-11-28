library(RMySQL)
library(dplyr)
library(fpp3)

con <- dbConnect(MySQL(),
                 user = 'lcalleja',
                 password = 'salmon01',
                 host = '100.102.223.21',
                 port = 3306,
                 dbname = 'membership_ard')

setwd('/home/candela/pCloudDrive/pCloud Backup/mofongo-HP-EliteBook-840-G8-Notebook-PC/Documents/ghfc/membershipReportsCIVI/greeneHill')

save.image('./memmbershipARD_TS_analysts.RData')
load('./memmbershipARD_TS_analysts.RData')

query <- "SELECT calendar_date, 
SUM(CASE WHEN activity_calc IN ('winback','initial enrollment','technical activation','technical reactivation','technical re-activation') THEN 1 ELSE 0 END) AS active_cnts, 
SUM(CASE WHEN activity_calc IN ('parental leave','medical leave','care giving leave') THEN 1 ELSE 0 END) AS temporary_inactivte, 
SUM(CASE WHEN activity_calc IN ('cancelled','deactivated','deactive','suspended') THEN 1 ELSE 0 END) revoked,
SUM(CASE WHEN activity_calc IN ('general leave') THEN 1 ELSE 0 END) general_leave, 
SUM(CASE WHEN activity_calc IN ('winback') THEN 1 ELSE 0 END) winbacks
FROM stack_job2 sj
INNER JOIN calendar cal ON calendar_date between start_dt AND lead_date 
GROUP BY 1 
ORDER BY 1 desc"

result <- dbGetQuery(con, query)

head(result)
str(result)
is.data.frame(result)

result |>
  mutate(date = as.Date(calendar_date,"%Y-%m-%d")) -> result

result |>
  as_tsibble(index = date) -> result.ts

#no gaps  
has_gaps(result.ts)

result.ts |>
  autoplot(vars(active_cnts)) +
  labs(title="active members over time")


result.ts |>
  autoplot(vars(general_leave)) +
  labs(title="general leave over time")

results.ts |>
  stl()

#calculate what seems to be the default trend window size (going off the documentation)
1.5*358/(1-(1.5/7))

#best fit
result.ts |>
  model(stl = STL(active_cnts~season(window=7)+trend(window=400))) |>
  components() |>
  autoplot() +
  labs(title = "trend window 400")

result.ts |>
  model(stl = STL(active_cnts~season(window=7)+trend(window=500))) |>
  components() |>
  autoplot() +
  labs(title = "trend window 500")

result.ts |>
  model(stl = STL(active_cnts~season(window=7)+trend(window=300))) |>
  components() |>
  autoplot() +
  labs(title = "trend window 300")

result.ts |>
  model(stl = STL(active_cnts~season(window=7)+trend(window=683))) |>
  components() |>
  autoplot() +
  labs(title = "trend window 683 ~ the default")

result.ts |>
  model(stl = STL(active_cnts~season(window=7))) |>
  components() |>
  autoplot() +
  labs(title = "the default")

#check trend strength and seasonal strength
result.ts |>
  model(stl = STL(active_cnts~season(window=7)+trend(window=400))) -> dable

names(dable$stl)
length(dable$stl)
names(dable$stl[[1]])

dable$stl[[1]]$fit

#feat_stl documentation: https://feasts.tidyverts.org/reference/feat_stl.html
result.ts |>
  features(active_cnts, feat_stl, .period=365.25,s.window=7, t.window=400)
