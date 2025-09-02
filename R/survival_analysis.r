# Install once:
# install.packages(c("DBI","RPostgres","dplyr","survival","survminer"))

library(DBI); library(RPostgres); library(dplyr)
library(survival); library(survminer)

con <- dbConnect(RPostgres::Postgres(),
  host="localhost", port=5432, dbname="viewerdb",
  user="viewer", password="viewerpass"
)

# last 24h events
events <- dbGetQuery(con, "
  SELECT ts, viewer_id, event_type
  FROM events
  WHERE ts > now() - interval '24 hours'
  ORDER BY viewer_id, ts
")
dbDisconnect(con)

# Build dwell per viewer (start→end; if missing end, censor at now)
events$ts <- as.POSIXct(events$ts, tz="UTC")
starts <- events %>% filter(event_type == "view_start") %>% group_by(viewer_id) %>% summarise(start=min(ts))
ends   <- events %>% filter(event_type == "view_end")   %>% group_by(viewer_id) %>% summarise(end=max(ts))
df <- full_join(starts, ends, by="viewer_id")
df$end[is.na(df$end)] <- Sys.time()
df$dwell_sec <- as.numeric(df$end - df$start, units="secs")
df$churned <- ifelse(is.na(ends$end[df$viewer_id %in% ends$viewer_id]), 0, 1)  # 1 if ended, 0 censored

fit <- survfit(Surv(dwell_sec, churned) ~ 1, data=df)
ggsurvplot(fit, conf.int=TRUE, risk.table=TRUE, ggtheme=theme_minimal(),
           xlab="Seconds", ylab="Survival (still engaged)")
