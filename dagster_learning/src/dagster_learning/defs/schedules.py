import dagster as dg
from dagster_learning.defs.jobs import trip_update_job, trips_by_week_job

trip_update_schedule = dg.ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="0 0 * * *", # every day at midnight
)

trip_week_schedule = dg.ScheduleDefinition(
    job=trips_by_week_job,
    cron_schedule="0 0 * * 1", # every Monday at midnight
)
