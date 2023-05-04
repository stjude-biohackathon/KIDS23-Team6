library(readr)
library(dplyr)
library(janitor)
library(RSQLite)
library(stringr)
library(DBI)


# Bit of code to help with file paths
find_file <- rprojroot::is_rstudio_project$find_file

SOURCE_FILE      <- find_file("log_data/lsf_4m.csv")
LOG_ENTRY_TABLE  <- "log_entries"

RAW_DATA_COL_NME <- c(
  "event_type", "version_number", "event_time", 
  "job_id", "user_id", "options", 
  "num_processors", "submit_time", "begin_time", 
  "term_time", "start_time", "user_name", 
  "queue", "res_req", "depend_cond", 
  "pre_exec_cmd", "from_host", "cwd", 
  "in_file", "out_file", "err_file", 
  "job_file", "num_asked_hosts", "asked_hosts", 
  "num_ex_hosts", "exec_hosts", "j_status", 
  "host_factor", "job_name", "command", 
  "ru_utime", "ru_stime", "ru_maxrss", 
  "ru_ixrss", "ru_ismrss", "ru_idrss", 
  "ru_isrss", "ru_minflt", "ru_majflt", 
  "ru_nswap", "ru_inblock", "ru_oublock", 
  "ru_ioch", "ru_msgsnd", "ru_msgrcv", 
  "ru_nsignals", "ru_nvcsw", "ru_nivcsw", 
  "ru_exutime", "mail_user", "project_name", 
  "exit_status", "max_num_processors", "login_shell", 
  "time_event", "idx", "max_r_mem", 
  "max_r_swap", "in_file_spool", "command_spool", 
  "rsv_id", "sla", "except_mask", 
  "additional_info", "exit_info", "warning_action", 
  "warning_time_period", "charged_saap", "license_project", 
  "app", "post_exec_cmd", "runtime_estimation", 
  "job_group_name", "requeue_evalues", "options2", 
  "resize_notify_cmd", "last_resize_time", "rsv_id2", 
  "job_description", "submit_ext_num", "submit_ext_key", 
  "submit_ext_value", "num_host_rusage", "host_rusage_hostname", 
  "host_rusage_mem", "host_rusage_swap", "host_rusage_utime", 
  "host_rusage_stime", "options3", "run_limit", 
  "avg_mem", "effective_res_req", "src_cluster", 
  "src_job_id", "dst_cluster", "dst_job_id", 
  "forward_time", "flow_id", "ac_job_wait_time", 
  "total_provision_time", "outdir", "run_time", 
  "subcwd", "num_network", "network_id", 
  "network_alloc_num_window", "network_alloc_affinity", "serial_job_energy", 
  "cpi", "gips", "gbs", 
  "gflops", "num_alloc_slots", "alloc_slots", 
  "ineligible_pend_time", "index_range_cnt", "index_range_start1", 
  "index_range_end1", "index_range_step1", "index_range_start_n", 
  "index_range_end_n", "index_range_step_n", "requeue_time", 
  "num_gpu_rusages", "g_rusage_hostname", "g_rusage_num_kvp", 
  "g_rusage_key", "g_rusage_value", "storage_info_c", 
  "storage_info_v", "finish_kvp_num_kvp", "finish_kvp_key", 
  "finish_kvp_value"
)

# Connect to SQLite ------------------------------------------------------------

# IMPORTANT: Remember to unzip the 'lsf_100k.zip' file!

# Setup the database connection
# conn <- DBI::dbConnect(RSQLite::SQLite(), find_file("demo.sqlite"))

# Connect to Postgres ----------------------------------------------------------

conn <- dbConnect(RPostgres::Postgres(), 
                  dbname   = "postgres", 
                  host     = "localhost", 
                  port     = "5432", 
                  user     = "postgres", 
                  password = "postgres")

# Write parsed log to CSV ------------------------------------------------------

# Drop previous log table
DBI::dbRemoveTable(conn, LOG_ENTRY_TABLE, fail_if_missing = FALSE)

# Read in the 10k dataset from CSV and write to the database
write_chunk_to_db <- function(conn, df) {
  clean <- janitor::clean_names(df)
  DBI::dbWriteTable(conn, LOG_ENTRY_TABLE, clean, append = TRUE)
}

write_chunk_callback <- \(x, y) write_chunk_to_db(conn, x)

read_csv_chunked(SOURCE_FILE, 
                 callback   = write_chunk_callback,
                 chunk_size = 100000,
                 guess_max  = 10000,
                 col_names  = RAW_DATA_COL_NME,
                 skip       = 1)

DBI::dbSendQuery(conn, "CREATE UNIQUE INDEX job_id_idx ON log_entries (job_id, idx);")

# (data
#   <- SOURCE_FILE
#   |> readr::read_csv(guess_max = 10000)
#   |> janitor::clean_names())
# 
# # Write the records to a DB table
# dplyr::copy_to(conn, data, "LogEntries", temporary = FALSE)

# DB Proof of Concept ----------------------------------------------------------

# Example for working with data directly through a DB connection.
# We'll get the average run time per user
# (avg_per_user_runtime
#   <- tbl(conn, "LogEntries")
#   |> group_by(user_id)
#   |> summarise(avg_runtime = mean(run_time))
#   |> collect())
# 
# 
# # You can always check to see what SQL query is being generated
# (tbl(conn, "LogEntries")
#   |> group_by(user_id)
#   |> summarise(avg_runtime = mean(run_time))
#   |> show_query())

# Code for building the current interim data set -------------------------------

#Define the 1-hour blocks which each time frame will be assigned to
# hour_blocks <- sprintf("%02d-%02d", 0:23, 1:24)

#Add modified columns to the data like changing Epoch time format
#Ex: '1682116121' (original colname startTime) becomes '2023-04-21 17:28:41 CDT' (new colname start_time)
# (working_file 
#   <- tbl(conn, "LogEntries")
#   |> mutate(
#     "start_time"      = as_datetime(start_time),   
#     "submit_time"     = as_datetime(submit_time),
#     "event_time"      = as_datetime(event_time),
#     "pend_time"       = start_time-submit_time,             #pending time should be start minus submit
#     "end_time"        = as_datetime(start_time + run_time), #end time should be start plus run
#     "event_time_hour" = hour(event_time),                   #which of the 24 hours a given event_time belongs to
#     "run_time_sec"    = run_time,
#     "hour_block"      = case_when(
#       event_time_hour == 0 ~ "00-01", #Define which of the 24 1-hour time blocks a given event belongs to
#       event_time_hour >  9 ~ paste0(     event_time_hour, "-",  event_time_hour+1),
#       event_time_hour == 9 ~ paste0("0", event_time_hour, "-",  event_time_hour+1),
#       event_time_hour <  9 ~ paste0("0", event_time_hour, "-0", event_time_hour+1)
#     ),
#     "day_block"       = format(event_time, "%u")
#   ) 
#   |> select(
#     hour_block,   day_block,      start_time, 
#     submit_time,  event_time,     event_time_hour,
#     run_time_sec, num_processors, max_r_mem,
#     user_name,    everything()
#   ))
         
         
# Write the working file back to the database
# dplyr::copy_to(conn, working_file, "LogEntriesCleaned", temporary = FALSE)

# Run the Per-Second Transform -------------------------------------------------

find_file("log_data/per_second_cpu_usage.R") |> source()

# Clean up your DB connection --------------------------------------------------
dbDisconnect(conn)