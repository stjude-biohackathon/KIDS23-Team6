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
CONN_TYPE        <- "postgres"  # postgres or sqlite

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

# Connect to DB ----------------------------------------------------------------

get_conn <- function(type) {
  if (type == "sqlite") {
    DBI::dbConnect(RSQLite::SQLite(), find_file("demo.sqlite"))
  } else if (type == "postgres") {
    dbConnect(RPostgres::Postgres(), 
              dbname   = "postgres", 
              host     = "localhost", 
              port     = "5432", 
              user     = "postgres", 
              password = "postgres")
  } else {
    stop(type)
  }
}

conn <- get_conn(CONN_TYPE)

# Write parsed log to CSV ------------------------------------------------------

# Drop previous log table
DBI::dbRemoveTable(conn, LOG_ENTRY_TABLE, fail_if_missing = FALSE)

LOG_ENTRY_FIELDS <- data.frame(
  row_number      = numeric(), job_id     = numeric(),  idx                  = numeric(), 
  requeue_time    = numeric(), start_time = numeric(),  approx_start         = numeric(),
  event_time      = numeric(), approx_end = numeric(),  num_processors       = numeric(),
  avg_mem         = numeric(), run_time   = numeric(),  ineligible_pend_time = numeric(),
  num_gpu_rusages = numeric(), queue      = character()
)

DBI::dbCreateTable(conn, LOG_ENTRY_TABLE, LOG_ENTRY_FIELDS)
DBI::dbSendStatement(conn, "ALTER TABLE log_entries ADD CONSTRAINT log_entries_pk PRIMARY KEY (job_id, idx, requeue_time);")
DBI::dbSendStatement(conn, "CREATE INDEX event_time_idx ON log_entries (event_time) WITH (deduplicate_items = off);")
DBI::dbSendStatement(conn, "CREATE UNIQUE INDEX row_idx ON log_entries (row_number);")

# Read in the 10k dataset from CSV and write to the database
write_chunk_to_db <- function(conn, df, pos) {
  (clean 
   <- janitor::clean_names(df)
   |> mutate(approx_start = signif(start_time, 7),
             approx_end   = signif(event_time, 7),
             row_number   = pos + row_number() - 1)
   |> filter(start_time > 0)
   |> select(
     row_number,      job_id,       idx,        requeue_time, 
     start_time,      approx_start, event_time, approx_end,
     num_processors,  avg_mem,      run_time,   ineligible_pend_time,
     num_gpu_rusages, queue
    ))
  
  DBI::dbWriteTable(conn, LOG_ENTRY_TABLE, clean, append = TRUE)
}

write_chunk_callback <- \(x, y) write_chunk_to_db(conn, x, y)

read_csv_chunked(SOURCE_FILE, 
                 callback   = write_chunk_callback,
                 chunk_size = 100000,
                 guess_max  = 10000,
                 col_names  = RAW_DATA_COL_NME,
                 skip       = 1)

# Run the Per-Second Transform -------------------------------------------------

find_file("log_data/per_second_cpu_usage.R") |> source()

# Clean up your DB connection --------------------------------------------------
dbDisconnect(conn)