library(readr)
library(dplyr)
library(janitor)
library(furrr)
library(RSQLite)
library(stringr)
library(DBI)

PER_SECOND_TABLE <- "per_second_cpus_count"

furrr_options(seed = NULL)
plan(multisession, workers = 6, gc = TRUE)

# Drop the per-second table
DBI::dbRemoveTable(conn, PER_SECOND_TABLE, fail_if_missing = FALSE)
DBI::dbSendStatement(conn, "ALTER TABLE per_second_cpus_count DROP CONSTRAINT timestamp_idx RESTRICT;")

# Database setup ---------------------------------------------------------------

pseq <- Vectorize(seq.default, vectorize.args = c("from", "to"), SIMPLIFY = FALSE)

# Want to keep only seconds that occurred during the window we're observing,
# AKA, on or after the first `event_time`
(first_event 
  <- tbl(conn, LOG_ENTRY_TABLE)
  |> summarise(first_event = min(approx_end))
  |> pull(first_event)
  |> unique())

expand_per_second_chunk_to_db <- function(start_row = 1, length = 1000) {
  conn <- get_conn(CONN_TYPE)
  
  (chunk
   <- tbl(conn, LOG_ENTRY_TABLE)
   |> filter(
     row_number >= start_row, 
     row_number <  start_row + length
   )
   |> collect()
   |> mutate(
     true_start = pmax(first_event, approx_start),
     second     = pseq(true_start, approx_end, by = 1000)
   )
   |> tidyr::unnest(second)
   |> group_by(second, queue)
   |> mutate(across(everything(), ~ replace(.x, .x == -1, NA)))
   |> summarise(
     cpus    = sum(num_processors, na.rm = T),
     mem     = sum(avg_mem, na.rm = T),
     run     = sum(run_time, na.rm = T),
     pend    = sum(ineligible_pend_time, na.rm = T),
     gpu     = sum(num_gpu_rusages, na.rm = T),
     .groups = "drop"
   )
   |> arrange(second))
  
  dbWriteTable(conn, PER_SECOND_TABLE, chunk, append = TRUE)
  dbDisconnect(conn)
  rm(chunk)
}

chunk_size    <- 750 # Don't make this too big!
total_rows    <- tbl(conn, LOG_ENTRY_TABLE) |> count() |> pull(n)
chunk_starts  <- seq(1, total_rows, by = chunk_size)
process_chunk <- \(x) expand_per_second_chunk_to_db(x, chunk_size)

furrr::future_walk(chunk_starts, process_chunk, .progress = TRUE)

 # Now combine all the chunk sub-results into one final result, in the database!
(tbl(conn, PER_SECOND_TABLE)
  |> group_by(second)
  |> summarise(cpus = sum(cpus, na.rm = TRUE))
  |> arrange(second)
  |> filter(second >= first_event)
  |> compute(PER_SECOND_TABLE, overwrite = TRUE))

DBI::dbSendStatement(conn, "CREATE UNIQUE INDEX timestamp_idx ON per_second_cpus_count (second);")




