library(readr)
library(dplyr)
library(janitor)
library(furrr)
library(RSQLite)
library(stringr)
library(DBI)

source("log_data/sqlite_poc.R")

PER_SECOND_TABLE <- "PerSecondCpuCounts"
LOG_ENTRY_TABLE  <- "LogEntries"

furrr::furrr_options(seed = 8675309)


# Bit of code to help with file paths
find_file <- rprojroot::is_rstudio_project$find_file

# Setup the database connection
sqlite_file <- DBI::dbConnect(RSQLite::SQLite(), find_file("demo.sqlite"))

# Drop the per-second table
DBI::dbRemoveTable(sqlite_file, PER_SECOND_TABLE, fail_if_missing = FALSE)


# Database setup ---------------------------------------------------------------

pseq <- Vectorize(seq.default, vectorize.args = c("from", "to"), SIMPLIFY = FALSE)

expand_per_second_chunk_to_db <- function(conn, start_row = 1, length = 1000){
  (chunk
   <- tbl(conn, LOG_ENTRY_TABLE)
   |> select(job_id, start_time, event_time, num_processors)
   |> filter(
     start_time   >  0,
     row_number() >= start_row, 
     row_number() <  start_row + length
   )
   |> collect()
   |> mutate(second = pseq(start_time, event_time))
   |> tidyr::unnest(second)
   |> group_by(second)
   |> summarise(cpus = sum(num_processors))
   |> arrange(second))
  
  dbWriteTable(conn, PER_SECOND_TABLE, chunk, append = TRUE)
  rm(chunk)
}

chunk_size    <- 1000
total_rows    <- tbl(sqlite_file, "LogEntries") |> count() |> pull(n)
chunk_starts  <- seq(1, total_rows, by = chunk_size)
process_chunk <- \(x) expand_per_second_chunk_to_db(sqlite_file, x, chunk_size)

future_walk(chunk_starts, process_chunk, .progress = TRUE)

# Want to keep only seconds that occurred during the window we're observing,
# AKA, on or after the first `event_time`
(first_event 
  <- tbl(sqlite_file, LOG_ENTRY_TABLE)
  |> summarise(first_event = min(event_time))
  |> pull(first_event)
  |> unique())

# Now combine all the chunk sub-results into one final result, in the database!
(tbl(sqlite_file, PER_SECOND_TABLE)
  |> group_by(second)
  |> summarise(cpus = sum(cpus, na.rm = TRUE))
  |> arrange(second)
  |> filter(second >= first_event)
  |> compute(PER_SECOND_TABLE, overwrite = TRUE))





