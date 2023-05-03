library(readr)
library(dplyr)
library(RSQLite)
library(DBI)

# Bit of code to help with file paths
find_file <- rprojroot::is_rstudio_project$find_file

# Setup the database connection
sqlite_file <- DBI::dbConnect(RSQLite::SQLite(), "demo.sqlite")

# Read in the 10k dataset from CSV
data <- find_file("log_data/lsf_10k.csv") |> readr::read_csv(guess_max = 10000)

# Write the records to a DB table
dplyr::copy_to(sqlite_file, data, "LogEntries")

# Example for working with data directly through a DB connection.
# We'll get the average run time per user
(avg_per_user_runtime
  <- tbl(sqlite_file, "LogEntries")
  |> group_by(userId)
  |> summarise(avg_runtime = mean(runTime))
  |> collect())


# You can always check to see what SQL query is being generated
(tbl(sqlite_file, "LogEntries")
  |> group_by(userId)
  |> summarise(avg_runtime = mean(runTime))
  |> show_query())

# Clean up your DB connection
dbDisconnect(sqlite_file)