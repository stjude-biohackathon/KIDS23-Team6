library(anytime)
library(readr)
library(dplyr)
library(janitor)
library(RSQLite)
library(stringr)
library(DBI)

# Database setup ---------------------------------------------------------------

# IMPORTANT: Remember to unzipe the 'lsf_100k.zip' file!

# Bit of code to help with file paths
find_file <- rprojroot::is_rstudio_project$find_file

# Setup the database connection
sqlite_file <- DBI::dbConnect(RSQLite::SQLite(), find_file("demo.sqlite"))

# Read in the 10k dataset from CSV
(data 
  <- find_file("log_data/lsf_large.csv") 
  |> readr::read_csv(guess_max = 10000) 
  |> janitor::clean_names())

# Write the records to a DB table
dplyr::copy_to(sqlite_file, data, "LogEntries", temporary = FALSE)

# DB Proof of Concept ----------------------------------------------------------

# Example for working with data directly through a DB connection.
# We'll get the average run time per user
(avg_per_user_runtime
  <- tbl(sqlite_file, "LogEntries")
  |> group_by(user_id)
  |> summarise(avg_runtime = mean(run_time))
  |> collect())


# You can always check to see what SQL query is being generated
(tbl(sqlite_file, "LogEntries")
  |> group_by(user_id)
  |> summarise(avg_runtime = mean(run_time))
  |> show_query())

# pseq <- Vectorize(seq.default, vectorize.args = c("from", "to"))
# 
# (test
#   <- data
#   |> select(start_time, event_time, num_processors)
#   |> head(2)
#   |> mutate(second = pseq(start_time, event_time))
#   |> tidyr::unnest(second))

# Code for building the current interim data set -------------------------------

#Define the 1-hour blocks which each time frame will be assigned to
hour_blocks <- sprintf("%02d-%02d", 0:23, 1:24)

#Add modified columns to the data like changing Epoch time format
#Ex: '1682116121' (original colname startTime) becomes '2023-04-21 17:28:41 CDT' (new colname start_time)
working_file <-
  tbl(sqlite_file, "LogEntries") %>%
  mutate("start_time"=as_datetime(start_time),   
         "submit_time"=as_datetime(submit_time),
         "event_time"=as_datetime(event_time),
         "pend_time"=start_time-submit_time,        #pending time should be start minus submit
         "end_time"=as_datetime(start_time + run_time), #end time should be start plus run
         "event_time_hour"=hour(event_time),      #which of the 24 hours a given event_time belongs to
         "run_time_sec"=run_time,
         "hour_block"=case_when(event_time_hour == 0 ~ "00-01", #Define which of the 24 1-hour time blocks a given event belongs to
                                event_time_hour > 9 ~ paste0(event_time_hour, "-", event_time_hour+1),
                                event_time_hour == 9 ~ paste0("0", event_time_hour, "-", event_time_hour+1),
                                event_time_hour < 9 ~ paste0("0", event_time_hour, "-0", event_time_hour+1)),
         "day_block"=format(event_time, "%u")) %>%
  #Arrange some of the most relevant columns to be first, followed by everything else
  select(hour_block,
         day_block,
         start_time, 
         submit_time, 
         event_time, 
         event_time_hour,
         run_time_sec,
         num_processors,
         max_r_mem,
         user_name,
         everything())
         
         
# Write the working file back to the database
dplyr::copy_to(sqlite_file, working_file, "LogEntriesCleaned", temporary = FALSE)

# Clean up your DB connection
dbDisconnect(sqlite_file)