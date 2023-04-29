#Some code to prepare the LSF logs

#Load libraries
library(anytime)
library(lubridate)
library(readr)
library(dplyr)
library(ggplot2)


#Read in current version of LSF log files
working_file <- read.csv("lsf_small.csv")

#Define the 1-hour blocks which each time frame will be assigned to
hour_blocks <- sprintf("%02d-%02d", 0:23, 1:24)

#Add modified columns to the data like changing Epoch time format
#Ex: '1682116121' (original colname startTime) becomes '2023-04-21 17:28:41 CDT' (new colname start_time)
working_file <-
  working_file %>%
  mutate("start_time"=anytime(startTime),   
         "submit_time"=anytime(submitTime),
         "event_time"=anytime(Event.Time),
         "pend_time"=startTime-submitTime,        #pending time should be start minus submit
         "end_time"=anytime(startTime + runTime), #end time should be start plus run
         "event_time_hour"=hour(event_time),      #which of the 24 hours a given event_time belongs to
         "run_time_sec"=runTime,
         "hour_block"=case_when(event_time_hour == 0 ~ "00-01", #Define which of the 24 1-hour time blocks a given event belongs to
                                TRUE ~ paste(sprintf("%02d", event_time_hour),
                                             sprintf("%02d", event_time_hour + 1), 
                                             sep = "-"))) %>%
  #Arrange some of the most relevant columns to be first, followed by everything else
  select(hour_block,
        start_time, 
          submit_time, 
          event_time, 
          event_time_hour,
          run_time_sec,
          numProcessors,
          maxRMem,
          userName,
          everything())


working_file %>%
  ggplot(aes(x=start_time, y=numProcessors)) +
  geom_line() +
  facet_wrap(~queue)
















