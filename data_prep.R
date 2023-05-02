#Prepare the LSF logs

#Load libraries
library(anytime)
library(lubridate)
library(readr)
library(dplyr)
library(ggplot2)


#Read in current version of LSF log files
working_file <- read.csv("lsf_100k.csv")

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
                                             sep = "-")),
         "day_block"=wday(event_time, label=TRUE)) %>%
  #Arrange some of the most relevant columns to be first, followed by everything else
  select(hour_block,
         day_block,
        start_time, 
          submit_time, 
          event_time, 
          event_time_hour,
          run_time_sec,
          numProcessors,
          maxRMem,
          userName,
          everything())

#Identify the time windows available in the data, as dates:
window_start <- format(as.Date(substr(min(working_file$event_time), 1, 10), format = "%Y-%m-%d"), "%d_%b_%Y")
window_end   <- format(as.Date(substr(max(working_file$event_time), 1, 10), format = "%Y-%m-%d"), "%d_%b_%Y")



working_file %>%
  ggplot(aes(x=day_block, y=numProcessors)) +
  geom_point() +
  facet_wrap(~queue) +
  theme(panel.border=element_rect(fill=NA, color="black"),
        axis.text.x=element_text(angle=45, hjust=1))

working_file %>%
  group_by(day_block) %>%
  mutate("numProcessors_sum"=sum(numProcessors)) %>%
  ungroup() %>%
  ggplot(aes(x=day_block, y=numProcessors_sum, color=numProcessors_sum)) +
  geom_point() +
  facet_wrap(~queue) +
  theme(panel.border=element_rect(fill=NA, color="black"),
        axis.text.x=element_text(angle=45, hjust=1))

#numProcessors by users, colored by queue
working_file %>%
  ggplot(aes(x=numProcessors, y=reorder(userName, numProcessors), color=queue)) +
  geom_point()

#Total numProcessors by users
working_file %>%
  group_by(userName) %>%
  mutate(numProcessors_sum = sum(numProcessors)) %>%
  ungroup() %>%
  ggplot(aes(x=numProcessors_sum, y=reorder(userName, numProcessors_sum))) +
  geom_point() +
  xlab("Cummulative # of Processors") +
  ylab("") +
  ggtitle(paste0(as.date(window_start)))
















