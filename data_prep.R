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
          numProcessors,
          maxRMem,
          userName,
          everything())



















#Scratch--------------------------------------------------------------

new_df <- data.frame(startTime = anytime(working_file$startTime),
                     submitTime = anytime(working_file$submitTime),
                     pendTime = working_file$startTime - working_file$submitTime,
                     numProcs = working_file$numProcessors,
                     queue = working_file$queue)

cpus_per <- data.frame(second = c (1682089142:1682116460))

cores_reserved <- vector(, 27319)
#colnames(cpus_per) <- c("second", "cores_reserved")

cpus_per = cbind(cpus_per, cores_reserved)

for (i in 1:nrow(working_file)){
  `%+=%` = function(e1,e2) eval.parent(substitute(e1 <- e1 + e2))

  cpus_per$cores_reserved[cpus_per$second <= working_file[i, "Event.Time"] & cpus_per$second >= working_file[i, "startTime"]] %+=% 1

  cpus_per$cores_reserved[cpus_per$second <= readfile[i, "Event.Time"] & cpus_per$second >= readfile[i, "startTime"]] %+=% 1

}

table(cpus_per$cores_reserved)
head(new_df)
head(cpus_per)


cpus_per %>%
  arrange(second) %>%
  mutate("time2"=anytime(second, origin="1970-01-01")) %>%
  group_by(interval = cut(time2, breaks=seq(min(time2), max(time2) + 3600, by=3600))) %>%
  #summarize(count=n()) %>%
  View()

cpus_per %>%
  slice(3601:7200) %>%
  mutate("average"=mean(cores_reserved))

my_interval <- 3599

testdf <- data.frame("interval"=seq(from=1, to=nrow(cpus_per), by=3600))

for(i in 1:nrow(testdf)){
  
  cpus_per %>%
    slice(testdf[i,c("interval")] : testdf[i+1,c("interval")]-1)
  
}
















