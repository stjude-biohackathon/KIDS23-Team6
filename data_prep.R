#Some code to prepare the LSF logs

#Load libraries
library(anytime)
library(lubridate)
library(readr)
library(dplyr)
library(ggplot2)


#Read in current version of LSF log files
working_file <- read.csv("lsf_small.csv")

#Modify some time columns to be POSIXct format rather than Epoch time
working_file <-
  working_file %>%
  mutate("pend_time"=startTime-submitTime,
         "end_time"=anytime(startTime + runTime),
         "start_time"=anytime(startTime),
         "submit_time"=anytime(submitTime),
         "event_time"=anytime(Event.Time))


hour_blocks <- sprintf("%02d-%02d", 0:23, 1:24)

#Group by 1 hour intervals and queues, then count processors and memory used
working_file %>%
  mutate("interval"=cut(working_file$submit_time, breaks = seq(min(working_file$submit_time), max(working_file$end_time) + 3600, by = 3600)),
         "hour"=hour(interval)) %>%
  group_by(hour, queue) %>%
  summarize(processors_used = sum(numProcessors),
            mem_used = sum(maxRMem))


hour_blocks <- sprintf("%02d-%02d", 0:23, 1:24)
test <- 
  working_file %>%
  mutate(hour = hour(event_time),
         hour_block = case_when(hour == 0 ~ "00-01",
                                TRUE ~ paste(sprintf("%02d", hour), sprintf("%02d", hour + 1), sep = "-"))) %>%
  select(event_time, hour, hour_block, numProcessors, maxRMem, startTime, runTime) %>%
  group_by(hour_block) %>%
  mutate("cpus_by_hour"=sum(numProcessors)) %>%
  ungroup()





  group_by(hour_block) %>%
  summarize(total_processors_used = sum(numProcessors)) %>%
  right_join(data.frame(hour_block = hour_blocks), by = "hour_block") %>%
  mutate(total_processors_used = ifelse(is.na(total_processors_used), 0, total_processors_used)) %>%
  View()










# group the jobs into 1 hour intervals and calculate the total number of processors used in each interval
proc_df <-
  working_file %>%
  group_by(interval = cut(working_file$submitTime2, breaks = seq(min(working_file$submitTime2), max(working_file$endTime2) + 3600, by = 3600))) %>%
  summarize(processors_used = sum(numProcessors))

proc_df %>%
  ggplot(aes(x=interval, y=processors_used)) +
  geom_point() +
  ggtitle(paste0("Processors Used from",min(proc_df$interval), "to", max(proc_df$interval)))



#





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
















