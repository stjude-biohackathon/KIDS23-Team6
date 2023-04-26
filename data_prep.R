#Some code to prepare the LSF logs

#Load libraries
library(anytime)
library(readr)
library(dplyr)
library(ggplot2)


#Read in current version of LSF log files
working_file <- read.csv("lsf_small.csv")


working_file <-
  working_file %>%
  mutate("startTime2"=anytime(startTime),
         "submitTime2"=anytime(submitTime),
         "pendTime"=startTime-submitTime,
         "endTime"=startTime2 + runTime,
         "endTime2"=anytime(endTime))

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
















