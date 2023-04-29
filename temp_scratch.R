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
















