#Prepare and visualize the LSF logs

#Load libraries
library(anytime)
library(lubridate)
library(readr)
library(dplyr)
library(ggplot2)


#Read in current version of LSF log files
working_file <- read.csv("log_data/lsf_100k.csv")

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
window_start <- as.Date(min(working_file$event_time))
window_end   <- as.Date(max(working_file$event_time))


#Plot CPU usage by day and by queue
p1 <- 
  working_file %>%
  group_by(day_block, queue) %>%
  mutate("cpu_sum"=sum(numProcessors)) %>%
  ungroup() %>%
  select(day_block, queue, cpu_sum) %>%
  unique() %>%
  ggplot(aes(x=day_block, y=cpu_sum)) +
  geom_col(fill="seagreen4") +
  geom_point() +
  facet_wrap(~queue) +
  ggtitle("CPU usage by queue",
          subtitle=paste0(window_start, " to ", window_end)) +
  theme(panel.border=element_rect(fill=NA, color="black"),
        plot.title=element_text(hjust=0.5),
        plot.subtitle = element_text(hjust=0.5)) +
  xlab("") +
  ylab("# CPU Processors")
ggsave("plots/p1.svg", p1, width=5, height=5)


#Plot total CPU usage by day
p2 <- 
  working_file %>%
  group_by(day_block) %>%
  mutate("cpu_sum"=sum(numProcessors)) %>%
  ungroup() %>%
  select(day_block, queue, cpu_sum) %>%
  unique() %>%
  ggplot(aes(x=day_block, y=cpu_sum)) +
  geom_bar(stat="sum", width=0.6, fill="steelblue4") +
  geom_point(size=2)+
  ggtitle("CPU usage by day (all queues)",
          subtitle=paste0(window_start, " to ", window_end)) +
  theme(panel.border=element_rect(fill=NA, color="black"),
        plot.title=element_text(hjust=0.5),
        plot.subtitle = element_text(hjust=0.5),
        legend.position="none") +
  xlab("") +
  ylab("# CPU Processors")
ggsave("plots/p2.svg", p2, width=5, height=5)

#Plot total CPU usage by queue
p3 <- 
  working_file %>%
  group_by(queue) %>%
  mutate("cpu_sum"=sum(numProcessors)) %>%
  ungroup() %>%
  select(queue, cpu_sum) %>%
  unique() %>%
  ggplot(aes(x=cpu_sum, y=reorder(queue, cpu_sum))) +
  geom_bar(stat="sum", width=0.7) +
  geom_point(size=2)+
  ggtitle("CPU usage by queue",
          subtitle=paste0(window_start, " to ", window_end)) +
  theme(panel.border=element_rect(fill=NA, color="black"),
        plot.title=element_text(hjust=0.5),
        plot.subtitle = element_text(hjust=0.5),
        legend.position="none") +
  ylab("") +
  xlab("# CPU Processors")
ggsave("plots/p3.svg", p3, width=5, height=5)


#Plot CPUs by each user (across all queues) 
p4 <- 
  working_file %>%
  group_by(userName) %>%
  mutate("cpu_sum"=sum(numProcessors)) %>%
  select(userName, cpu_sum) %>%
  unique() %>%
  ggplot(aes(x=cpu_sum, y=reorder(userName, cpu_sum))) +
  geom_point() +
  xlab("# CPUs") +
  ylab("")+
  ggtitle("CPU usage by user",
          subtitle=paste0(window_start, " to ", window_end)) +
  theme(panel.border=element_rect(fill=NA, color="black"),
        plot.title=element_text(hjust=0.5),
        plot.subtitle = element_text(hjust=0.5))
ggsave("plots/p4.svg", p4, width=4.3, height=10.75)


#Plot CPUs by each user (highlight the queues)
p5 <- 
  working_file %>%
  group_by(userName, queue) %>%
  mutate("cpu_que_sum"=sum(numProcessors)) %>%
  ungroup() %>%
  group_by(userName) %>%
  mutate("total_cpu_sum"=sum(numProcessors)) %>%
  ungroup() %>%
  select(userName, queue, cpu_que_sum, total_cpu_sum) %>%
  unique() %>%
  ggplot(aes(x=cpu_que_sum, y=reorder(userName, total_cpu_sum), color=queue)) +
  geom_point() +
  xlab("# CPUs") +
  ylab("")+
  ggtitle("CPU usage by user and queue",
          subtitle=paste0(window_start, " to ", window_end)) +
  theme(panel.border=element_rect(fill=NA, color="black"),
        plot.title=element_text(hjust=0.5),
        plot.subtitle = element_text(hjust=0.5))
ggsave("plots/p5.svg", p5, width=6, height=10.75)