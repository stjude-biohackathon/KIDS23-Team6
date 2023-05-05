library(prophet)
library(dplyr)
library(forecast)
library(anytime)
library(lubridate)
library(ggplot2)
library(readr)

#Read in transformed log file
working_file <- 
  read_csv("per_second_cpu_counts_4M.csv")


#Define the 1-hour blocks which each time frame will be assigned to
hour_blocks <- sprintf("%02d-%02d", 0:23, 1:24)


#Assign each row into an hour block (of which there are 24 total):
working_file <- 
  working_file %>%
  mutate("hour_block"=case_when(hour(anytime(second)) == 0 ~ "00-01", #Define which of the 24 1-hour time blocks a given event belongs to
                                TRUE ~ paste(sprintf("%02d", hour(anytime(second))),
                                             sprintf("%02d", hour(anytime(second)) + 1), 
                                             sep = "-")))


working_file <- 
  working_file %>%
  mutate("ds"=anytime(second),  #ds column is required for forecasting. This contains date + timestamp
         "date"=date(ds)) %>%   #This will contain just the date, no timestamp (used for grouping)
  
  #Calculate the utilization per hour_block (total and mean)
  #This gives mean cpus/gpus/mem in a given hour block (note: it's not not 'per hour')
  group_by(queue, hour_block) %>%
  mutate("mean_cpus_per_block"=mean(cpus), 
         "totl_cpus_per_block"=sum(cpus),
         "mean_gpus_per_block"=mean(gpu),
         "totl_gpus_per_block"=sum(gpu),
         "mean_mem_per_block"=mean(mem),
         "totl_mem_per_block"=sum(mem)) %>%
  ungroup() %>%
  
  
  #Calculate the utilization per hour_block for each DATE
  #[mean/total]_cpus_per_block :           how many cpus were used from 10-11 ranging from jan1-jan3
  #[mean/total]_cpus_per_block_per_date :  how many cpus were used from 10-11 on jan1, how many on jan2, how many on jan3...
  group_by(queue, hour_block, date) %>%     #Aggregate data by hour block
  mutate("mean_cpus_per_block_per_date"=mean(cpus),  #Note this is not cpus per hour, but rather total cpus in a given hour block
         "totl_cpus_per_block_per_date"=sum(cpus),
         "mean_gpus_per_block_per_date"=mean(gpu),
         "totl_gpus_per_block_per-date"=sum(gpu),
         "mean_mem_per_block_per_date"=mean(mem),
         "totl_mem_per_block_per_date"=sum(mem)) %>%
  ungroup() 


mean_test <- working_file %>%
  select(queue, mean_cpus_per_block, hour_block) %>%
  unique() %>% #Otherwise you will be plotting hundreds of thousands of redundant points
  ggplot(aes(x=hour_block, y=mean_cpus_per_block, fill=queue)) +
  geom_point() +
  geom_col(color="black") +
  facet_wrap(~queue, ncol=1) +
  theme(legend.position = "none")

totl_test <- working_file %>%
  select(queue, totl_cpus_per_block, hour_block) %>%
  unique() %>% #Otherwise you will be plotting hundreds of thousands of redundant points
  ggplot(aes(x=hour_block, y=totl_cpus_per_block, fill=queue)) +
  geom_point() +
  geom_col(color="black") +
  facet_wrap(~queue, ncol=1) +
  theme(legend.position = "none")

p <- totl_test +  mean_test

ggsave("totl_and_mean_cpus_by_queue.svg",
       p,
       width = 15,
       height = 15)
