library(prophet)
library(dplyr)
library(forecast)
library(anytime)
library(lubridate)
library(ggplot2)
library(readr)

#Read in transformed log file
working_file <- 
  read_csv("combined_time_series.csv")


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
  ungroup() %>%
  
  #Make a column of weekday corresponding to date:
  mutate("weekday"=wday(date, label = TRUE))


working_file <- 
  working_file %>%
  mutate("ds"=anytime(second),  #ds column is required for forecasting. This contains date + timestamp
         "date"=date(ds)) %>%   #This will contain just the date, no timestamp (used for grouping)
  
  #Calculate the utilization per hour_block
  #This gives mean cpus/gpus/mem in a given hour block (note: it's not not 'per hour')
  group_by(queue, hour_block) %>%
  mutate("mean_cpus_per_block"=mean(cpus), 
         "mean_gpus_per_block"=mean(gpu),
         "mean_mem_per_block"=mean(mem)) %>%
  ungroup() %>%
  
  #Calculate the utilization per hour_block for each DATE
  #mean_cpus_per_block :           how many cpus were used from 10-11 ranging from jan1-jan3
  #mean_cpus_per_block_per_date :  ho wmany cpus were used from 10-11 on jan1, how many on jan2, how many on jan3...
  group_by(queue, hour_block, date) %>%     #Aggregate data by hour block
  mutate("mean_cpus_per_block_per_date"=mean(cpus),  #Note this is not cpus per hour, but rather total cpus in a given hour block
         "mean_gpus_per_block_per_date"=mean(gpu),
         "mean_mem_per_block_per_date"=mean(mem)) %>%
  ungroup() 


 #Select which utilization type and queue we want to forecast
 utilization_choice <- "mean_cpus_per_block_per_date" 
 queue_choice <- "compbio"
 forecast_increment_hour <- 48
 
 #Depending on utilization choice, select y-axis label of resulting plot
 my_ylabel <- "reserved_null"
 if(utilization_choice=="mean_cpus_per_block_per_date"){
   my_ylabel <<- "Mean hourly reserved CPUs"
 } else if(utilization_choice=="mean_gpus_per_block_per_date"){
   my_ylabel <<- "Mean hourly reserved GPUs"
 } else if(utilization_choice=="mean_mem_per_block_per_date") {
   my_ylabel <<- "Mean hourly reserved memory"
 }
 
  
 mydf <- 
   working_file %>%
   mutate("y"= switch(utilization_choice, 
                      "mean_cpus_per_block_per_date" = mean_cpus_per_block_per_date,
                      "mean_gpus_per_block_per_date" = mean_gpus_per_block_per_date,
                      "mean_mem_per_block_per_date"  = mean_mem_per_block_per_date)) %>%
   filter(queue==queue_choice) %>%
   select(hour_block, queue, ds, y, date) %>%
   unique() 
 
 #Prophet forecast model
 m <- prophet(mydf,
              growth="linear")
 
 #Make a dataframe with future time points desired for forecasting
 future <- make_future_dataframe(m, 
                                 periods=forecast_increment_hour,
                                 freq=3600,  #1=sec, 60=min, 3600=hour
                                 include_history=TRUE)
 
 #Use prophet model to forecast into desired future space
 forecast <- 
   predict(m, future) %>%
   left_join(., 
         mydf %>% select(ds, date),
         by="ds")
   

 p <-
    ggplot(forecast, aes(x = ds, y = yhat)) +
    geom_line(color = "blue") +
    geom_ribbon(aes(ymin = yhat_lower, ymax = yhat_upper), alpha = 0.3) +
    geom_point(data=mydf, aes(x=ds, y=y), alpha=0.1) +
    xlab("") +
    ylab(my_ylabel) +
    scale_x_datetime(date_labels = "%m/%d", date_breaks = "1 day") +
    geom_vline(xintercept = as.numeric(as.POSIXct(max(mydf$date)+0.4)), linetype=2) +
    theme(panel.border = element_rect(fill=NA, color="black"),
          axis.text.x = element_text(angle=45, hjust=1),
          plot.title=element_text(hjust=0.5)) +
   ggtitle("HPC utilization")
 
 
 ggsave("model/forecast.svg", 
        p,
        width=5,
        height=2)
 
 
 
 
 
 
 
 
 #####################
 #Loop test
 #Select which utilization type and queue we want to forecast
 utilization_choice <- "mean_mem_per_block_per_date" 
 queue_choices <- c("cryo_core", "compbio", "standard", "heavy_io", "gpu", "large_mem")
 forecast_increment_hour <- 48
 
 cpu_list <- list()
 gpu_list <- list()
 mem_list <- list()
 
 for(i in seq_along(queue_choices)){
   
   queue_choice <- queue_choices[i]
   
   #Depending on utilization choice, select y-axis label of resulting plot
   my_ylabel <- "reserved_null"
   if(utilization_choice=="mean_cpus_per_block_per_date"){
     my_ylabel <<- "Mean hourly reserved CPUs"
   } else if(utilization_choice=="mean_gpus_per_block_per_date"){
     my_ylabel <<- "Mean hourly reserved GPUs"
   } else if(utilization_choice=="mean_mem_per_block_per_date") {
     my_ylabel <<- "Mean hourly reserved memory"
   }
   
   mydf <- 
     working_file %>%
     mutate("y"= switch(utilization_choice, 
                        "mean_cpus_per_block_per_date" = mean_cpus_per_block_per_date,
                        "mean_gpus_per_block_per_date" = mean_gpus_per_block_per_date,
                        "mean_mem_per_block_per_date"  = mean_mem_per_block_per_date)) %>%
     filter(queue==queue_choice) %>%
     select(hour_block, queue, ds, y, date) %>%
     unique() 
   
   
   #Prophet forecast model
   m <- prophet(mydf,
                growth="linear")
   
   #Make a dataframe with future time points desired for forecasting
   future <- make_future_dataframe(m, 
                                   periods=forecast_increment_hour,
                                   freq=3600,  #1=sec, 60=min, 3600=hour
                                   include_history=TRUE)
   
   #Use prophet model to forecast into desired future space
   forecast <- 
     predict(m, future) %>%
     left_join(., 
               mydf %>% select(ds, date),
               by="ds")
   
   
   p <-
     ggplot(forecast, aes(x = ds, y = yhat)) +
     geom_line(color = "blue") +
     geom_ribbon(aes(ymin = yhat_lower, ymax = yhat_upper), alpha = 0.3) +
     geom_point(data=mydf, aes(x=ds, y=y), alpha=0.1) +
     xlab("") +
     ylab("Mem") +
     scale_x_datetime(date_labels = "%m/%d", date_breaks = "1 day") +
     geom_vline(xintercept = as.numeric(as.POSIXct(max(mydf$date)+0.8)), linetype=2) +
     theme(panel.border = element_rect(fill=NA, color="black"),
           axis.text.x = element_text(angle=45, hjust=1),
           plot.title=element_text(hjust=0.5)) +
     ggtitle(paste0("utilization on ", queue_choice))
   
   mem_list[[i]] <- p
   
   
 }
 
 names(cpu_list) <- queue_choices
 names(gpu_list) <- queue_choices
 names(mem_list) <- queue_choices
 
 library(gridExtra)
 cpu_plot <- do.call(grid.arrange, c(cpu_list, ncol=1))
 gpu_plot <- do.call(grid.arrange, c(gpu_list, ncol=1))
 mem_plot <- do.call(grid.arrange, c(mem_list, ncol=1))
 
 
 p_grid <- 
   grid.arrange(cpu_plot, gpu_plot, mem_plot, ncol=3)
 
 ggsave("model/grid_forecast.svg", 
        p_grid,
        width=12, height=12)
 
 
 
 
 




