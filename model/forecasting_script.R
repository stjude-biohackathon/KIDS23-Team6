library(prophet)
library(dplyr)
library(forecast)
library(anytime)

#Read in transformed log file
working_file <- 
  read_csv("C:/Users/wchar/Desktop/hackathon/KIDS23-Team6/per_second_cpu_counts_4M.csv")


working_file <- 
  working_file %>%
  mutate("ds"=anytime(second),
         "y"=cpus) 

m <- prophet(working_file)
future <- make_future_dataframe(m, 
                                periods=3,
                                freq="day",
                                include_history = TRUE)
forecast <- predict(m, future)
plot(m, forecast)
