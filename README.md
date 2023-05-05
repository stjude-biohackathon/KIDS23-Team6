# KIDS23-Team6 - Visualization and prediction of HPC resource utilization

This purpose of this project is to visualize the historical resource usage of the HPCF resources and predict the future usage. Our efforts focus on CPU, GPU, and memory utilization and prediction.

The aim is to summarize historical data, provide useful forecasts of utilization, and ultimately to provide a dashboard which displays these results interactively. 

- Historical usage trends.
- Current usage of the cluster queues.
- A prediction of future usage of the cluster queues.

# About this project

This project was built with:

-	Tidyverse, Dplyr R packages – Packages for data science and data manipulation
-	PostgreSQL – Relational database management system
-	Docker – containerization software
-	LSTM – AI, deep learning model for time-series data prediction
-	R Shiny – R package for developing web applications

# Pipeline

This project changed from a data processing to a data engineering and reduction effort once we realized how large the data-set is. The following is an illustration of the tranformation and reduction of data that had to take place before we could start visualization and machine-learning efforts:

<img width="1179" alt="image" src="https://user-images.githubusercontent.com/43145254/236521567-dd7976cd-8e57-4c7c-98fe-4c326fedf647.png">
