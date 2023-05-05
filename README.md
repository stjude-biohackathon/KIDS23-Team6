# KIDS23-Team6 - Visualization and prediction of HPC resource utilization

This purpose of this project is to visualize the historical resource usage of the HPCF resources and predict the future usage. Our efforts focus on CPU, GPU, and memory utilization and prediction.

The aim is to summarize historical data, provide useful forecasts of utilization, and ultimately to provide a dashboard which displays these results interactively. We provide: 

- Historical usage trends.
- Current usage statistics of the cluster queues.
- A forecast/prediction of future usage to inform HPRC and users.

# So, what's the need?

The HPCF team has gotten multiple requests regarding the usage of the cluster as a whole as well as for individual queues and departments. There is a tool which provides some visualiztion capabilties, but it's overly complicated for everyday use and for researchers who cannot dedicate time to understand all the intricacies.

So, we're designing a simple and intuitive dashboard where researchers can view data relevant to them, their departments, or the institution as a whole.

Furthermore, the HPCF team needs to be able to accurately predict future resource usage so that we can plan for expansions and refreshes of the infrastructure. This project provides information and visualization tools that will assist both researchers and the HPCF team in accomplishing these goals.

# About this project

This project was built with:

-	Tidyverse, Dplyr R packages – Packages for data science and data manipulation
-	PostgreSQL – Relational database management system
-	Docker – containerization software
-	LSTM – AI, deep learning model for time-series data prediction
-	R Shiny – R package for developing web applications

# Preprocessing Pipeline

This project changed from a data processing to a data engineering and reduction effort once we realized how large the data-set is. The following is an illustration of the tranformation and reduction of data that had to take place before we could start visualization and machine-learning efforts:

<img width="1179" alt="image" src="https://user-images.githubusercontent.com/43145254/236521567-dd7976cd-8e57-4c7c-98fe-4c326fedf647.png">

The following is a visual representing the transformation on the original data records to create a time-series for visualization and training purposes:
<img width="1113" alt="image" src="https://user-images.githubusercontent.com/43145254/236573325-e2374ec6-7bb6-4af5-bce7-5a76e92d2785.png">

## Data files and inputs

The input data we used comes primarily from LSF log files and LSF command outputs. Here is a sample of the command outputs:

<img width="964" alt="image" src="https://user-images.githubusercontent.com/43145254/236540070-fa00d7f1-6608-440a-a5bd-afa5981ef78f.png">

The data in the LSF log files is quite large indeed. We have logs dating back to September of 2021 the total size of which is around 360GB. The data files are comma-delimited and have over 130 columns per line with over a hundred million lines total. Here is a sample of the data:

<img width="774" alt="image" src="https://user-images.githubusercontent.com/43145254/236546771-3e32ef74-688a-4808-9385-47af26d854b1.png">

