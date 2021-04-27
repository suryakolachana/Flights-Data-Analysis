# **US-Flights Data for Northwood Airlines**



## Overview:

The objective of this project is to create a data model and ETL flow to gather US Flights Data Information for the Northwood Airlines between January till August for the year 2015 and provide actionable insights in the form of reports or dashboards. 

### Data source Details:

The datasets containing airports, airlines, and flights Information have been provided by the data owners in a shared location. 

[flight-data - Google Drive](https://drive.google.com/drive/folders/18Mkt2Ku3gIxenT-zjYi68kcufpcvNwbv)

## Scope:

1. The scope of the project is to create a data pipeline which will accept the source files, process and clean them, transform as per the need of the final data model and load them in dimension and fact tables. We are going to use Airflow to orchestrate the Databricks jobs to read the source files from google drive, drop and create tables in snowflake, use spark and python to do the transformation and eventually load the processed data into the data model created in snowflake database.

   ![image](https://user-images.githubusercontent.com/48069267/116017703-57ef8500-a60e-11eb-9013-e835a163fd20.png)


### Technology used:

1. Airflow
2. Databricks
3. Python
4. Spark
5. Snowflake 

## Structure:

```
.
├── Data
│   ├── Dimensions
│   │   ├── airlines.csv
│   │   └── airports.csv
│   └── Facts
│       └── US-flights-Data
│           ├── partition-01.csv
│           ├── partition-02.csv
│           ├── partition-03.csv
│           ├── partition-04.csv
│           ├── partition-05.csv
│           ├── partition-06.csv
│           ├── partition-07.csv
│           └── partition-08.csv
├── Databricks
│   ├── DDL-snowflake.ipynb
│   ├── Data to DBFS.ipynb
│   ├── Databricks to Snowflake.ipynb
│   └── Reports.ipynb
├── README.md
├── Snowflake
│   ├── DDL
│   │   └── DDL.sql
│   └── Views
│       ├── Airline_Delays_Info_v.sql
│       ├── Airlines_Unique_Route_Info_V.sql
│       ├── Airport_Delay_Reasons_Info_V.sql
│       ├── Airport_Flight_Cancellations_Info_v.sql
│       ├── Monthly_Total_Flights_Info_v.sql
│       └── On_Time_Airline_Info_v.sql
├── airflow
│   ├── Dockerfile
│   ├── airflow_settings.yaml
│   ├── dags
│   │   └── Flights_Data.py
│   ├── include
│   ├── packages.txt
│   ├── plugins
│   └── requirements.txt
└── images
    ├── DAG.PNG
    ├── DDL-snowflake.html
    ├── Data to DBFS.html
    ├── Databricks to Snowflake.html
    ├── Reports.html
    ├── Snowflake_Tables.PNG
    ├── Snowflake_Views.PNG
    └── airflow.PNG

13 directories, 35 files
```

### Data Pipeline Design

The data pipeline was designed using Apache Airflow. The whole process was segregated in several phases:

- Copying the Data from Google Shared Drive to Databricks File System
- Dropping Snowflake fact and Dimension tables
- Creating the snowflake fact and dimension tables
- Transforming and Loading into Snowflake
- Providing Analytic Reports 

Following is the Airflow dag for the whole process:
![image](https://user-images.githubusercontent.com/48069267/116017728-65a50a80-a60e-11eb-80fa-e2142650ce86.png)

## Development:

#### Airflow:

1. Creating a Databricks Connection by using a Personal Access Token (PAT) to authenticate to the Databricks REST API.

2. The DatabricksSubmitRunOperator makes use of the Databricks Runs Submit API Endpoint and submits a new Spark job run to Databricks.

3. Creating a Databricks job to have the cluster attached and a parameterized notebook as a Task.

4. Defing the Dag to orchestrate couple of spark jobs.

5. Error Handling: When using the operator, any failure in submitting the job, starting or accessing the cluster, or connecting with the Databricks API will propagate to a failure of the airflow task and generate an error message in the logs. If there is a failure in the job itself, like in one of the notebooks, that failure will also propagate to a failure of the airflow task.  Please check below.

   ![image](https://user-images.githubusercontent.com/48069267/116017778-8d946e00-a60e-11eb-9815-c7d0ec301b32.png)

6. The Pipeline is scheduled to run on a Yearly Basis but we can change into monthly or daily depending on the Business needs.

#####   Databricks: 

1. Standard cluster

2. Create a Databricks application using a notebook using Python and spark.

3. The Below Notebook applications performs the following tasks:
      1. Data to DBFS will load the Datasets from Google shared location to DBFS. 
      2. DDL-snowflake will drop and create the tables in snowflake.
   3. Databricks to Snowflake Extracts the CSV files and pushes the transformed data to snowflake.
   4. Reports provides actionable insights.

#####   Snowflake: 

1.  INTERVIEW_WH warehouse. 

2. USER_NW database.

3. Tables.

    Dimension Tables: 

   1.  Airlines
   2. Airports

​	    Facts Tables: 

​           1. Flights

4. Load data from the external stage into corresponding tables.
5. All data persisted within Snowflake.

#### Docker:

To run this DAG use the Astronomer CLI to get an Airflow instance up and running locally:

 1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)

 2. Clone this repo somewhere locally and navigate to it in your terminal

 3. Initialize an Astronomer project by running `astro dev init`
  
 4. Start Airflow locally by running `astro dev start`

 5. Navigate to localhost:8080 in your browser and you should see the DAG.

    
## Sample Reports:

=======

### 1. Total number of flights by airline at BWI Airport on a monthly basis for year 2015

![image](https://user-images.githubusercontent.com/48069267/115457627-cbe6f300-a1f2-11eb-97ac-85b486f0ac68.png)


### 2. On time percentage of each airline for the year 2015

![image](https://user-images.githubusercontent.com/48069267/115457915-11a3bb80-a1f3-11eb-8e73-383628a9520a.png)


### 3. Airlines with the largest number of delays for year 2015

![image](https://user-images.githubusercontent.com/48069267/115457948-1cf6e700-a1f3-11eb-8bab-d3f882896afe.png)


### 4. Major Cancellation reasons by airport for year 2015


![image](https://user-images.githubusercontent.com/48069267/115457988-27b17c00-a1f3-11eb-96df-25207a213d45.png)


### 5. Delay reasons by airport

![image-20210420154433091](C:\Users\vamsi\AppData\Roaming\Typora\typora-user-images\image-20210420154433091.png)


### 6. Airline with the most unique routes

![image](https://user-images.githubusercontent.com/48069267/115458077-44e64a80-a1f3-11eb-9612-5f90132b0530.png)

### Addressing Other Scenarios

1. The Pipeline is broken into multiple stages to monitor for failures, deadlocks and long running tasks.
2. Managing the metadata such as time of run, end to end time taken and failure reasons.

### Snowflake Architecture and Benefits:

### Architecture:
 1. Database Storage: The database storage layer holds all the data loaded into snowflake, including structured and semi-structured data. snowflake manages all aspects of how     the data is stored: organization, file size, structure,compression,metadata and statistics. This storage layer runs independently of compute resources.
 2. Compute Layer:The compute layer is made up of virtual warehouses that execute data processing tasks required for queries. Each virtual warehouse (or cluster) can access        all the data in the storage layer, then work independently, so the warehouses do not share, or compete for compute resources. This enables nondisruptive, automatic scaling,      which means that while queries are running, compute resources can scale without the need to redistribute or rebalance the data in storage layer.
 3. Cloud services: The cloud services layer uses ANSI SQL and coordinates the entire system. It eliminates the need for manual data warehouse management and tuning. Services      in this layer include:
            1. Authentication
            2. Infrastructure management
            3. metadata management
            4. query parsing and optimization
            5. access control
            
### Benefits:
  1. The snowflake architecture decouples the storage and compute functions, which means organizations that have high storage demands but less need for CPU cycles, or vice         versa, don't have to pay for an integrated bundle that requires them to pay for both.
  3. Performance and speed - The elastic nature of the cloud means if you want to load data faster, or run a high volume of queries, you can scale up your virtual warehouse         to take advantage of extra compute resources. Afterward, you can scale down the virtual warehouse and pay for only the time you used.
  4. Concurrency and accessibility - With a traditional data warehouse and a large number of users or use cases, you could experience concurrency issues (suchs as delays or           failures) when too many queries compete for resources. Snowflake addresses concurrency issues with its unique multicluster architecture: Queries from one virtual                 warehouse never affect the queries from another, and each virtual warehouse can scale up or down as required. 
     
### Databricks Architecture and Benefits:

### Architecture:
  1. Workspace - A workspace is an environment for accessing all of your Databricks assets. A workspace organizes objects (notebooks, libraries, dashboards, and experiments)         into folders and provides access to data objects and computational resources.
  2. Interface - The Databricks UI provides an easy-to-use graphical interface to workspace folders and their contained objects, data objects, and computational resources.
  3. Data Management - The objects that hold the data on which you can perform analytics are below:
                      1. Databricks File System (DBFS) - Contains Directories, files (data files, libraries and images).
                      2. Database
                      3. Tables
                      4. Metastore
  4. Computation Management - This section describes concepts that you need to know to run computations in Databricks.

      1. Cluster - A set of computation resources and configurations on which you run notebooks and jobs. There are two types of clusters: all-purpose and job.You create an            all-purpose cluster using the UI, CLI, or REST API. You can manually terminate and restart an all-purpose cluster. Multiple users can share such clusters to do                  collaborative interactive analysis.The Databricks job scheduler creates a job cluster when you run a job on a new job cluster and terminates the cluster when the job is          complete. You cannot restart a job cluster.
      2. Pool - A set of idle, ready-to-use instances that reduce cluster start and auto-scaling times. When attached to a pool, a cluster allocates its driver and worker              nodes from the pool. If the pool does not have sufficient idle resources to accommodate the cluster’s request, the pool expands by allocating new instances from the              instance provider. When an attached cluster is terminated, the instances it used are returned to the pool and can be reused by a different cluster.
      3. Databricks runtime - The set of core components that run on the clusters managed by Databricks. Databricks offers several types of runtimes:
      Databricks Runtime includes Apache Spark but also adds a number of components and updates that substantially improve the usability, performance,                                  and security of big data analytics.
      4. Job - A non-interactive mechanism for running a notebook or library either immediately or on a scheduled basis.
      5. Workload - Databricks identifies two types of workloads subject to different pricing schemes: data engineering (job) and data analytics (all-                                  purpose).Data engineering An (automated) workload runs on a job cluster which the Databricks job scheduler creates for each workload. Data analytics An (interactive)            workload runs on an all-purpose cluster. Interactive workloads typically run commands within a Databricks notebook. However, running a job on an existing all-purpose            cluster is also treated as an interactive workload.
      6. Execution context - The state for a REPL environment for each supported programming language. The languages supported are Python, R, Scala, and SQL. 
### Benefits:
  1. Databircks provides a zero-management cloud platform built around spark that delivers
                          1. fully managed Spark clusters.
                          2. An interactive workspace for exploration and visualization.
                          3. A production pipeline scheduler.
                          4. A platform for powering Spark-based applications.
