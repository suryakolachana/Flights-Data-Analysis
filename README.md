# **US-Flights Data for Northwood Airlines**



## Overview:

The objective of this project is to create a data model and ETL flow to gather US Flights Data Information for the Northwood Airlines between January till August for the year 2015 and provide actionable insights in the form of reports or dashboards. 

### Data source Details:

The datasets containing airports, airlines, and flights has been provided by the data owners in a shared location and asked to load these datasets into the cloud environments (Databricks and Snowflake) so that they can develop reports and gather insights.

[flight-data - Google Drive](https://drive.google.com/drive/folders/18Mkt2Ku3gIxenT-zjYi68kcufpcvNwbv)

## Scope:

1. The scope of the project is to create a data pipeline which will accept the source files, process and clean them, transform as per the need of the final data model and load them in dimension and fact tables. We are going to read the source files from google drive to Databricks file system, and then load from DBFS using spark and python to create a data pipeline, and eventually load the processed and transformed data into the data model created in snowflake database.

### Technology used:

1. Databricks
2. Spark SQL
3. Pyspark
4. Snowflake 

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
│   ├── Databricks_Snowflake_Data_Pipeline.ipynb
│   ├── Flight_Data_to_DBFS.ipynb
│   └── Snowflake_Reports.ipynb
├── README.md
├── Snowflake
│   ├── Create_Tables
│   │   └── Create_Tables.sql
│   └── Views
│       ├── Airline_Delays_Info_v.sql
│       ├── Airlines_Unique_Route_Info_V.sql
│       ├── Airport_Delay_Reasons_Info_V.sql
│       ├── Airport_Flight_Cancellations_Info_v.sql
│       ├── Monthly_Total_Flights_Info_v.sql
│       └── On_Time_Airline_Info_v.sql
└── images
    ├── Snowflake_Tables.PNG
    └── Snowflake_Views.PNG

9 directories, 23 files
```

### Data Pipeline Design

The data pipeline was designed using Spark. The whole process was segregated in several phases:

- Creating the dimension tables
- Loading the dimension tables
- Creating the facts tables
- Loading the fact tables
- Performing data quality checks

## Development:

#####   Databricks: 

1. Standard cluster

2. Create a Databricks application using a notebook using Python and spark.

3. The application should read in the provided CSV files as data frames
      1. Flight_Data_to_DBFS will load the Datasets from shared location to DBFS. 
      2. Databricks_Snowflake_Data_Pipeline Extracts the CSV files and pushes to snowflake.
   3. Snowflake_Reports provides actionable insights.

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



## Reports:


### 1. Total number of flights by airline and airport on a monthly basis for year 2015

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


## Data Quality:

```
assert(airlinesDF.count() == airlinestableDF.count())
```

```
assert(airportsDF.count() == airportstableDF.count())
```

```
assert(flightsDF.count() == flightstableDF.count())
```



## Scheduling:

#### The Data pipeline can be run on a monthly basis.

This can be handled using the Apache Airflow or Databricks.

