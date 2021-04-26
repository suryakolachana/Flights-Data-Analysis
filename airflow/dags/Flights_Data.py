from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging

# Default settings applied to all tasks
default_args = {
    'owner': 'suryakolachana',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

#function definitions for different tasks
def start():
    """This is the definition of the start task."""
    logging.info("Execution Started")

def end():
    """This is te definition of the end task"""
    logging.info("Execution Ended")

#define Cluster params for submit run operator
new_cluster = {
    'spark_version': '7.3.x-scala2.12',
    'num_workers': 2,
    'node_type_id': 'i3.xlarge',
}

#define Bash Task params for submit run operator
Bash_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {
    'notebook_path': '/Users/venkatasurya605@gmail.com/Data to DBFS',
  },
}

#define Drop Table Task params for submit run operator
drop_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {
    'notebook_path': '/Users/venkatasurya605@gmail.com/DDL-snowflake',
    'base_parameters':{
        "AIRLINES": "DROP TABLE IF EXISTS AIRLINES",
        "AIRPORTS": "DROP TABLE IF EXISTS AIRPORTS",
        "FLIGHTS" : "DROP TABLE IF EXISTS FLIGHTS"
    }
  },
}

#define Create Table Task params for submit run operator
create_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {
    'notebook_path': '/Users/venkatasurya605@gmail.com/DDL-snowflake',
    'base_parameters':{
        "AIRLINES": "CREATE TABLE IF NOT EXISTS AIRLINES(IATA_CODE STRING, AIRLINE STRING)",
        "AIRPORTS": "CREATE TABLE IF NOT EXISTS AIRPORTS(IATA_CODE STRING,AIRPORT STRING, CITY STRING, STATE STRING, COUNTRY STRING, LATITUDE NUMBER, LONGITUDE NUMBER)",
        "FLIGHTS": "CREATE TABLE IF NOT EXISTS FLIGHTS(YEAR NUMBER,MONTH NUMBER, DAY NUMBER, DAY_OF_WEEK NUMBER, AIRLINE STRING, FLIGHT_NUMBER STRING,TAIL_NUMBER STRING,ORIGIN_AIRPORT STRING,DESTINATION_AIRPORT STRING,SCHEDULED_DEPARTURE STRING,DEPARTURE_TIME STRING,DEPARTURE_DELAY NUMBER,TAXI_OUT NUMBER,WHEELS_OFF STRING,SCHEDULED_TIME NUMBER,ELAPSED_TIME NUMBER,AIR_TIME NUMBER,DISTANCE NUMBER,WHEELS_ON NUMBER,TAXI_IN NUMBER,SCHEDULED_ARRIVAL NUMBER,ARRIVAL_TIME STRING,ARRIVAL_DELAY STRING,DIVERTED NUMBER,CANCELLED NUMBER,CANCELLATION_REASON STRING,AIR_SYSTEM_DELAY NUMBER,SECURITY_DELAY NUMBER,AIRLINE_DELAY NUMBER,LATE_AIRCRAFT_DELAY NUMBER,WEATHER_DELAY NUMBER)"
    }
  },
}

#define Spark Task params for submit run operator
Spark_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {
    'notebook_path': '/Users/venkatasurya605@gmail.com/Databricks to Snowflake',
  },
}

#define Snowflake Reports Task params for submit run operator
Reports_task_params = {
    'new_cluster': new_cluster,
    'notebook_task': {
    'notebook_path': '/Users/venkatasurya605@gmail.com/Reports',
  },
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('NW-Airline-Analytics',
         start_date=datetime(2016, 1, 1),
         schedule_interval='@yearly',  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False 
         ) as dag:

    #Start task definition
    start = PythonOperator(
        task_id = "start",
        dag = dag,
        python_callable = start
    )

    #Copy files task definition 
    Copy_files_to_DBFS = DatabricksSubmitRunOperator(
        task_id = 'copy_to_dbfs',
        databricks_conn_id = 'databricks',
        new_cluster = new_cluster,
        json = Bash_task_params,
        )
    
    #drop snowflake tables task definition 
    drop_snowflake_tables = DatabricksSubmitRunOperator(
        task_id = 'drop_snowflake_tables',
        databricks_conn_id = 'databricks',
        json=drop_task_params,
        )

    #create snowflake tables task definition 
    create_snowflake_tables = DatabricksSubmitRunOperator(
        task_id = 'create_snowflake_tables',
        databricks_conn_id = 'databricks',
        json=create_task_params,
        )
    
    #Datbricks to Snowflake task definition 
    databricks_snowflake = DatabricksSubmitRunOperator(
        task_id = 'Databricks_to_Snowflake',
        databricks_conn_id = 'databricks',
        json=Spark_task_params,
        )
    
    #Snowflake Reports task definition 
    final_reports = DatabricksSubmitRunOperator(
        task_id = 'Snowflake_Reports',
        databricks_conn_id = 'databricks',
        json=Reports_task_params,
        )

    #End task definition
    end = PythonOperator(
        task_id = "end",
        dag = dag,
        python_callable = end
    )
    
    start  >> Copy_files_to_DBFS >> drop_snowflake_tables >> create_snowflake_tables >> databricks_snowflake >> final_reports >> end
