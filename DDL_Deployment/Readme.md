# DDL Deployment using Airflow

1. Upload the code to the dags folder in GCS bucket. 
2. Create a folder structure inside dags folder in ddlscript/Unprocessed/<layer_name>/<.sql files or .sh files>
3. Create Airflow variables with key name of ddl_config