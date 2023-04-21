import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from google.cloud import storage
import google.auth
import google.auth.transport.requests
import os,json,logging,subprocess
import pandas as pd
from airflow.models import Variable
import google.cloud.logging
from read_arguments import Read_Arguments_File
from start_pipeline import Start_Datafusion_Pipeline
from pipeline_status import Datafusion_Pipeline_Status


class Constants(object):
    OWNER         = "airflow"
    RETRIES       = 0
    START_DATE    = datetime(2020, 1, 1)
    loglevel      = logging.INFO
    pipeline_name = str(Variable.get("pipeline_name"))
    instance_name = str(Variable.get("instance_name"))
    project_id    = str(Variable.get("project_id"))  
    bucket_name   = str(Variable.get("bucket_name")) 
    file_name     = str(Variable.get("file_name"))   
    location      = str(Variable.get("location")) 
    instance_url  = None
    namespace     = "default"
    
logging.basicConfig(level=Constants.loglevel)


# DAG configuration variables
default_args = {"owner": Constants.OWNER,
                "depends_on_past": False,
                "retries": Constants.RETRIES,
                "catchup": False,
                "start_date": Constants.START_DATE
                }
				
def get_auth_token():
    cred, projects = google.auth.default()
    return cred

credentials = get_auth_token()

# Create the DAG
dag = DAG(
    "DataFusion_Pipeline",
     default_args=default_args,
     start_date=datetime(2021, 1, 1),
     schedule_interval=None,
     catchup=False,
     description="DAG to trigger DataFusion Pipelines")

def get_instance_url():
    command = 'export CDAP_ENDPOINT=$(gcloud beta data-fusion instances describe --location={location} \
        --format="value(apiEndpoint)" {instance_name}); echo $CDAP_ENDPOINT'.format(location = Constants.location,\
            instance_name = Constants.instance_name)
    process = subprocess.Popen(command,stdout=subprocess.PIPE, shell=True)
    output, errors  = process.communicate()
    return output.decode().strip()

def data_processing(**kwargs):
    logging.info("Fetching Instance URL for DataFusion....")
    Constants.instance_url=get_instance_url()
    logging.info("Created Instance URL :")
    logging.info(Constants.instance_url)
    logging.info("Fetching Argument File....")
    pl_args = Read_Arguments_File(project_id=Constants.project_id,bucket_name=Constants.bucket_name,file_name=Constants.file_name).read_argument_file()
    logging.info("Arguments File Read Complete")
    logging.info("Arguments : ")
    logging.info(pl_args)
    payload=pl_args
    logging.info("Triggering DataFusion Pipeline....")
    response = Start_Datafusion_Pipeline( pipeline_name=Constants.pipeline_name,instance_url= Constants.instance_url,runtime_args=payload).start_pipeline()
    pipeline_id=response['runId']
    logging.info("Checking Status of the DataFusion Pipeline....")
    status = Datafusion_Pipeline_Status(pipeline_name=Constants.pipeline_name,instance_url= Constants.instance_url,pipeline_id=pipeline_id,namespace=Constants.namespace).wait_for_pipeline_state()
    logging.info(status)
    return status

read_file = PythonOperator(
    task_id='start_pipeline',
    python_callable=data_processing,
    provide_context=True,
    dag=dag,
)
