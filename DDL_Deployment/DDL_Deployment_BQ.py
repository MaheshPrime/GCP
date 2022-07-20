import logging
from airflow import DAG
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from google.cloud import storage
import google.auth
import google.auth.transport.requests
import yaml
from io import StringIO
import argparse, os, sys, shutil, shlex, subprocess,json
from airflow.models import Variable

class Constants(object):
    OWNER = "airflow"
    RETRIES = 0
    START_DATE = datetime(2022, 1, 1)
    
    
	

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
    "BQ_Tables_Creation_Using_DDL_PRD",
     default_args=default_args,
     schedule_interval=None,
     catchup=False,
     description="DAG to Deploy BQ Tables in Prod environment"
)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the google storage bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    print(bucket)
    blob = bucket.blob(destination_blob_name)
    print(blob)
    print("source_file_name")
    print(source_file_name)
    print("destination_blob_name")
    print(destination_blob_name)
    blob.upload_from_filename(source_file_name)
 
    print(
        "File {} uploaded to Storage Bucket with Bob name  {} successfully .".format(
            source_file_name, destination_blob_name
        )
    )

def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))


# This method receives .sql file,.sh file as script names and folder name as a layer
def preexecution(script_names,layer):
    
    for script_name in script_names:
        create_table(script_name,layer)
    print("Successfully executed")
    

#list all folder under dags/ddl_scripts/UnProcessed/<list of folders>
#1. Process each file in <list of folders>
def list_blobs( **kwargs):
    storage_client = storage.Client()
    config_ddl_j =str(Variable.get("ddl_config"))
    config_ddl = json.loads(config_ddl_j)
    layers = config_ddl["layer"]
    airflow_bkt = config_ddl["airflow_bkt"]
    source_bucket = storage_client.bucket(airflow_bkt)

    
    for layer in layers:
        name = 'dags/ddl_scripts/UnProcessed/'+layer +'/'
        scripts = []
        for item in list(source_bucket.list_blobs(prefix=name)):
            scripts.append(str(item))
        print("scripts")
        print(scripts)
        script_name = []
        for item in scripts:
            script_name.append(item.split(',')[1].split('/')[4])
        script_name = list(filter(None, script_name))
        print(script_name)
        print("list_bolbs done for "+ layer)
        preexecution(script_name,layer)
    return "execution_done"

# ddl_config = {
#    "airflow_bkt":"europe-west1-vf-hu-ngbi-dev-2dbcee98-bucket",
#    "project":"vf-hu-datahub",
#    "dataset":"vfhu_dh_lake_edw_integrated_hu_dev_02_v",
#    "collibra_project":"vf-hu-ngbi-dev-gen-02",
#    "collibra_dataset":"vfhu_dh_lake_edw_integrated_hu_dev_02_v",
#    "local_mkt":"hu",
#    "env_var":"dev",
#    "location":"EU",
#    "layer":[
#       "R1_R3",
#       "Collibra"
#    ]
# }

# ddl_script = json.dumps(ddl_script)


#This method is reponsible for Creating Tables
def create_table(script_name,layer):
    #Assign parameter values to variables

    file_name = script_name
    #input_file= 'ddl_scripts/UnProcessed/'+ file_name
    #'gcs/dags/ddl_scripts/UnProcessed/' -> folder structure can be replaced as per your requirement.
    prefix = 'gcs/dags/ddl_scripts/UnProcessed/' + layer +'/' 

    input_file= prefix+file_name
    #load data from airflow admin variable.
    config_ddl_j =str(Variable.get("ddl_config"))
    #convert json to string
    config_ddl = json.loads(config_ddl_j)
    #fetch values for each key
    airflow_bkt = config_ddl["airflow_bkt"]
    project= config_ddl["project"]
    dataset= config_ddl["dataset"]
    location=config_ddl["location"]
    collibra_project = config_ddl["collibra_project"]
    collibra_dataset = config_ddl["collibra_dataset"]
    local_mkt = config_ddl["local_mkt"]
    env_var = config_ddl["env_var"]

    #File Created during runtime
    input_file1 = prefix+'new_' + script_name
    input_file2 = prefix+'run_' + script_name

    # Create variables for input, new file names
    # current_dir=os.chdir('gcs/dags')
    current_dir=os.getcwd()

    # to check where we are:
    # print("Current_Dir :")
    # print(current_dir)
    # print("pwd")
    # print(os.system('pwd'))
    # print("ls command")
    # os.system('ls -ltr')

    file=os.path.join(current_dir,input_file)
    # print("File :")
    # print(file)
    #Merge two file path
    new_file_name=os.path.join(current_dir, input_file1) 
    run_file_name=os.path.join(current_dir, input_file2)
    windows_line_end = b'\r\n'
    unix_line_end = b'\n\t'

    # Check if file exists
    if not(os.path.exists(file)):
        print("\nFile \"" + file + "\" does not exists\n")
        sys.exit(1)

    # # Open the as-is file to read and new file to write
    # # fin=open(file,'rt')
    # # fout=open(new_file_name,'wt')

    # Read given file :
    fin=open(file,'rt', encoding='utf-8',errors='ignore')
    fout=open(new_file_name,'wt', encoding='utf-8',errors='ignore')

    # Replace BQ Project, Dataset, Location
    # Specific layer needs to deploy DDL in specific project:
    # For Collibra we have seperate project i.e compute project 
    if 'Collibra' == layer:
        for line in fin:
            if '<project>'  in line:
                fout.write(' '+line.replace('<project>', collibra_project).replace('<dataset>', collibra_dataset))
            else:
                fout.write(' '+line)  
    # For Rest we have  seperate project i.e datahub project
    else: 
        for line in fin:
            if '<local_mkt>' in line:
                fout.write(' '+line.replace('<local_mkt>', local_mkt).replace('<env_var>', env_var))
            elif '<project>'  in line:
                fout.write(' '+line.replace('<project>', project).replace('<dataset>', dataset))
            elif '{project_id}' or '{dataset_id}' or '{location}' in line:
                fout.write(' '+line.replace('{project_id}', project).replace('{dataset_id}', dataset).replace('{location}', location))
            else:
                fout.write(' '+line) 
    # Close the files
    fin.close()
    fout.close()

    # Read the new file
    with open(new_file_name, 'rb') as open_file:
        file_content = open_file.read()

    # Remove Carriage Return (CR)
    file_content = file_content.replace(windows_line_end, unix_line_end)

    with open(new_file_name, 'wb') as open_file:
        open_file.write(file_content)

    # # Rename new file to the input one
    shutil.move(new_file_name, run_file_name)

    # # Print that the script has been updated
    print("\nScript has been updated successfully")

    # Creating a list of tables
    table_list=[]
    fopen=open(run_file_name,'rt')
    file_data=fopen.readlines()
    
    find_str="CREATE TABLE "
    # print("file_data :",file_data)
    for l in file_data:
        if find_str in l.upper():
            table_list.append(l.split(".")[2].replace("`","").strip("\n"))
    # print("**************************")
    # print("table_list :", table_list)
    # print("**************************")

    #Filter out .sh file alone
    #1. This executes multiple sql statement in single shellscript file.
    
    if  ".sh" not in script_name:
    #split multiple queries to single query for execution
        print('run_file_name :')        
        new_file=open(run_file_name,'rt')
        file_data1=new_file.read()
        file_data1 = file_data1.split(");")

        file_data2 = [item + ");" for item in file_data1]
        file_data2.pop()

        for item in file_data2:
            print(item) 
            print("**************************")
    
        #To execute SQL statement
        find_bq = 'bq query'
        flag = 0
        # Remove existing Query statement
        fWopen=open(run_file_name,'w')
        fWopen.truncate()

        #add "bq query" command for each DDL Script.
        for index,item in enumerate(file_data2):
            if find_bq in str(item):
                flag = 1
            print(flag)
            if 'Collibra' == layer:
                bq_stmt = "\t bq query --project_id={project_id} --use_legacy_sql=false ' ".format(project_id=collibra_project)
            else:
                bq_stmt = "\t bq query --project_id={project_id} --use_legacy_sql=false ' ".format(project_id=project)
            if 0 == flag:
                fWopen=open(run_file_name,'a')
                fWopen.write(bq_stmt)
                fWopen.write(item)
                fWopen.write("\t '\n")
                fWopen.close()
                fWopen=open(run_file_name,'rt')
                # print(fWopen.readlines())

    print("Current Working Directory  before Log file creation")
    dir = os.getcwd()
    print(dir)
    lst = os.listdir()
    print(lst)

    # Specify the log files for successful and errorneous table creation
    name = input_file.split("/")[-1]
    test = name.split(".", 1)
    tabl=(test[0])
    print(tabl)
    logsuffix=script_name
    print(logsuffix)

    #set log file name for Creation log and Error log
    exec_log_file='bq_table_creation_'+logsuffix+'_.log'
    error_log_file='bq_table_error_'+logsuffix+'_.log'
    error_file=open(error_log_file,'w')
    log_file=open(exec_log_file,'w')
    success_table=[]

    # To check log file created as expected.
    print("Current Working Directory  after Log file creation")
    dir = os.getcwd()
    print(dir)
    lst = os.listdir()
    print(lst)

    # Execute the shell
    print("\nExecuting the script")
    exec_shell=shlex.split("sh " + run_file_name) 
    job_result = subprocess.Popen(exec_shell, stdout=log_file, stderr=error_file)

    text = job_result.communicate()[0]
    returncode = job_result.returncode
    print('return code')
    print(returncode)
    
    
    job_result.wait()
    log_file.close()
    error_file.close()

    print("Current Working Directory for Log file")
    dir = os.getcwd()
    print(dir)
    lst = os.listdir()
    print(lst)
    #Location of log file available in server.
    log_path = 'ddl_scripts/logs/'   
    dest_exec = log_path+'bq_table_creation_'+logsuffix+'_.log'
    dest_err = log_path+'bq_table_error_'+logsuffix+'_.log' 

    #Upload log files to the GCS BKT.
    print("Uploading log files")
    upload_blob(bucket_name= airflow_bkt, source_file_name = exec_log_file ,destination_blob_name= dest_exec )
    upload_blob(bucket_name= airflow_bkt, source_file_name = error_log_file ,destination_blob_name= dest_err )
    print("Uploaded")

    
    # Check for errors while execution
    if "error" in open(exec_log_file).read().lower():

        # Create a list of tables that were created successfully
        fopen=open(exec_log_file,'r')
        file_data=fopen.readlines()
        for l in file_data:
            if "Created" in l:
                success_table.append(l.split(".")[2].strip("\n"))

        # Create a list of tables that were not created
        error_table=[item for item in table_list if item not in success_table]

        # Print the list of tables which could not be created
        for table in error_table:
            print("\nError while creating the below table - ")
            print(table)
            print("\nPlease check the log file for more details")

        #sys.exit(1)
    

    elif returncode!=0:
        print("An issue occurred during the script execution")
        print("\nPlease check the log file for more details")

    # move the scripts to the archival folder  airflow_bkt
    else:
        src_path = 'dags/ddl_scripts/UnProcessed/'+ layer + '/' + file_name
        dest_path = 'dags/ddl_scripts/Processed/'+layer+ '/' + file_name
        upload_blob(bucket_name= airflow_bkt,source_file_name = input_file, destination_blob_name = dest_path)
        delete_blob(bucket_name= airflow_bkt,blob_name = src_path)
        print("\n All tables have been created successfully \n")

    # Remove both the log files from server.
    if os.path.exists(exec_log_file):
        os.remove(exec_log_file)
        print("deleted" + exec_log_file)
    if os.path.exists(error_log_file):
        os.remove(error_log_file)
        print("deleted" + error_log_file)
    else:
        print("The file does not exist")

#Dummy Operator
dummy_task = DummyOperator(
    task_id='Standby_Flows',
    retires=3,
	dag=dag,
)

#Python Opertaor
run_this = PythonOperator(
    task_id='Table_Creation',
    provide_context=True,
    python_callable=list_blobs,
    dag=dag,
)