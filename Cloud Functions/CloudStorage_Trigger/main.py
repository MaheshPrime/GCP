from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
from datetime import datetime

now = datetime.now()
class Constants:
    project_id = "data-exchange-01"
    dataset_id = "panthersdataset"
    Staging_table = "STAGING_TABLE"
    Final_table = "FINAL_TABLE"
    table_id_prefix = f"{project_id}.{dataset_id}."
    folder = "sourcefolder"
    

def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    print("Blob {} deleted.".format(blob_name))

def schema_generator(jsonfile):
    data = pd.read_json(jsonfile)
    schema =""
    print(data)
    for index,item in data.iterrows():
        schema +=  data['name'][index] + " " +data['type'][index] + ","
    schema = ','.join(schema.split(",")[:-1:])
    return schema

def staging_table_creation(jsonfile):
    schema = schema_generator(jsonfile)
    client = bigquery.Client()
    Staging_table = Constants.Staging_table
    table_id = Constants.table_id_prefix+ Staging_table
    query = """
    DROP TABLE IF EXISTS """+table_id+""";
    CREATE TABLE  IF NOT EXISTS """+table_id+"""  (""" + schema + """ );  """
    print(query)
    query_job = client.query(query)  # Make an API request.
    query_job.result()
    return table_id

def dataload_on_stagingtable(table_id,jsonfile,bucket_name):
    data = pd.read_json(jsonfile)
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix = Constants.folder + "/Processed")
    schema=[bigquery.SchemaField(data['name'][index], data['type'][index]) for index in data.index ]
    client = bigquery.Client()
    for blob in blobs:
        print(blob.name)
        newpath = "gs://" + bucket_name + "/" + blob.name
        job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  )
        uri = newpath 

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        ) 
        load_job.result()  
    print("Data landed in Staging Table :", table_id)
    destination_table = client.get_table(table_id)
    print("Loaded {} rows in the {} ".format(destination_table.num_rows,table_id))
    return table_id

def Final_table_creation(jsonfile):
    schema = schema_generator(jsonfile)
    client = bigquery.Client()
    Final_table = Constants.Final_table
    table_id = Constants.table_id_prefix+ Final_table
    query = """
    CREATE TABLE  IF NOT EXISTS """+table_id+"""
    (""" + schema + """ ,Brand STRING,Channel STRING);  """
    print(query)
    query_job = client.query(query)  # Make an API request.
    query_job.result()
    return table_id

def final_table_schema_generator(jsonfile):
    data = pd.read_json(jsonfile)
    schema =""
    for index,item in data.iterrows():
        schema +=  data['name'][index] +  ","
    schema = ','.join(schema.split(",")[:-1:])
    return schema


def dataload_on_finaltable(table_id,jsonfile):
    schema = final_table_schema_generator(jsonfile)
    client = bigquery.Client()
    Final_table = Constants.Final_table
    Staging_table = Constants.Staging_table
    stg_table_id =  Constants.table_id_prefix + Staging_table
    final_table_id = Constants.table_id_prefix + Final_table
    query = """  MERGE """+final_table_id+""" T
USING (
  SELECT *,
case
when upper(FileName) like 'CHICOS%' then 'CHS'
when upper(FileName) like 'COTR%' then 'CHS'
when upper(FileName) like  'SOMA%' then 'SOMA'
when upper(FileName) like 'WHBM%' then 'WHBM'
END 
as Brand,
 case
when upper(FileName) like 'COTR%' then 'OTR'
else 'DCOM'
END
 as Channel

from  """+stg_table_id+""") S
ON T.FileName = S.FileName

WHEN NOT MATCHED THEN
  INSERT("""+schema+""",Brand,Channel)
  VALUES("""+schema+""",Brand,Channel)"""
    query_job = client.query(query)  # Make an API request.
    query_job.result()
    print("Data landed in Final Table :", final_table_id)
    destination_table = client.get_table(table_id)
    print("Loaded {} rows in the {} ".format(destination_table.num_rows,table_id))

def hello_gcs(event, context, jsonfile = 'schema.json'):
    file = event
    print(f"Processing file: {file['name']}.")
    Folder = Constants.folder
    if "Processed" not in file['name'] and Folder in file['name'] :
        print("event :", event)
        print("context :", context)
        dt_string = now.strftime("%Y%m%d")
        print("date and time =", int(dt_string))	
        curr_date =  int(dt_string)

        client = storage.Client()
        Folder = Constants.folder
        bucket = client.get_bucket(file['bucket'])
        bucket_name = file['bucket']
        blobs = bucket.list_blobs(prefix=Folder)
        print("Current date :", curr_date)
        
        filelist = []
        for blob in blobs:
            filename = (blob.name).split("/")[-1]
            print(filename)
            if len(filename) != 0 and Folder in blob.name and "modified" not in blob.name and "Processed" not in blob.name:
                file_corename = filename.split(".")[0]
                print("File_Corename",file_corename)
                filedate = int(file_corename.split("_")[-2].replace('-', ''))
                print("File_Date ",filedate)

                if  curr_date > filedate : # filter Condition
                    filelist.append(blob.name)

        print(filelist)
        # del filelist[0]
        Bucket = file['bucket']
        for file in filelist:
            file = file.split("/")[-1]
            path = 'gs://'+ Bucket + '/'+ Folder +'/' + file
            print(path)
            blob_name = Folder +'/' + file

            FileName = path.split("/")[-1]
            print("FileName: ", FileName)

            csv_input = pd.read_csv(path)
            csv_input['FileName'] = FileName
            newfolder = Folder+"/Processed"
            newpath = 'gs://'+ Bucket + '/'+ newfolder +'/' + file

            print(newpath)
            csv_input.to_csv(newpath, line_terminator="\n", index = False) # Load Data
            delete_blob(bucket_name, blob_name)

        table_id = staging_table_creation(jsonfile)
        staging_table_id = dataload_on_stagingtable(table_id,jsonfile,bucket_name)
        final_table_id = Final_table_creation(jsonfile)
        dataload_on_finaltable(final_table_id,jsonfile)

        return "Successfully Executed !!!"

    else:
        print("Path Mismatch")
        # bucket_name = "teampanthers_bucket"
        # table_id = staging_table_creation(jsonfile)
        # staging_table_id = dataload_on_stagingtable(table_id,jsonfile,bucket_name)
        # final_table_id = Final_table_creation(jsonfile)
        # dataload_on_finaltable(final_table_id,jsonfile)
        # print("Path Mismatch")
