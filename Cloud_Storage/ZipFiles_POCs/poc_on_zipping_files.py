from google.cloud import storage
from zipfile import ZipFile, ZipInfo,ZIP_DEFLATED, io, os
from datetime import datetime


# The ID of your GCS bucket my-bucket/logs/dag_id=DataFusion_Pipeline
bucket_name = "my-bucket"

# The ID of your GCS object
parent_prefix = 'logs/dag_id=DataFusion_Pipeline' 
delimiter =None
child_prefix = []

storage_client = storage.Client()
source_bucket = storage_client.get_bucket(bucket_name)
blobs = source_bucket.list_blobs(prefix=parent_prefix)
for blob in blobs:
    if blob.name.endswith("/"): continue
    child_prefix.append(blob.name)
print(child_prefix)

for prefix in child_prefix:
    archive = io.BytesIO()
    with ZipFile(archive, 'w',compression=ZIP_DEFLATED ) as zip:
        storage_client = storage.Client()
        source_bucket = storage_client.get_bucket(bucket_name)
        blobs = source_bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            print(blob.name)
            filename = blob.name #.replace('/', '_') 
            data = source_bucket.blob(filename).download_as_string()
            zip_file = ZipInfo(filename)
            zip.writestr(zip_file,data)
        zip.close()

    archive.seek(0)

    now = datetime.now()
    dt_string = now.strftime("%d-%m-%Y_%H:%M:%S")
    object_name = "_".join(filename.split("/")[2:4])
    print(object_name+"_"+dt_string+".zip")
    print(os.getcwd())
    
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.Blob(object_name, bucket)
    blob.upload_from_file(archive, content_type='application/zip')
    
