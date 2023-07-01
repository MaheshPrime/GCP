from google.cloud import storage
from zipfile import ZipFile, ZipInfo,ZIP_DEFLATED, io, os,is_zipfile
from datetime import datetime
from pathlib import Path
import glob

# The ID of your GCS bucket
bucket_name = "my-bucket"

#The ID of your GCS object
prefix    ='Fold1/Fold2' 
delimiter =None

#### Create zip file
archive = io.BytesIO()
with ZipFile(archive, 'w', compression=ZIP_DEFLATED) as zip:
    storage_client = storage.Client()
    source_bucket = storage_client.get_bucket(bucket_name)
    blobs = source_bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        print(blob.name)
        if blob.name.endswith("/"): continue
        filename = blob.name
        data = source_bucket.blob(filename).download_as_string()
        zip_file = ZipInfo(filename)
        zip.writestr(zip_file,data)
    zip.close()
    archive.seek(0)
now = datetime.now()
dt_string = now.strftime("%d-%m-%Y_%H:%M:%S")
object_name = "Zipped_file_"+dt_string+".zip"
#-------------------------------------------------------------------------------------------------------------

#### download to local
blob.download_to_filename(object_name)
#-------------------------------------------------------------------------------------------------------------
#### upload archive ZIP file to any bucket
target_bucket = "TARGET_BUCKET"
bucket = storage_client.get_bucket(bucket_name)
blob = storage.Blob(object_name, bucket)
blob.upload_from_file(archive, content_type='application/zip')
#-------------------------------------------------------------------------------------------------------------

### Download and extract ZipFile file from GCS bucket to local :
object_name = 'Fold1test2.zip'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
blob = storage.Blob(object_name, bucket)
object_bytes = blob.download_as_bytes()
archive = io.BytesIO()
archive.write(object_bytes)
with ZipFile(archive, 'r') as zip_archive:
    zip_archive.extractall("/home/maheshwaran_kannan/extracted_folder")
#-------------------------------------------------------------------------------------------------------------

### Download zip file from GCS bucket and place it in local.
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob('Fold1test2.zip')
zip_file_path = 'prd/Fold1test2_local.zip'
my_file = Path(zip_file_path)
# create folder path if not exists
if not my_file.exists():
    path = zip_file_path.replace(zip_file_path.split("/")[-1],"")
    os.mkdir(path)
with open(zip_file_path, 'wb') as file:
    print(file)
    blob.download_to_file(file)
#-------------------------------------------------------------------------------------------------------------

### upload all files from a local folder to GCS bucket
# target_bucket = "blubbet7"
GCS_CLIENT = storage.Client()
def upload_from_directory(directory_path: str, dest_bucket_name: str, dest_blob_name: str):
    rel_paths = glob.glob(directory_path + '/**', recursive=True)
    print(rel_paths)
    bucket = GCS_CLIENT.get_bucket(dest_bucket_name)
    for local_file in rel_paths:
        if local_file.endswith("/"): continue
        print(local_file.split("/"))
        remote_path = dest_blob_name + local_file.split("/")[-1]
        # remote_path = dest_blob_name
        print(remote_path)
        print(local_file)
        if os.path.isfile(local_file):  
            blob = bucket.blob(remote_path)       # archive/archive.zip
            blob.upload_from_filename(local_file) # /home/maheshwaran_kannan/archive/archive.zip

upload_from_directory("/home/maheshwaran_kannan/archive",bucket_name,"archive/")
#-------------------------------------------------------------------------------------------------------------

#### download & extract zip from GCS and upload to GCS :
def zipextract(bucketname, zipfilename_with_path):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucketname)

        destination_blob_pathname = zipfilename_with_path
        
        blob = bucket.blob(destination_blob_pathname)
        zipbytes = io.BytesIO(blob.download_as_string())  # download zipfile from source path

        if is_zipfile(zipbytes):                                                        # check it is a zip file:
            with ZipFile(zipbytes, 'r') as myzip:                                       # open zip file in read mode
                for contentfilename in myzip.namelist():                                # iterate over zip files to extract data
                    contentfile = myzip.read(contentfilename)                           # read content in files
                    zip_filename = zipfilename_with_path.split("/")[-1].split(".")[0]   # generate zip filename
                    blob = bucket.blob(zip_filename + "/" + contentfilename)            # dest loc: mybucket/file/<extracted files>
                    blob.upload_from_string(contentfile)                                # upload data

zipextract("mybucket", "path/file.zip") # if the file is gs://mybucket/path/file.zip
#-------------------------------------------------------------------------------------------------------------