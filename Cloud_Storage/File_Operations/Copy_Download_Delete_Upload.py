from google.cloud import storage
import os

def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name   = "your-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))


def copy_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    """Copies a blob from one bucket to another with a new name."""
    # bucket_name = "your-bucket-name" [source-bucket-name]
    # blob_name   = "your-object-name" [Source-object-name]
    # destination_bucket_name = "destination-bucket-name"
    # destination_blob_name   = "destination-object-name"

    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)  # source_bucket_client
    source_blob = source_bucket.blob(blob_name)         # source_blob_name
    destination_bucket = storage_client.bucket(destination_bucket_name) # destination_bucket_client

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name )

    print(
        "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )


def file_name(blobs,year = ''):
    file_list = []
    for i in blobs:
        #print(i)
        #add year under the if statement to filter the .csv file based on the year
        #path = (str(i).split(",")[1])
        if '.Success' in str(i) and '/success/' in str(i) and '20220304' not in str(i) and '20220307'  not in str(i) and '20220308' not in str(i):
            file_list.append((str(i).split(',')[1]).strip())
            #print(file_list)
        
    print("*************************")
    print(file_list)
    print("*************************")
    
    return file_list


def folder_name(blobs,bucket,bucket_name):
    file_list = file_name(blobs)
    print(file_list)
    #spliting_filename_location(file_list,bucket,bucket_name)
    #CURRENCY/success/CURRENCY_20210728.Success
    for successpath in file_list:
        copy_blob(
        bucket_name='my-bucket-test',
        blob_name= successpath,
        destination_bucket_name='my-bucket-test',
        destination_blob_name= successpath.split("/")[0] + '/SuccessFileArchive/'+successpath.split("/")[-1] )
        print(successpath.split("/")[0] + '/SuccessFileArchive/'+successpath.split("/")[-1] )
        delete_blob(bucket_name='my-bucket-test',blob_name=successpath)
        
   

client = storage.Client()
bucket_name = "my-bucket-test"
bucket = client.get_bucket(bucket_name)
blobs  = client.list_blobs(bucket_name)
fold_name = folder_name(blobs,bucket,bucket_name)