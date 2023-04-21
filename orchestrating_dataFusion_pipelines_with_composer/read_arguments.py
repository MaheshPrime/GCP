import json,logging
from google.cloud import storage


class Read_Arguments_File:
    def __init__(self, project_id: str,bucket_name: str,file_name: str):
        self.project_id=project_id
        self.bucket_name=bucket_name
        self.file_name=file_name
        
    def read_argument_file(self):
        storage_client = storage.Client(project=self.project_id)
        source_bucket = storage_client.bucket(self.bucket_name)
        source_blob = source_bucket.blob( self.file_name)
        data = json.loads(source_blob.download_as_string())
        return data