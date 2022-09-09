from google.cloud import bigquery
from google.cloud import storage
import pandas as pd

def hello_world(request):
    request_json = request.get_json()
    print( "Request  :",request_json)
    Bucket = request_json['bucket'] #"<Bkt_Name>"
    Folder = request_json['folder'] #"<Folder/to/path>"
    client = storage.Client()
    bucket = client.get_bucket(Bucket)
    blobs = bucket.list_blobs(prefix=Folder)


    filelist = []
    for blob in blobs:
        if Folder in blob.name and "2022" in blob.name and "modified" not in blob.name: # filter Condition
            filelist.append(blob.name)

    del filelist[0]
    print(filelist)

    for file in filelist:
        # print(file)
        file = file.split("/")[-1]
        path = 'gs://'+ Bucket + '/'+ Folder +'/' + file
        print(path)

        FileName = path.split("/")[-1]
        print("FileName: ", FileName)

        csv_input = pd.read_csv(path) # Extract Data
        # print("Raw File : ")
        # print(csv_input)
        # # print("\n")

        # print("Modified File : ")
        csv_input['FileName'] = FileName # Transforamtion
        # print(csv_input)

        newfolder = Folder+"_modified"
        newpath = 'gs://'+ Bucket + '/'+ newfolder +'/' + file
        # csv_input.to_csv(newpath)  
        
        
        csv_input.to_csv(newpath, line_terminator="\n", index = False) # Load Data
        # print("NewLocation : ", newpath)
        # # print("\n")

        

    return f'Successfully Executed!!!'
