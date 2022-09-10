# GCP - Cloud Function with Cloud Storage Trigger

Overview-
Every day file gets uploaded in specific folder, function needs to pick the uploaded file then add FileName column and place it in a processed Folder, then create a staging table using historical data (pre-existing data) and new data (uploaded data), then creata a final table by performing Query over staging table.

Given Requirement was split it 3 steps:

Step 1: [ Transformation and File Migration ]

1) Every Day sourcefolder was upload with one or more csv file, migrate the new csv files from sourcefolder to sourcefolder/processed folder by applying necessary transformation to add FileName as the last coulumn. 

Problem Found and Resolved:

1) During File Migration CF itself call it own function resulted in running again, this was resolved by adding folder validation check.

Step 2: [ Staging Table Creation ]

1) Create a Staging Table by using data available in the sourcefolder/processed folder.
2) sourcefolder/processed folder contains both historical data and new data.

Step 3: [ Final Table Creation ]

1) Create a Final Table by querying the Staging Table with given condtion.
2) Final Table need to be updated with new data addtionall two columns need to get added which is mentioned in the query execution.

