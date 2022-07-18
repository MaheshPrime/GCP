import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
import csv 
import json

class SplitRow(beam.DoFn):
    def process(self, element):
        return [element.split(',')]

class lst2csv(beam.DoFn):
    def process(self, element):
        return [','.join(element)]

class Filter1(beam.DoFn):
    #Create a STATUS Field and add data either pass or fail based on MATH marks 
    def process(self, element):
        if element[1] == "Math":
            element.append("STATUS")
        elif int(element[1])>50:
            element.append("PASS")
        else :
            element.append("FAIL")
        return [element] 
 
count = "start"
lst =[]
min_dict={}
final = ""
class csv2json(beam.DoFn):
    def process(self,csvFilePath):
        global count
        global header
        jsonArray = []
        global lst,final
        min_dict={}
        lst=[]         
        # First row considered as Headers storing on list for KEYS on json.
        if "start" == count:
            for item in range(len(csvFilePath)):
                min_dict[csvFilePath[item]]=""
            count = "end"
            header = csvFilePath
        #Converting each list of record to json. 
        else:
            for item in range(len(csvFilePath)):
                min_dict[header[item]]=csvFilePath[item]
            lst.append(min_dict)
            final = lst 
        return final



def run():
    # Static runtime Args
    argv = [
      '--temp_location=gs://temporary-bkt/dataflow_test_temp \
       --runner=DataflowRunner'
   ]
   #instantiate pipeline
    p1 = beam.Pipeline(argv=argv)
    #Extract Data
    grocery = (p1
           | "Read from Text" >> beam.io.ReadFromText("gs://gcp-dataflow-bkt/TEST_DATA.csv")
           | "split the record" >> beam.ParDo(SplitRow())
           )
    #Transformation on Data
    filtering = (grocery           
           | 'Simple Trasformation on Adding Status Field' >>  beam.ParDo(Filter1()))

    interim = (filtering 
                | "csv2json_ " >> beam.ParDo(csv2json()))

    #Load Data to GCS
    output_data = (interim
        |'Write to text'>> beam.io.WriteToText('gs://gcp-dataflow-bkt/output.csv') )

    #Load Data to BQ     
    table_spec = bigquery.TableReference(
            projectId='gcp-dataflow-bkt',
            datasetId='dataflow-BQ-dataset',
            tableId='Batch_table'
        )
    #Scehma Defnition
    table_schema1 = 'Gender:STRING,Math:STRING,Reading:STRING,Writing:STRING,STATUS:STRING'
    
    output_data = (interim | beam.io.WriteToBigQuery(
    table_spec,
    schema=table_schema1,
    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    #write_disposition:
    #WRITE_EMPTY    -> Write on table only when table is empty else operation will fail.
    #WRITE_TRUNCATE -> Truncate the destination table then do write operation.
    #WRITE_APPEND   -> Append to the destination table. 
    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)) 
    #create_disposition
    #CREATE_IF_NEEDED -> create table if not exists,
    #CREATE_NEVER -> table does not exists then write fails.

    #Executes the pipeline -> p1
    p1.run()


if __name__ == '__main__':
   run()