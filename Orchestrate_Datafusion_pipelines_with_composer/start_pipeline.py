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
from urllib.parse import quote, urlencode
from airflow.exceptions import AirflowException
from typing import Any, Callable,List, Dict, Optional, Sequence, Tuple, TypeVar, Union, cast
from airflow.providers.google.common.hooks.base_google import  GoogleBaseHook
from airflow.providers.google.cloud.utils.credentials_provider import (
    _get_scopes,
    _get_target_principal_and_delegates,
    get_credentials_and_project_id,
)
import google.auth
import google.auth.transport.requests

from datetime import datetime, timezone
import time
import requests
import json
import ast
import urllib3.exceptions
logging.basicConfig(level=logging.INFO)

class Start_Datafusion_Pipeline:
    def __init__(self,pipeline_name:str,instance_url:str,namespace:str="default",runtime_args=None):
        self.pipeline_name=pipeline_name
        self.instance_url=instance_url
        self.namespace=namespace
        self.runtime_args=runtime_args
    
    def get_auth_token(self):
        cred, projects = google.auth.default()
        auth_req = google.auth.transport.requests.Request()
        if cred.valid is False:
            cred.refresh(auth_req)
        else:
            cr_dt = cred.expiry
            now_dt = datetime.now(tz=timezone.utc)
            elapsed = now_dt - cr_dt
            elapsed_s = elapsed.total_seconds()
            if elapsed_s < 500:
                cred.refresh(auth_req)
        return cred.token

    
    def cdap_request(self,  url: str, method: str, body: Optional[Union[List, Dict]] = None) -> google.auth.transport.Response:
        request = google.auth.transport.requests.Request()
        temp = self.get_auth_token()
        token = "Bearer " + temp
        js =""
        headers = {"Authorization": token, "Accept": "application/json"}
        payload = json.dumps(body) if body else None
        response = request(method=method, url=url, headers=headers, body=payload)
        return response
        
    def start_pipeline(self):
        url = os.path.join(self.instance_url, "v3","namespaces",quote(self.namespace),"start", )
        self.runtime_args = self.runtime_args or {}
        body = [
            {
                "appId": self.pipeline_name,
                "programType": "workflow",
                "programId": "DataPipelineWorkflow",
                "runtimeargs": self.runtime_args,
            }
        ]
        response = self.cdap_request(url=url, method="POST", body=body)
        logging.info(response.status)
        logging.info("Data : ")
        response_json = json.loads(response.data)
        logging.info(response_json)
        if response_json[0]['statusCode'] != 200:
            raise AirflowException(f"Starting a pipeline failed with code {response_json[0]['statusCode']}")
        return response_json[0]
        