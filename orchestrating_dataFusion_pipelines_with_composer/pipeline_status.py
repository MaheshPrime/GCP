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
from airflow.exceptions import AirflowException, AirflowNotFoundException
from typing import Any, Callable,List, Dict, Optional, Sequence, Tuple, TypeVar, Union, cast
from airflow.providers.google.common.hooks.base_google import  GoogleBaseHook
from airflow.providers.google.cloud.utils.credentials_provider import (
    _get_scopes,
    _get_target_principal_and_delegates,
    get_credentials_and_project_id,
)
import google.auth
import google.auth.transport.requests
from time import monotonic,sleep
from datetime import datetime, timezone
import time
import requests
import json
import ast
import urllib3.exceptions
from start_pipeline import Start_Datafusion_Pipeline
logging.basicConfig(level=logging.INFO)

class PipelineStates:
    """Data Fusion pipeline states"""
    PENDING = "PENDING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    RESUMING = "RESUMING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    KILLED = "KILLED"
    REJECTED = "REJECTED"
    
FAILURE_STATES = [PipelineStates.FAILED, PipelineStates.KILLED, PipelineStates.REJECTED]
SUCCESS_STATES = [PipelineStates.COMPLETED]
    
class Datafusion_Pipeline_Status:
    def __init__(self, pipeline_name: str,instance_url: str,pipeline_id: str,namespace: str = "default"):
        self.pipeline_name=pipeline_name
        self.instance_url=instance_url
        self.pipeline_id=pipeline_id
        self.namespace=namespace
        self.timeout= 5 * 60
        self.FAILURE_STATES=FAILURE_STATES
        self.SUCCESS_STATES=SUCCESS_STATES
    
    def _base_url(self,instance_url: str, namespace: str) -> str:
        return os.path.join(instance_url, "v3", "namespaces", quote(namespace), "apps")
    
    def _check_response_status_and_data(self,response, message: str) -> None:
        if response.status == 404:
            raise AirflowNotFoundException(message)
        elif response.status != 200:
            raise AirflowException(message)
        if response.data is None:
            raise AirflowException(
                "Empty response received. Please, check for possible root "
                "causes of this behavior either in DAG code or on Cloud DataFusion side"
            )
    
    def _cdap_request(self, url, method ,body: Optional[Union[List, Dict]] = None):
        return Start_Datafusion_Pipeline(pipeline_name=self.pipeline_name,instance_url=self.instance_url).cdap_request(url, method)
        
    
    def get_pipeline_workflow(self,pipeline_name: str,instance_url: str,pipeline_id: str,
        namespace: str, ) -> Any:
        url = os.path.join(
            self._base_url(instance_url, namespace),
            quote(pipeline_name),
            "workflows",
            "DataPipelineWorkflow",
            "runs",
            quote(pipeline_id),
        )
        response = self._cdap_request(url=url, method="GET")
        self._check_response_status_and_data(
            response, f"Retrieving a pipeline state failed with code {response.status}"
        )
        workflow = json.loads(response.data)
        logging.info("Workflow : ")
        logging.info(workflow)
        return workflow
    
    def  wait_for_pipeline_state(self):
        start_time = monotonic()
        current_state = None
        while monotonic() - start_time < self.timeout:
            try:
                workflow = self.get_pipeline_workflow(pipeline_name=self.pipeline_name,instance_url=self.instance_url,
                    pipeline_id=self.pipeline_id,namespace=self.namespace,
                )
                current_state = workflow["status"]
            except AirflowException:
                pass  # Because the pipeline may not be visible in system yet
            if current_state in self.SUCCESS_STATES:
                return current_state
            if current_state in self.FAILURE_STATES:
                raise AirflowException(
                    f"Pipeline {self.pipeline_name} state {current_state} is not one of {self.SUCCESS_STATES}"
                )
            sleep(30)
    