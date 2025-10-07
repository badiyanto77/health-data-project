"""
===============================================================================
    Lambda Name : trigger_mwaa_dag_lambda
    Author      : Bagus Adiyanto
    Created On  : 2025-10-07
    Last Updated: 2025-10-07

    Summary:
    --------
    This AWS Lambda function programmatically triggers an Apache Airflow DAG 
    running in Amazon Managed Workflows for Apache Airflow (MWAA). The workflow 
    includes the following key steps:

    1. Request a temporary CLI token from the MWAA environment.
    2. Establish an HTTPS connection to the MWAA web server.
    3. Construct and send a CLI command (`dags trigger <dag_name>`).
    4. Parse the response and decode the output for logging or downstream use.

    Notes:
    ------
    - MWAA environment name is defined in `mwaa_env_name`.
    - DAG name to trigger is defined in `dag_name`.
    - Requires IAM permissions to call `mwaa:create_cli_token`.
    - Returns the decoded CLI output from MWAA.
    - Designed for event-driven orchestration (e.g., CloudWatch, API Gateway).
===============================================================================
"""

import boto3
import http.client
import base64
import ast
mwaa_env_name = 'health_data_project_mwaa'
dag_name = 'health_data_project_dag'
mwaa_cli_command = 'dags trigger'
​
client = boto3.client('mwaa')
​
def lambda_handler(event, context):
    # get web token
    mwaa_cli_token = client.create_cli_token(
        Name=mwaa_env_name
    )
    
    conn = http.client.HTTPSConnection(mwaa_cli_token['WebServerHostname'])
    payload = mwaa_cli_command + " " + dag_name
    headers = {
      'Authorization': 'Bearer ' + mwaa_cli_token['CliToken'],
      'Content-Type': 'text/plain'
    }
    conn.request("POST", "/aws_mwaa/cli", payload, headers)
    res = conn.getresponse()
    data = res.read()
    dict_str = data.decode("UTF-8")
    mydata = ast.literal_eval(dict_str)
    return base64.b64decode(mydata['stdout'])