import logging

import boto3

emr_client = boto3.client('emr')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    bucket = 'simple-s3-jy'
    script_key = "cleanse_tlc_data_script.py"
    
    cluster_id = 'j-306T8WVE71CJR'
    step = {
        'Name': 'Data Cleansing Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                f's3://{bucket}/{script_key}'
            ]
        }
    }

    response = emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])
    return response