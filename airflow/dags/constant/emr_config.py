def generate_step(year, month, day, car_name):
    return [
        {
            "Name": f"Process {car_name} at {year}-{month}-{day}",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "s3://the-all-new-bucket/py/process_text.py",
                    "--year",
                    year,
                    "--month",
                    month,
                    "--day",
                    day,
                    "--car_name",
                    car_name,
                ],
            },
        }
    ]


JOB_FLOW_OVERRIDES = {
    "Name": "EMR Test",
    "ReleaseLabel": "emr-7.7.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "InstanceType": "m4.large",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceCount": 1,
            },
            {
                "Name": "Worker nodes",
                "InstanceType": "m4.large",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceCount": 2,
            },
        ],
        "Ec2SubnetId": "subnet-06f1e9f77ff80e755",
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "LogUri": "s3://the-all-new-logs/elasticmapreduce",
    "Tags": [{"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"}],
    "VisibleToAllUsers": True,
    "JobFlowRole": "DE_3_EMR_Instance_Role",
    "ServiceRole": "DE_3_EMR_Service_Role",
}
