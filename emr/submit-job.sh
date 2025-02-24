#!/bin/bash

# UTF-8 환경 설정 추가
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8

# 입력값 검증
if [ "$#" -ne 2 ]; then
    echo "Usage: ./submit-job.sh <keyword> <date (YYYY-MM)>"
    exit 1
fi

KEYWORD=$1
DATE=$2

# DATE에서 연도와 월 분리
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# UTF-8 인코딩 문제 해결 (iconv 제거)
ENCODED_KEYWORD=$(printf "%s" "$KEYWORD")

# 설정
S3_SCRIPT_PATH="s3://the-all-new-bucket/py/spark_performance_test.py"

# 실행할 EMR 클러스터 ID
CLUSTER_ID="j-95UQX2W48CL1"

# Spark 작업 제출
aws emr add-steps --cluster-id $CLUSTER_ID --steps Type=Spark,Name="Performance Test",ActionOnFailure=CONTINUE,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.executor.memory=2g,\
--conf,spark.driver.memory=2g,\
--conf,spark.executor.cores=2,\
$S3_SCRIPT_PATH,\
--year,$YEAR,\
--month,$MONTH,\
--day,$DAY,\
--car_name,$ENCODED_KEYWORD\
]

echo "Spark job submitted to EMR cluster $CLUSTER_ID with parameters: KEYWORD=$ENCODED_KEYWORD, YEAR=$YEAR, MONTH=$MONTH, day = $DAY"
