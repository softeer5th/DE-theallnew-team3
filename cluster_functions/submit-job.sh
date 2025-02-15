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

# UTF-8 인코딩 강제 적용
KEYWORD=$(echo $KEYWORD | iconv -f utf-8 -t utf-8)

# 설정
S3_SCRIPT_PATH="s3a://the-all-new-bucket/nlp_emr.py"
INPUT_S3_PATH="s3a://the-all-new-bucket/${KEYWORD}/${YEAR}/${MONTH}"
OUTPUT_S3_PATH="s3a://the-all-new-bucket/${KEYWORD}/${YEAR}/${MONTH}/raw_output"

# 실행할 EMR 클러스터 ID
CLUSTER_ID="j-T5PKZ70I6R2I"

# Spark 작업 제출
aws emr add-steps --cluster-id $CLUSTER_ID --steps Type=Spark,Name="TextTransformJob",ActionOnFailure=CONTINUE,\
Args=[\
--deploy-mode,client,\
--master,yarn,\
--conf,spark.yarn.submit.waitAppCompletion=true,\
--conf,spark.executor.memory=4g,\
--conf,spark.driver.memory=4g,\
--conf,spark.executor.cores=2,\
$S3_SCRIPT_PATH,\
$INPUT_S3_PATH,\
$OUTPUT_S3_PATH\
]

echo "Spark job submitted to EMR cluster $CLUSTER_ID with parameters: KEYWORD=$KEYWORD, YEAR=$YEAR, MONTH=$MONTH"
