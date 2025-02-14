#!/bin/bash

# 시스템 업데이트
sudo apt-get update -y

# Python3 및 pip 설치
sudo apt-get install -y python3-pip

# aws-cli 설치
sudo apt-get install -y awscli

# boto3와 pandas 설치
pip3 install boto3 pandas

# S3에서 파일 다운로드
aws s3 cp s3://test-kga/test.py /home/ubuntu/test.py

# 다운로드된 파일 존재 여부 확인
if [ -f /home/ubuntu/test.py ]; then
    echo "test.py 파일 다운로드 완료!"
else
    echo "test.py 파일 다운로드 실패!"
    exit 1
fi

# Python 스크립트 실행
python3 /home/ubuntu/test.py
