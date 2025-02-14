#!/bin/bash

USER_DATA_FILE="user_data.sh"

# .env 파일 로드
if [ -f ../.env ]; then
    source ../.env
else
    echo ".env 파일을 찾을 수 없습니다!"
    exit 1
fi

# EC2 인스턴스 생성
INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SECURITY_GROUP_ID" \
    --iam-instance-profile "Name=$IAM_ROLE" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=MyCustomEC2}]" \
    --subnet-id "subnet-06f1e9f77ff80e755" \
    --associate-public-ip-address \
    --user-data file://$USER_DATA_FILE \
    --query "Instances[0].InstanceId" \
    --output text)

if [ -z "$INSTANCE_ID" ]; then
    echo "EC2 인스턴스 생성 실패!"
    exit 1
fi

echo "EC2 인스턴스 생성 완료! ID: $INSTANCE_ID"

# 인스턴스가 실행될 때까지 대기
echo "EC2 인스턴스 시작 대기 중..."
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"

# EC2 인스턴스 재부팅 (user_data 문제 해결)
echo "C2 인스턴스 재부팅 중..."
aws ec2 reboot-instances --instance-ids "$INSTANCE_ID"

# 인스턴스가 준비될 때까지 기다리기
echo "EC2 인스턴스가 준비될 때까지 기다리는 중..."
aws ec2 wait instance-status-ok --instance-ids "$INSTANCE_ID"

# 생성된 인스턴스의 퍼블릭 IP 가져오기
PUBLIC_IP=$(aws ec2 describe-instances \
    --instance-ids "$INSTANCE_ID" \
    --query "Reservations[0].Instances[0].PublicIpAddress" \
    --output text)

if [ "$PUBLIC_IP" == "None" ]; then
    echo "퍼블릭 IP를 가져오지 못했습니다."
    exit 1
fi

echo "EC2 생성 완료! 접속 정보:"
echo "   인스턴스 ID: $INSTANCE_ID"
echo "   퍼블릭 IP  : $PUBLIC_IP"
echo "   SSH 접속   : ssh -i ec2-kga.pem ubuntu@$PUBLIC_IP"


# # user_data.sh 파일을 EC2 인스턴스로 복사 (StrictHostKeyChecking=no 및 UserKnownHostsFile=/dev/null 옵션 추가)
# scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ec2-kga.pem user_data.sh ubuntu@$PUBLIC_IP:/home/ubuntu/

# # SSH를 통해 인스턴스에 접속하여 user_data.sh 실행
# echo "EC2 인스턴스에 접속하여 user_data.sh 실행 중..."

# # EC2 인스턴스에 접속하여 user_data.sh 실행 (StrictHostKeyChecking=no 및 UserKnownHostsFile=/dev/null 옵션 추가)
# ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ec2-kga.pem ubuntu@$PUBLIC_IP << 'EOF'
#     # user_data.sh 실행
#     chmod +x /home/ubuntu/user_data.sh
#     /home/ubuntu/user_data.sh
# EOF

# user_data.sh 파일을 EC2 인스턴스로 복사
# scp -i ec2-kga.pem user_data.sh ubuntu@$PUBLIC_IP:/home/ubuntu/


echo "user_data.sh 실행 완료!"
# user_data.sh 실행까지 약 3분정도 걸리는 듯 합니다.