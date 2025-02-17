# copy ./dags to s3
BUCKET_NAME=the-all-new-bucket

aws s3 cp ./dags s3://$BUCKET_NAME/dags --recursive --exclude "__pycache__/*"
aws s3 cp ./requirements.txt s3://$BUCKET_NAME/requirements.txt
