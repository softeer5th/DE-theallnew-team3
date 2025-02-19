# ./deploy.sh {python_file_name}
# upload to s3

BUCKET_NAME=the-all-new-bucket

aws s3 cp $1 s3://$BUCKET_NAME/py/$(basename $1)
