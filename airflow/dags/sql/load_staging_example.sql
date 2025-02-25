COPY staging.tb_posts
FROM 's3://{s3-bucket}/{s3-object}/post_data/'
IAM_ROLE 'arn:aws:iam::910534606964:role/DE_3_Redshift_S3_ReadAccess_Role'
FORMAT AS PARQUET;

COPY staging.tb_comments
FROM 's3://{s3-bucket}/{s3-object}/comment_data/'
IAM_ROLE 'arn:aws:iam::910534606964:role/DE_3_Redshift_S3_ReadAccess_Role'
FORMAT AS PARQUET;

COPY staging.tb_sentences
FROM 's3://{s3-bucket}/{s3-object}/sentence_data/'
IAM_ROLE 'arn:aws:iam::910534606964:role/DE_3_Redshift_S3_ReadAccess_Role'
FORMAT AS PARQUET;

COPY staging.tb_keywords
FROM 's3://{s3-bucket}/{s3-object}/classified/'
IAM_ROLE 'arn:aws:iam::910534606964:role/DE_3_Redshift_S3_ReadAccess_Role'
FORMAT AS PARQUET;