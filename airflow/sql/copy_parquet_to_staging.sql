COPY {{ schema_name }}.{{ table_name }}
FROM 's3://{{ s3_bucket }}/{{ car_name }}/{{ ds.year }}/{{ ds.month }}/{{ ds.day }}/{{ s3_object }}'
FORMAT AS PARQUET;