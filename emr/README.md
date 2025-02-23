## EMR에서 텍스트 processing 코드 잘 작동하는지 확인하기

1. 텍스트 전처리 코드 s3에 업로드하기  
```bash  
./deploy.sh {python_file_name}
```  
2. AWS EMR 생성하기  

3. submit-job.sh 안에 생성한 emr 클러스터 ID로 변경하기    

4. 스크립트로 spark job submit하기  
```bash  
./submit-job.sh '싼타페' '2025-01' 
``` 

5. 결과확인  
- s3://{bucket_name}/{차종}/{년}/{월}/post_data  


| post_id            |       title               |     author |    article                | timestamp|like_cnt|dislike_cnt|view_cnt|comment_cnt|car_name| source|  
+--------------------+----------------------------------+------------------------------------+----------------------------------+--------------------------------+  
|ee21afb1-cd4e-4d6...| 너무 멋진 현대자동차 신형 그랜...|  Virtual Now| 너무 멋진 현대자동차 신형 그랜...|1736038839|     248|          0|   46216|         31|  그랜저|youtube|  


- s3://{bucket_name}/{차종}/{년}/{월}/comment_data  
+--------------------+----------------+----------------------------------+----------+--------+-----------+--------------------+  
|          comment_id|          author|                           content| timestamp|like_cnt|dislike_cnt|             post_id|  
+--------------------+----------------+----------------------------------+----------+--------+-----------+--------------------+  
|2e8a56ad-5479-4fe...|       @영원-o6i|               앞으론 사륜 사세요ᆢ|1739262966|       1|          0|7092fdf6-bcdd-495...|  
|f392fb81-f3b9-41f...|    @radiotracer|  옆에 제설함이 떡하니 있는데. ...|1739435766|       1|          0|7092fdf6-bcdd-495...|  


- s3://{bucket_name}/{차종}/{년}/{월}/sentence_data  
+--------------------+----+----------------------------------+--------------------+----------+  
|         sentence_id|type|                              text|             post_id|comment_id|  
+--------------------+----+----------------------------------+--------------------+----------+  
|0884a9ec-76be-4b8...|post|      그랜저 vs 제네시스 g80 주...|cc10c0e8-6264-463...|      NULL|  
|00e13739-d64f-45a...|post|    80년대에 그랜저 타면 '진짜 ...|215b197d-be59-4ec...|      NULL|  


클러스터 상태 확인  
```bash  
aws emr describe-cluster --cluster-id "j-000000"   
```   
 
step 상태 확인  
```bash  
aws emr describe-step --cluster-id "j-000000" --step-id "s-0000000"  
```   


## 로컬에서 Spark 사용해서 s3 데이터 읽고 쓰기 
- hadoop-aws-3.2.4.jar, aws-java-sdk-bundle-1.11.901.jar 파일이 $SPARK_HOME/jars에 있어야 합니다.  

- test.py 코드로 결과를 볼 수 있습니다.  
```bash
spark-submit test.py s3a://the-all-new-bucket/싼타페/2025/01/raw_output    
```  


## spark data skew 확인하기
실제데이터가 아니기에 2025/11/11에 데이터를 넣어두었고, sentence 데이터는 2025/02/20의 데이터를 뽑는 것으로 하드코딩 해두었습니다.   
- s3://the-all-new-bucket/Tucson/2025/11/11 : 총 4.5GB   
- s3://the-all-new-bucket/Tucson/2025/11/12 : 총 320MB   
test 코드: test_dataskew.py (s3에 배포돼있음)  

1. aws EMR 클러스터 생성하기  
2. submit-job.sh에 클로스터 id 넣기, spakr 옵션 추가 등  
3. submit-job 코드 실행하기  
```bash 
./submit-job.sh "Tucson" "2025-11-12" #60분 내외 걸림 
./submit-job.sh "Tucson" "2025-11-11" #7분 내외 걸림  
```   