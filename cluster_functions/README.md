## 로컬에서 Spark 사용해서 s3 데이터 읽고 쓰기 
- hadoop-aws-3.2.4.jar, aws-java-sdk-bundle-1.11.901.jar 파일이 $SPARK_HOME/jars에 있어야 합니다.  


- input: .json, output: .parquet   
```bash  
spark-submit nlp_test.py s3a://the-all-new-bucket/싼타페/2025/01 s3a://the-all-new-bucket/싼타페/2025/01/raw_output  
```  

- test.py 코드로 결과를 볼 수 있습니다.  
```bash
spark-submit test.py s3a://the-all-new-bucket/싼타페/2025/01/raw_output    
```  

- 결과 예시  
column: text, date(timestamp 형식), view_count, like_count, dislike_count,source(3가지), type(title/article/comment 중), weight

+------------------------------------+----------+----------+----------+-------------+------+-------+------+  
|                                text|      date|view_count|like_count|dislike_count|source|   type|weight|  
+------------------------------------+----------+----------+----------+-------------+------+-------+------+  
|         기다린 기간이 오래되서 ㅠㅠ|1738364286|     10890|         0|            0| clien|article|   9.3|  
|                  5월 출고 예정인데.|1738364286|     10890|         0|            0| clien|article|   9.3|  
|저거 넣으면 첨부터 다시 기다려야해서|1738364286|     10890|         0|            0| clien|article|   9.3|  
|    지금부터 기다리면 10월이나 받...|1738364286|     10890|         0|            0| clien|article|   9.3|  
|                딜러분에게 물어보니.|1738364286|     10890|         0|            0| clien|article|   9.3|  
| 그때쯤 받으면 차량 연식이 바뀔때라.|1738364286|     10890|         0|            0| clien|article|   9.3|  
|               차값 상승 될수도 있음|1738364286|     10890|         0|            0| clien|article|   9.3|  
|                 개소세 할인 없어짐.|1738364286|     10890|         0|            0| clien|article|   9.3|  
|     보니까 재수없으면 약 200정도...|1738364286|     10890|         0|            0| clien|article|   9.3|  
|      1. 그냥 받는다. 드와 그따위...|1738364286|     10890|         0|            0| clien|article|   9.3|  
|               2. 취소하고 옵션 수정|1738364286|     10890|         0|            0| clien|article|   9.3|  
|    참고로 20년넘게 운전하면서 무...|1738364286|     10890|         0|            0| clien|article|   9.3|  
| 뭔가 전자장치가 개입하는게 어색하게|1738364286|     10890|         0|            0| clien|article|   9.3|  
|  느껴질 수도 있을 것 같습니다. ㅜㅜ|1738364286|     10890|         0|            0| clien|article|   9.3|  
|  근대 다들 필수라고 하는 것 같아서.|1738364286|     10890|         0|            0| clien|article|   9.3|  
|     드라이브와이즈. 사용해본적없고.|1738364286|     10890|         0|            0| clien|article|   9.3|  
|와이프님차에는 크루저 기능이 있지만.|1738364286|     10890|         0|            0| clien|article|   9.3|  
|   그것도 거이 안쓰는 스타일이라....|1738364286|     10890|         0|            0| clien|article|   9.3|  
|   원래 남자는 스틱 수동 아닙니까...|1738364286|     10890|         0|            0| clien|article|   9.3|  
|  어쩌다 세상이.온통 오토가 돼가지고|1738364286|     10890|         0|            0| clien|article|   9.3|  
+------------------------------------+----------+----------+----------+-------------+------+-------+------+  


## parquet 파일 나눠서 저장하기  
```bash  
spark-submit nlp_test_split.py s3a://the-all-new-bucket/싼타페/2025/01 s3a://the-all-new-bucket/싼타페/2025/01/raw_output  
```  
the-all-new-bucket/싼타페/2025/01/raw_output/data_part_1.parquet (1부터 5까지 저장됨)  


## 스크립트로 실행하기
- 실행 코드는 기본 버킷(the-all-new-bucket)에 있습니다.  

1. aws 콘솔에서 emr 실행하기  
"cluster-kga" or "EMR Test" clone하기   

Service role for Amazon EMR: AmazonEMR-ServiceRole-20250207T173004   
EC2 instance profile: AmazonEMR-InstanceProfile-20250207T172947  

2. submit-job.sh 안에 생성한 emr 클러스터 ID로 변경하기  

3. 스크립트 코드 실행
```bash  
./submit-job.sh '싼타페' '2025-01'  
```
-> s3://the-all-new-bucket/싼타페/2025/01/raw_output 에 parquet 형식으로 저장됨.  

클러스터 상태 확인  
```bash  
aws emr describe-cluster --cluster-id "j-000000"   
```   
 
step 상태 확인  
```bash  
aws emr describe-step --cluster-id "j-000000" --step-id "s-0000000"  
```   
