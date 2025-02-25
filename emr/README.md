# EMR

## 개발 가이드라인
[EMR_DEV_GUIDE](EMR_DEV_GUIDE.md) 참고

### process_text.py
- 인자로 car_name, year, month, day 정보가 필요합니다.
인자에 해당 위치에 raw/폴더가 있어야 합니다.   

- 스파크를 활용하여 데이터를 분리하고 전처리하는 코드입니다.  
json파일의 데이터를 불러옵니다.    
결측치나 이상치가 있다면 따로 저장합니다.   
불러온 데이터를 post_data와 comment_data로 나누어 저장합니다.    
크롤링하는 날짜에 해당하는 게시글과 댓글만 필터링합니다.  
필터링한 데이터로부터 sentence를 추출하여 저장합니다.  


### test_dataskew.py
- 로컬 및 EMR 클러스터에서 데이터 스큐잉을 테스트하는 코드입니다.  
- repartition 등을 적용할 수 있습니다.  


### show.py
- s3에 저장돼 있는 parquet 데이터를 조회합니다.
