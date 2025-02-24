# EMR

## 개발 가이드라인
[EMR_DEV_GUIDE](EMR_DEV_GUIDE.md) 참고

### process_text.py

스파크를 활용하여 데이터를 분리하고 전처리하는 코드입니다.  

json파일의 데이터를 불러와 post_data와 comment_data로 나눕니다.

post_data와 comment_data의 title, content, text 컬럼 데이터를 문장 단위로 나누어 sentence_data로 저장합니다.




### test_dataskew.py
로컬 및 EMR 클러스터에서 데이터 스큐잉을 테스트하는 코드입니다. 


### show.py

s3에 저장돼 있는 parquet 데이터를 조회할 수 있는 코드입니다. 
