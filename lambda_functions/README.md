# Lambda Functions

## 개발 가이드라인
[LAMBDA_DEV_GUIDE](LAMBDA_DEV_GUIDE.md) 참고

## 함수 목록

### Collect Target Posts

#### `collect_target_bobae`, `collect_target_clien`, `collect_target_video`

`search_keywords`에 대해 `input_date` 기준으로 최근 1주일 치의 게시글/영상 url을 수집합니다. 수집한 데이터는 S3에 `.csv` 형태로 저장됩니다.

#### input
```json
{
    "input_date": "2025-01-01",
    "car_name": "Grandeur",
    "search_keywords": "그랜저,그랜져,그랜져 후기"
}
```


### Crawl Posts

#### `crawl_bobae`, `crawl_clien`, `crawl_youtube`

수집한 게시글/영상 url을 크롤링/API 호출하여 데이터를 수집합니다. 수집한 데이터는 S3에 `.json` 형태로 저장됩니다.  
실패한 url과 영상은 별도로 저장 후 재시도 처리를 기다립니다.

#### input
```json
{
    "input_date": "2025-01-01",
    "car_name": "Grandeur",
}
```

#### output
```json
{
    "statusCode": 200,
    "failed": ["url1", "url2"] // if failed posts exist
}
```

### Crawl Posts Recovery

#### `crawl_bobae_recovery`, `crawl_clien_recovery`, `crawl_youtube_recovery`

실패한 게시글/영상에 대해 다시 수집을 시도합니다. 일시적인 네트워크 장애로 인한 실패를 방지하기 위한 목적입니다.  
재시도에 실패한 url은 별도 저장 후 개발자의 조치를 기다립니다.

#### input
```json
{
    "input_date": "2025-01-01",
    "car_name": "Grandeur",
}
```

#### output
```json
{
    "statusCode": 200,
    "failed": ["url1", "url2"] // if failed posts exist
}
```

### Validate Data

#### `validate_json`, `validate_parquet`

데이터의 스키마를 검증합니다. raw data(json)의 경우 데이터 소스별로 데이터 샘플링을 통해 검증하고, parquet 파일은 메타데이터를 읽어서 검증합니다.

### Classify Posts

#### `classify_sentence`

LLM을 사용해서 문장 분석을 진행합니다. S3 Object 단위로 병렬로 처리할 수 있습니다.

#### input
```json
{
    "input_date": "2025-01-01",
    "car_name": "Grandeur",
    "object_key": "/2025/01/01/sentence_data.parquet"
}
```


