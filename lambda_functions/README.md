# AWS Lambda 함수 가이드라인

## 배포 스크립트

스크립트는 기존 람다 함수 업데이트만 가능합니다. 새로운 람다 함수를 생성하시려면 AWS 콘솔을 이용해주세요.

배포 전에 로컬에서 lambda_function.py를 충분히 테스트해주세요.

```bash
cd lambda_functions
./deploy.sh <function_name>
```

## 배포된 함수 호출

```bash
cd lambda_functions
./invoke.sh <function_name> <input_file_path> <output_file_path>
```

예시

```bash
cd lambda_functions
./invoke.sh lambda_example input.json out.json
```
