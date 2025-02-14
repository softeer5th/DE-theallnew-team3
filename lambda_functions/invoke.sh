#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: ./invoke.sh <lambda_function_name>"
    exit 1
fi

if [ -z "$2" ]; then
    echo "Usage: ./invoke.sh <lambda_function_name> <input_file_path>"
    exit 1
fi

if [ -z "$3" ]; then
    echo "Usage: ./invoke.sh <lambda_function_name> <input_file_path> <output_file_path>"
    exit 1
fi

FUNCTION_NAME="$1"
INPUT_FILE_PATH="$2"
OUT_FILE_PATH="$3"

aws lambda invoke --function-name "$FUNCTION_NAME" --payload fileb://"$INPUT_FILE_PATH" "$OUT_FILE_PATH" --cli-read-timeout 300 --cli-connect-timeout 300
