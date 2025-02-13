#!/bin/bash

if [ -z "$1" ]; then
    echo "Usage: ./deploy.sh <lambda_function_name>"
    exit 1
fi

FUNCTION_NAME="$1"

cd "$FUNCTION_NAME"

ZIP_FILE="$FUNCTION_NAME.zip"
VENV_PATH="venv"
PACKAGE_DIR="package"

echo "Deploying $FUNCTION_NAME..."

if [ ! -d "$VENV_PATH" ]; then
    python -m venv "$VENV_PATH"
fi

rm -rf "$PACKAGE_DIR"
mkdir "$PACKAGE_DIR"

source "$VENV_PATH/bin/activate"
pip install --upgrade pip
pip install --platform manylinux2014_aarch64 --only-binary=:all: -r "requirements.txt" -t "$PACKAGE_DIR"

rm -f "$ZIP_FILE"

cd "$PACKAGE_DIR" || exit
zip -r9 "../$ZIP_FILE" .
cd ..

zip -g "$ZIP_FILE" "lambda_function.py" || { echo "Error: lambda_function.py not found!"; exit 1; }

export AWS_PAGER=""  
aws lambda update-function-code --function-name "$FUNCTION_NAME" --zip-file fileb://"$ZIP_FILE"

echo "Deployment complete for $FUNCTION_NAME"

cd ..