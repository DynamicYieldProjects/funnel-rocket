#!/usr/bin/env sh

APISERVER_UP_LINE=`docker-compose ps frocket-apiserver | grep ' Up '`
if [[ "$APISERVER_UP_LINE" == "" ]]; then
  echo "API server service is not up, did you start docker-compose?"
  exit 1
fi

export TEST_REDIS_PORT=6380
export FROCKET_INVOKER=aws_lambda
export FROCKET_LAMBDA_AWS_NOSIGN=true
export FROCKET_LAMBDA_AWS_ENDPOINT_URL=http://localhost:9001
export FROCKET_LAMBDA_AWS_REGION=us-east-1
export FROCKET_INVOKER_LAMBDA_LEGACY_ASYNC=false
export FROCKET_S3_AWS_ENDPOINT_URL=http://localhost:9000
export FROCKET_S3_AWS_ACCESS_KEY_ID=testonly
export FROCKET_S3_AWS_SECRET_ACCESS_KEY=testonly

pytest tests/ -vv
