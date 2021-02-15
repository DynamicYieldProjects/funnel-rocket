#!/usr/bin/env sh
set -e
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "${YELLOW}==> Running docker build to install packages in Lambda-like image...${NC}"
docker build -f docker/local-lambda.Dockerfile . -t frocket:local-lambda
docker run -d --name lambda-builder frocket:local-lambda

BUILD_DIR=$(mktemp -d -t build-lambda)
mkdir -p $BUILD_DIR/function
mkdir -p $BUILD_DIR/layer
echo "${YELLOW}==> Copying files from container to build directory: ${BUILD_DIR}...${NC}"
docker cp lambda-builder:/var/task/frocket $BUILD_DIR/function/frocket
docker cp lambda-builder:/opt/python $BUILD_DIR/layer/python
echo "${YELLOW}==> Stopping & removing container...${NC}"
docker stop lambda-builder
docker rm lambda-builder

pushd $BUILD_DIR
echo "${YELLOW}==> Cleaning-up a bit and zipping lambda function and layer...${NC}"
(cd function && zip -qr ../lambda_function.zip ./frocket)
find ./layer/python -type d -name tests | xargs rm -rf
find ./layer/python -type d -name include | xargs rm -rf
(cd layer && zip -qr ../lambda_layer.zip ./python)
echo "${YELLOW} NOTE: Lambda size limit is 50mb compressed/250mb uncompressed for the function PLUS any layers it uses (unless using containers)${NC}"
echo "${YELLOW} Lambda layer size, uncompressed:${NC}"
du -sh ./layer
echo "${YELLOW} Lambda layer size, zipped:${NC}"
du -h lambda_layer.zip
echo "${YELLOW} Lambda function, zipped:${NC}"
du -h lambda_function.zip
popd
cp $BUILD_DIR/lambda_function.zip .
cp $BUILD_DIR/lambda_layer.zip .
echo "${GREEN} DONE! lambda_function.zip and lambda_layer.zip copied to current dir${NC}"
