#!/usr/bin/env sh
set -e
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color
GITHASH=`git rev-parse HEAD | cut -c1-8``[[ -z $(git status -s) ]] || echo dirty`

echo "${YELLOW}==> Git commit hash: ${GITHASH}${NC}"
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
FUNCTION_ZIPFILE=lambda-function-${GITHASH}.zip
LAYER_ZIPFILE=lambda-layer-${GITHASH}.zip

(cd function && zip -qr ../$FUNCTION_ZIPFILE ./frocket)
find ./layer/python -type d -name tests | xargs rm -rf
find ./layer/python -type d -name include | xargs rm -rf
(cd layer && zip -qr ../$LAYER_ZIPFILE ./python)
echo "${YELLOW}NOTE: Lambda size limit is 50mb compressed/250mb uncompressed for the function PLUS any layers it uses (unless using containers)${NC}"
echo "${YELLOW}Lambda layer size, uncompressed:${NC}"
du -sh ./layer
echo "${YELLOW}Lambda layer size, zipped:${NC}"
du -h $LAYER_ZIPFILE
echo "${YELLOW}Lambda function, zipped:${NC}"
du -h $FUNCTION_ZIPFILE
popd
# Don't fail if previous files don't exist
rm lambda-function-*.zip lambda-layer-*.zip || true
cp $BUILD_DIR/$FUNCTION_ZIPFILE .
cp $BUILD_DIR/$LAYER_ZIPFILE ./
rm -rf $BUILD_DIR
echo "${YELLOW}DONE! copied to current dir:${NC}\n${FUNCTION_ZIPFILE} ${LAYER_ZIPFILE}"
