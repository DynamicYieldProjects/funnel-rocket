#!/usr/bin/env sh

REQS_FILE=lambda-reqs.txt
python pkginfo.py --skip-lambda-builtins > $REQS_FILE
echo 'Python packages to be installed:'
cat $REQS_FILE
docker build -f frocket-packages.Dockerfile -t frocket-packages-layer:latest --build-arg REQS_FILE=$REQS_FILE .
docker run -d --name package-builder frocket-packages-layer:latest
docker cp package-builder:/packages.zip ./frocket-packages-layer.zip
docker rm package-builder
echo 'Final zip file size (AWS Lambda limit for code + all layers is 50mb compressed, unless using container images)'
du -h ./frocket-packages-layer.zip
