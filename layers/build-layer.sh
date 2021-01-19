#!/usr/bin/env sh

PACKAGES=`python pkginfo.py --skip-lambda-builtins`
echo 'Python packages to be installed:' $PACKAGES
docker build -f frocket-packages.Dockerfile -t frocket-packages-layer:latest --build-arg PACKAGES .
docker run -d --name packages frocket-packages-layer:latest
docker cp packages:/packages.zip ./frocket-packages-layer.zip
docker rm packages
echo 'Final zip file size (AWS Lambda limit for code + all layers is 50mb compressed, unless using container images)'
du -h ./frocket-packages-layer.zip
