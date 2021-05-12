ARG PYTHON_VERSION=3.8
# Note: not using multi-stage build here, in contrary to all-in-one image.
# This has the pro of very fast incremental builds locally, and the con of large image size - ok for tests.
# Since we're switching to root during build,
# need to return to default Lambda user afterwards (as defined in base image)
ARG RUN_USER=sbx_user1051
FROM lambci/lambda:python3.8
# Lambda function code should be in /var/task
WORKDIR /var/task
COPY ./setup.py .
COPY ./requirements.txt .
# Lambda layer(s) (useful for holding all big & infrequently changing dependencies)
# should be located under /opt, which is only writable by root.
# Don't install boto3/botocore, which is vendored by AWS in its most appropriate version
USER root
RUN grep -v boto requirements.txt > lambda_requirements.txt
RUN mkdir /opt/python && pip install --no-compile --no-cache-dir -r lambda_requirements.txt -t /opt/python
# Clean-up some big files
RUN rm /opt/python/pyarrow/*flight*.so* \
    /opt/python/pyarrow/*plasma*.so* \
    /opt/python/pyarrow/plasma-store-server \
    setup.py requirements.txt lambda_requirements.txt
# Go back to user & workdir of base image
USER ${RUN_USER}
# Copy package source code, which is frequently changing, only at end of Dockerfile
COPY ./frocket /var/task/frocket
WORKDIR /var/task
# These values are for running tests, not production usage
ENV DOCKER_LAMBDA_STAY_OPEN=1 \
    AWS_LAMBDA_FUNCTION_NAME=frocket \
    AWS_LAMBDA_FUNCTION_TIMEOUT=15 \
    AWS_LAMBDA_FUNCTION_MEMORY_SIZE=256
