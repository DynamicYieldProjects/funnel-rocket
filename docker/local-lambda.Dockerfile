ARG PYTHON_VERSION=3.8
# User as defined in base image. Since we're switching to root during build,
# need to return to default user afterwards
ARG RUN_USER=sbx_user1051
FROM lambci/lambda:python${PYTHON_VERSION}

# Lambda function code should be in /var/task
WORKDIR /var/task
ADD ./setup.py .
ADD ./README.md .
# Copy package code. Note that any .pyc files/__pycache__ dirs are filtered by .dockerignore
ADD ./frocket frocket
# Layers should be under /opt, only writable by root
USER root
RUN mkdir /opt/python && pip install --no-cache-dir . -t /opt/python
# Layer should not have (a) funnel-rocket package itself, (b) boto3/botocore which is vendored
# Additionally, cleanup some big files
RUN rm -rf /opt/python/frocket* \
    /opt/python/funnel_rocket* \
    /opt/python/boto* \
    /opt/python/pyarrow/*flight*.so* \
    /opt/python/pyarrow/*plasma*.so* \
    /opt/python/pyarrow/plasma-store-server \
    setup.py README.md
# RUN find /opt/python -type d -name tests | xargs rm -rf
# RUN find /opt/python -type d -name include | xargs rm -rf
# Go back to user & workdir of base image
USER ${RUN_USER}
WORKDIR /
ENV DOCKER_LAMBDA_STAY_OPEN=1 \
    AWS_LAMBDA_FUNCTION_NAME=frocket \
    AWS_LAMBDA_FUNCTION_TIMEOUT=15 \
    AWS_LAMBDA_FUNCTION_MEMORY_SIZE=256