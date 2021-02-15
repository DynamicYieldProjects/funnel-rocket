# Base image with up-to-date packages & pip version
ARG PYTHON_VERSION=3.8
FROM python:${PYTHON_VERSION}-slim as base
RUN apt-get update && apt-get clean
RUN python -m pip install --upgrade pip

# Builder image: install packages (including frocket itself) and cleanup some big un-needed dirs
FROM base as package-install
WORKDIR /app
ADD ./setup.py .
ADD ./requirements.txt .
ADD ./frocket frocket
RUN pip install --no-cache-dir . -t ./packages
RUN rm ./packages/pyarrow/*flight*.so* \
    ./packages/pyarrow/*plasma*.so* \
    ./packages/pyarrow/plasma-store-server
# Delete all directories in botocore/data except for services actually used by frocket
RUN find ./packages -type d -name tests | xargs rm -rf && \
    find ./packages -type d -name include | xargs rm -rf && \
    find ./packages/botocore/data -type d -mindepth 1 -maxdepth 1 | \
    grep -vE 's3|lambda' | xargs rm -rf

# Copy the pruned packages to the final image
# By having that based on 'base' again, we're not carrying over intermediate fat layers from the builder image
FROM base
WORKDIR /app
COPY --from=package-install /app/packages packages
ADD ./docker/entrypoint.sh .
RUN chmod +x ./entrypoint.sh
RUN useradd -ms /bin/bash frocket
USER frocket
ENV PYTHONPATH=/app/packages
ENTRYPOINT ["./entrypoint.sh"]