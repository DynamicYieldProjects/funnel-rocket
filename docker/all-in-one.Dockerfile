# Base Python image with up-to-date OS packages & pip
ARG PYTHON_VERSION=3.8
FROM python:${PYTHON_VERSION}-slim as base
RUN apt-get update && apt-get clean && \
    python -m pip install --upgrade pip

# Builder image: install packages and cleanup some big un-needed dirs
FROM base as package-install
WORKDIR /app
COPY ./requirements.txt .
RUN pip install --no-cache-dir --no-compile -r requirements.txt -t ./packages
# Delete un-needed big files in pyarrow,
# and all directories in botocore/data except for services actually used by frocket
RUN rm ./packages/pyarrow/*flight*.so* \
    ./packages/pyarrow/*plasma*.so* \
    ./packages/pyarrow/plasma-store-server && \
    find ./packages -type d -name tests | xargs rm -rf && \
    find ./packages -type d -name include | xargs rm -rf && \
    find ./packages/botocore/data -type d -mindepth 1 -maxdepth 1 | \
    grep -vE 's3|lambda' | xargs rm -rf
# Copy and install funnel-rocket itself only after seldom-chanding dependencies are done, for better caching
COPY ./setup.py .
COPY ./frocket frocket
RUN pip install --no-cache-dir --no-compile --no-deps . -t ./packages

# Copy the pruned packages to the final image.
# This image is based on 'base' again, so it doesn't carry over intermediate fat layers from package-install image
FROM base
WORKDIR /app
COPY --from=package-install /app/packages packages
COPY ./docker/entrypoint.sh .
RUN chmod +x ./entrypoint.sh
RUN useradd -ms /bin/bash frocket
USER frocket
ENV PYTHONPATH=/app/packages
ENTRYPOINT ["./entrypoint.sh"]