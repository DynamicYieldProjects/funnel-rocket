# Base Python image with up-to-date OS packages & pip
FROM python:3.8-slim as base
RUN apt-get update && apt-get clean && \
    python -m pip install --upgrade pip

# Builder image: install packages and then cleanup some un-needed large files and directories
FROM base as package-install
WORKDIR /app
COPY ./requirements.txt .
RUN pip install --no-cache-dir --no-compile -r requirements.txt -t ./packages
# Delete un-needed big files in pyarrow, tests & include dirs,
# and all directories in botocore/data except for services actually used by frocket
RUN rm ./packages/pyarrow/*flight*.so* \
    ./packages/pyarrow/*plasma*.so* \
    ./packages/pyarrow/plasma-store-server && \
    find ./packages -type d -name tests | xargs rm -rf && \
    find ./packages -type d -name include | xargs rm -rf && \
    find ./packages/botocore/data -type d -mindepth 1 -maxdepth 1 | grep -vE 's3|lambda' | xargs rm -rf

# This image is based on 'base' again, so it doesn't carry over intermediate fat layers from package-install image.
# It copies over only the pruned packages to the final image.
FROM base
WORKDIR /app
COPY ./docker/entrypoint.sh .
RUN chmod +x ./entrypoint.sh
RUN useradd -ms /bin/bash frocket
COPY --from=package-install /app/packages packages
# The most frequently-changing file set - the source code itself, is copied last so previous layers are unaffected
COPY ./requirements.txt .
COPY ./test-requirements.txt .
COPY ./setup.py .
COPY ./frocket frocket
COPY ./tests tests
RUN pip install --no-cache-dir --no-compile --no-deps . -t ./packages
USER frocket
ENV PYTHONPATH=/app/packages
ENTRYPOINT ["./entrypoint.sh"]
