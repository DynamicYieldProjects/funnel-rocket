ARG PYTHON_VERSION=3.8
FROM python:${PYTHON_VERSION}-slim as base
RUN apt-get update && apt-get clean
RUN useradd -ms /bin/bash frocket

FROM base as package-install
WORKDIR /app
ADD ./setup.py .
ADD ./README.md .
ADD ./frocket frocket
USER frocket
RUN pip install --no-cache-dir --user .
WORKDIR /home/frocket/.local/lib/python3.8/site-packages/
RUN rm ./pyarrow/*flight*.so* ./pyarrow/*plasma*.so* ./pyarrow/plasma-store-server
RUN find . -type d -name tests | xargs rm -rf
RUN find . -type d -name include | xargs rm -rf
RUN find ./botocore/data -type d  -mindepth 1 -maxdepth 1 | grep -vE 's3|lambda' | xargs rm -rf

FROM base
USER frocket
COPY --from=package-install /home/frocket/.local /home/frocket/.local
WORKDIR /app
ADD --chown=frocket:frocket ./docker/entrypoint.sh .
RUN chmod +x ./entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]