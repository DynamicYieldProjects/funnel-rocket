ARG PYTHON_VERSION=3.8

FROM python:${PYTHON_VERSION}-slim as base

ADD ./setup.py /app/setup.py
ADD ./README.md /app/README.md
ADD ./frocket /app/frocket
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libssl-dev \
        libcurl4-openssl-dev \
        gcc \
        build-essential \
    && cd /app \
    && pip install . --ignore-installed \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

RUN useradd -ms /bin/bash frocket
ADD ./docker/entrypoint.sh /app/entrypoint.sh
RUN chmod +x ./entrypoint.sh
RUN chown frocket:frocket ./entrypoint.sh

#USER frocket

FROM base as dev
ADD ./requirements /app/requirements
RUN pip install -r /app/requirements/test.txt

FROM base as prod
ENTRYPOINT ["./entrypoint.sh"]
