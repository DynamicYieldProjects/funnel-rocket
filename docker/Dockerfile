ARG PYTHON_VERSION=3.8
FROM python:${PYTHON_VERSION}-slim
RUN apt-get update \
    && apt-get clean
WORKDIR /app
ADD ./setup.py .
ADD ./README.md .
ADD ./frocket frocket
RUN useradd -ms /bin/bash frocket
USER frocket
RUN pip install --no-cache-dir --user .
WORKDIR /home/frocket/.local/lib/python3.8/site-packages/
RUN rm ./pyarrow/*flight*.so*
RUN rm ./pyarrow/*plasma*.so*
RUN rm ./pyarrow/plasma-store-server
RUN find . -type d -name tests | xargs rm -rf
RUN find . -type d -name include | xargs rm -rf
WORKDIR /app
ADD ./docker/entrypoint.sh .
USER root
RUN chmod +x ./entrypoint.sh
USER frocket
RUN du -h  /home/frocket
ENTRYPOINT ["./entrypoint.sh"]