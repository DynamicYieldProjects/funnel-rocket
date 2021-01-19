# See README.md
FROM amazonlinux:latest AS base
RUN amazon-linux-extras enable python3.8
RUN yum install -y python38 && \
    yum install -y python3-pip && \
    yum install -y zip && \
    yum clean all
RUN python3.8 -m pip install --upgrade pip && \
    python3.8 -m pip install virtualenv

FROM base
ARG PACKAGES
RUN python3.8 -m venv frocketenv
RUN source frocketenv/bin/activate
RUN pip install $PACKAGES -t ./python
RUN rm /python/pyarrow/*flight*.so*
RUN rm /python/pyarrow/*plasma*.so*
RUN rm /python/pyarrow/plasma-store-server
RUN find /python -type d -name tests | xargs rm -rf
RUN find /python -type d -name include | xargs rm -rf
RUN zip -r packages.zip ./python