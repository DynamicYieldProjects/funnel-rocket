# See https://stackoverflow.com/a/64182487 to learn why this exists at all ;-)
FROM minio/minio:RELEASE.2021-01-30T00-20-58Z
ENV MINIO_ROOT_USER=testonly
ENV MINIO_ROOT_PASSWORD=testonly
ENTRYPOINT ["/bin/sh", "-c", "minio server /data"]