FROM python-base:latest
RUN pip install -U textblob
RUN python3 -m textblob.download_corpora

COPY server/query5/synchronizer /
COPY server/query5/serializer /utils/serializer
COPY server/query5/dto /dto

ENTRYPOINT ["/bin/sh"]