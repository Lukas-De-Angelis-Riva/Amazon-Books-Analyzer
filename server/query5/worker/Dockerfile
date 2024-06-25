FROM server-base:latest
RUN pip install -U textblob
RUN python3 -m textblob.download_corpora

COPY server/query5/worker /
COPY server/query5/serializer /utils/serializer
COPY server/query5/dto /dto

ENTRYPOINT ["/bin/sh"]