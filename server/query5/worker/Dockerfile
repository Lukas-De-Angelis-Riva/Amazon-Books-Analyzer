FROM server-base:latest

COPY server/query5/worker /
COPY server/query5/serializer /utils/serializer
COPY server/query5/dto /dto

ENTRYPOINT ["/bin/sh"]