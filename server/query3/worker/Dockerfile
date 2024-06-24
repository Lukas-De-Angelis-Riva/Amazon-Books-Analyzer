FROM server-base:latest

COPY server/query3/worker /
COPY server/query3/serializer /utils/serializer
COPY server/query3/dto /dto

ENTRYPOINT ["/bin/sh"]