FROM python-base:latest

COPY server/resultHandler /
COPY server/query1/serializer /utils/serializer
COPY server/query2/serializer /utils/serializer
COPY server/query3/serializer /utils/serializer
COPY server/query3/dto /dto
COPY server/query5/serializer /utils/serializer

ENTRYPOINT ["/bin/sh"]