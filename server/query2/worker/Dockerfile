FROM server-base:latest

COPY server/query2/worker /
COPY server/query2/serializer /utils/serializer
COPY server/query2/dto /dto

RUN python -m unittest test/tests.py

ENTRYPOINT ["/bin/sh"]