FROM server-base:latest

COPY server/query1/synchronizer /
COPY server/query1/serializer /utils/serializer

RUN python -m unittest test/tests.py

ENTRYPOINT ["/bin/sh"]