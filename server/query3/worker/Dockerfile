FROM python-base:latest

COPY server/query3/worker /
COPY server/query3/serializer /utils/serializer
COPY server/query3/dto /dto

RUN python -m unittest test/tests.py

ENTRYPOINT ["/bin/sh"]