FROM python:3.9.7-slim

COPY utils /utils
COPY server/utils /utils
COPY model /model/

RUN pip install pika