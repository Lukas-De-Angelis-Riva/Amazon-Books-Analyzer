FROM python:3.9.7-slim

COPY utils /utils
COPY server/utils /utils
COPY model /model/
COPY base-images/test.sh /test.sh

RUN chmod 777 /test.sh

RUN pip install pika