FROM python:3.9.7-slim

COPY base-images/test.sh /test.sh

RUN chmod 777 /test.sh

RUN pip install pika
RUN pip install -U textblob
RUN python3 -m textblob.download_corpora
