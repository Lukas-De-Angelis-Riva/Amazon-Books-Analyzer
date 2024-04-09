FROM python:3.9.7-slim

COPY utils /utils
COPY model /model/

RUN pip install pika
RUN python -m unittest utils/test/test_utils.py