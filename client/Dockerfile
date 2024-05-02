FROM python:3.9.7-slim

COPY utils /utils
COPY model /model
COPY client/common /common/
COPY client/main.py /

RUN pip install -U alive-progress

ENTRYPOINT ["/bin/sh"]