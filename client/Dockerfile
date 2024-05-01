FROM python-base:latest

COPY client/common /common/
COPY client/main.py /

RUN pip install -U alive-progress

ENTRYPOINT ["/bin/sh"]