FROM python:3.9.7-slim

COPY chaosMonkey /
COPY utils /utils
COPY server/utils /utils
COPY model /model/

RUN pip install docker

ENTRYPOINT ["/bin/sh"]