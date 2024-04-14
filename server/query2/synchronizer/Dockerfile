FROM python-base:latest

COPY server/query2/synchronizer /

RUN python -m unittest test/tests.py

ENTRYPOINT ["/bin/sh"]