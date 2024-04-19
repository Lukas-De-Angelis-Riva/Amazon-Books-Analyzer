FROM python-base:latest

COPY server/query3/worker /

RUN python -m unittest test/tests.py

ENTRYPOINT ["/bin/sh"]