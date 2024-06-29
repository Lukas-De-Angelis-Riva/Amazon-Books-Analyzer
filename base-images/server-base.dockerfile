FROM server-libraries:latest

COPY utils /utils
COPY server/utils /utils
COPY model /model/
