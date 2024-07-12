FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN ln -sf /usr/share/zoneinfo/Europe/Zurich /etc/localtime

RUN apt-get update && apt-get install -q -y --no-install-recommends curl rsync ssh jq htop

COPY requirements.txt /tmp

RUN apt-get install -q -y --no-install-recommends build-essential && \
    pip install --no-cache-dir --upgrade setuptools pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt scikit-learn && \
    apt-get purge -q -y --auto-remove build-essential

WORKDIR /home/bbp-workflow

COPY logging.cfg ./

ENV HOME=/home/bbp-workflow

COPY dist/* dist/
RUN pip install --no-cache-dir $(ls -t $PWD/dist/*.* | head -n 1)

ENTRYPOINT ["luigi", "--local-scheduler", "--logging-conf-file", "/home/bbp-workflow/logging.cfg", "--module"]
