FROM alpine
# ISSUE: centos? 6 or 7?
MAINTAINER Dan Connolly <dconnolly@kumc.edu>

RUN apk add --update python-dev py-pip musl-dev gcc && rm -rf /var/cache/apk/*

ENV ORACLE_HOME /opt/oracle/instantclient_12_1

ADD ora_cache/instantclient-*.zip /tmp/
RUN mkdir -p /opt/oracle && \
    unzip -q -d /opt/oracle/ /tmp/instantclient-basic-*.zip && \
    unzip -q -d /opt/oracle/ /tmp/instantclient-sdk-*.zip && \
    ln -s $ORACLE_HOME/libclntsh.so.12.1 $ORACLE_HOME/libclntsh.so
RUN pip install cx_Oracle

ADD . .
RUN ORACLE_HOME=/opt/instantclient_12_1 pip install -r requirements.txt
