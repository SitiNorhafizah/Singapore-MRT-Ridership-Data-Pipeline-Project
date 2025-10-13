FROM apache/superset:latest

USER root

# Upgrade pip
RUN pip install --upgrade pip

# Only pure Python SASL support
RUN pip install --no-cache-dir pyhive thrift pure-sasl

USER superset

