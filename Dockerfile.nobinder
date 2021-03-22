FROM python:3.7.4

RUN apt-get update\
 && apt-get install -y --no-install-recommends graphviz

RUN pip install --upgrade pip==21.0.1

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY setup.py README.rst /app/
COPY flexvalue/ /app/flexvalue/

WORKDIR /app
ENV PYTHONPATH=/app

RUN cd /usr/local/lib/python3.7/site-packages && \
    python /app/setup.py develop
