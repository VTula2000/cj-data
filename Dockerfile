FROM python:3.7

LABEL version="1.0"
LABEL maintainer="Ian Ansell"

RUN pip install --upgrade pip

COPY . /cj-data
WORKDIR /cj-data

RUN pip install --no-cache-dir -r requirements.txt
RUN chmod 444 src/*.py
RUN chmod 444 requirements.txt


ENV gcp_project customerjourney-214813
ENV bq_dataset cj_data

ENV PORT 8080

CMD python src/controller.py