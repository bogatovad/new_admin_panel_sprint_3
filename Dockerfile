FROM python:3.10
COPY etl .
RUN pip3 install -r requirements.txt