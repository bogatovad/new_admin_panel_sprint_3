FROM python:3.10
COPY etl .
COPY enviroments /enviroments
RUN pip3 install -r requirements.txt