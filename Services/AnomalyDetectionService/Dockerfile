FROM python:3.9-slim

#RUN apk update \
 # && apk add \
  #  build-base 

WORKDIR /usr/src/app

COPY ./requirements.txt ../
RUN pip install -U pip
RUN pip install -r ../requirements.txt

COPY ./src .
COPY ./misc ../misc
ENV PYTHONUNBUFFERED 1
CMD ["python", "anomalyDetection.py"]
