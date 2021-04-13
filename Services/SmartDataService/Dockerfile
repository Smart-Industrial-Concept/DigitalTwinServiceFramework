FROM python:3.9-alpine

RUN apk update \
  && apk add \
    build-base 

WORKDIR /usr/src/app

COPY ./requirements.txt ../
RUN pip install -r ../requirements.txt

COPY ./src .
ENV PYTHONUNBUFFERED 1
CMD ["python", "smartServiceEndpoint.py"]
