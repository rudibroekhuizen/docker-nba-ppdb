FROM python:3.6.5-alpine

RUN apk update \
  && apk add \
    build-base \
    postgresql \
    postgresql-dev \
    libpq \
    libffi \
    libffi-dev

RUN mkdir /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED 1

COPY . .
