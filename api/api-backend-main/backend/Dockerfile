FROM python:3

WORKDIR /backend

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONBUFFERED 1

# install system dependencies
RUN apt-get update \
  && apt-get clean

# install python dependencies
RUN pip install --upgrade pip

COPY requirements.txt /backend/requirements.txt

RUN pip install -r requirements.txt

COPY .. /backend