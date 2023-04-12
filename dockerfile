
FROM python:3.9

WORKDIR /code

RUN apt update -y

COPY ./app /code/app
COPY ./requirements.txt /code/requirements.txt

EXPOSE 8000

RUN pip install -r requirements.txt