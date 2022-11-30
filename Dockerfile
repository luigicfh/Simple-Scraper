FROM python:3.10-slim


ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN apt-get update -y && apt-get update

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

CMD python scrape.py