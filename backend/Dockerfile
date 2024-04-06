FROM python:3.9.6

WORKDIR /backend

RUN apt-get update && \
    apt-get install -y \
    wkhtmltopdf \
    xvfb \
    libfontconfig \
    libjpeg62-turbo \
    libxrender1

RUN mkdir "tmp/runtime-root"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1