FROM python:3.9.6

WORKDIR /backend

RUN apt-get update && \
    apt-get install -y \
    wkhtmltopdf \
    xvfb \
    libfontconfig \
    libjpeg62-turbo \
    libxrender1 \
    tesseract-ocr \
    tesseract-ocr-rus\
    poppler-utils build-essential libssl-dev libffi-dev

RUN mkdir "tmp"

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1