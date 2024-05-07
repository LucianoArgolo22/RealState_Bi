# syntax=docker/dockerfile:1
FROM ubuntu:latest
FROM python:3
RUN pip install --no-cache-dir --upgrade pip
WORKDIR /code
COPY . .
RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --only main
RUN ls && ls ..
CMD ["python3","main.py"]