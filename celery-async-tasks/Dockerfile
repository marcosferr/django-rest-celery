#Setear imagen
FROM python:3.10

# Setear variables de entorno
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /code/

# Install dependencies
RUN pip install django
RUN pip install celery
RUN pip install redis

# Copy project
COPY . /code/