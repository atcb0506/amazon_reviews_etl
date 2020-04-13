# base image
FROM python:3.6-stretch

# Set up kernel
RUN apt-get update
RUN apt-get install -y awscli vim openjdk-8-jdk zip

# Install Python requirements
ADD spark_etl/requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Add codes to container
ADD spark_etl /app
RUN cd /app

# Packaging app
WORKDIR app