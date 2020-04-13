# base image
FROM python:3.6-stretch

# Set up kernel
RUN apt-get update
RUN apt-get install -y awscli vim openjdk-8-jdk zip

# Handle user rights
RUN groupadd -g 500 hadoop
RUN useradd -u 498 -g hadoop hadoop

# Install Python requirements
ADD requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Add codes to container
ADD spark_etl /app
RUN cd /app
RUN chown -R hadoop:hadoop /app

# Packaging app
WORKDIR app
RUN zip -r spark_etl_package.zip *

ENTRYPOINT ["bash", "run_app.sh"]