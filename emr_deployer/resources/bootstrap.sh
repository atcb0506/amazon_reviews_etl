#!/bin/bash -xe


sudo yum install -y docker

sudo usermod -aG docker $USER
sudo service docker start

sudo pip3 install -U \
psycopg2-binary \
boto3

DOCKER_LOGIN=`aws ecr get-login --no-include-email --region ap-southeast-1`
SUDO_DOCKER_LOGIN="sudo $DOCKER_LOGIN"
eval $SUDO_DOCKER_LOGIN

exit $?