#! /bin/bash
# Build a docker image
cd ..
docker build -t pyengine/aws-cloudwatch . --no-cache
docker tag pyengine/aws-cloudwatch pyengine/google-cloud-stackdriver:1.0
docker tag pyengine/aws-cloudwatch spaceone/google-cloud-stackdriver:1.0
