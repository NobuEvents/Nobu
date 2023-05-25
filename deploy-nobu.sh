#!/bin/bash

docker build -t nobu:1.0 .
docker tag nobu gcr.io/just-site-344717/nobu:1.0
docker push gcr.io/just-site-344717/nobu:1.0

kubectl apply -f kube.yaml