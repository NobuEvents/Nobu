#!/bin/bash

TAG="1.5"
docker build -t nobu:$TAG .
docker tag nobu:$TAG gcr.io/just-site-344717/nobu:$TAG
docker push gcr.io/just-site-344717/nobu:$TAG

sed -e "s/tag/"$TAG"/g" kube.yaml > kube_latest.yaml
kubectl delete deploy nobu-dep
sleep 15
kubectl apply -f kube_latest.yaml
rm kube_latest.yaml

sleep 30
POD_NAME=$(kubectl get pods | grep "nobu" | awk '{ print $1 }')
kubectl port-forward $POD_NAME 8080:8080
#open http://localhost:8080/q/health/live
