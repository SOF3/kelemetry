#!/usr/bin/env bash

kubectl create -f ${REPO_PATH}/crds/samples/pod-secret.yaml

kubectl create secret generic content

kubectl create -f - << EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: reader
  labels:
    app: reader
spec:
  selector:
    matchLabels:
      app: reader
  replicas: 2
  template:
    metadata:
      labels:
        app: reader
    spec:
      containers:
        - name: reader
          command: ["sleep", "infinity"]
          image: alpine:3.16
          volumeMounts:
            - name: content
              mountPath: /mnt/content
      volumes:
        - name: content
          secret:
            secretName: content
EOF
sleep 10

kubectl delete deployment/reader secret/content
sleep 20
