#!/usr/bin/env bash
# Cài Strimzi + cụm Kafka + các topic vào namespace "ingest" trên cụm k3d đang chạy.
# Chạy từ gốc repo:  bash deploy/kafka/install.sh   (chạy lại nhiều lần vẫn an toàn)
set -euo pipefail

STRIMZI_VERSION=1.2.0   # pin theo joblens/versions.md – không dùng "latest"
HERE="$(cd "$(dirname "$0")" && pwd)"

kubectl create namespace ingest --dry-run=client -o yaml | kubectl apply -f -

echo "==> Strimzi operator ${STRIMZI_VERSION} (Helm)"
helm upgrade --install strimzi \
  "https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-kafka-operator-helm-3-chart-${STRIMZI_VERSION}.tgz" \
  --namespace ingest --set watchNamespaces="{ingest}" --wait --timeout 5m

echo "==> Cụm Kafka 'joblens' (KRaft, 1 node)"
kubectl apply -f "${HERE}/kafka-cluster.yaml"
kubectl wait kafka/joblens -n ingest --for=condition=Ready --timeout=600s

echo "==> Topic"
kubectl apply -f "${HERE}/topics.yaml"
kubectl wait kafkatopic --all -n ingest --for=condition=Ready --timeout=180s
kubectl get kafkatopic -n ingest -o custom-columns=TOPIC:.spec.topicName,PARTITIONS:.spec.partitions,READY:.status.conditions[0].status
