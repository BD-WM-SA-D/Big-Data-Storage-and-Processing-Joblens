"""Dựng lệnh spark-submit cho task Airflow chạy Spark job JobLens trong cụm K8s.

Mỗi task là một pod (KubernetesPodOperator) chạy image spark-jobs và làm luôn **driver** (client mode);
driver xin K8s tạo pod executor trong namespace "compute" bằng ServiceAccount "spark". Khác deploy/spark/submit.sh
(cluster mode, chạy từ máy dev): ở đây log của driver hiện thẳng trong giao diện Airflow và mã thoát của job
chính là kết quả của task.

Module thuần Python (không import Airflow) để kiểm thử bằng pytest (tests/test_airflow_dag.py).
"""

from __future__ import annotations

import shlex

NAMESPACE = "compute"
SERVICE_ACCOUNT = "spark"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
JOB_DIR = "/opt/joblens"
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"

# Địa chỉ dịch vụ trong cụm – giống deploy/spark/submit.sh và deploy/README.md
CLUSTER_ENV = {
    "JOBLENS_KAFKA_BOOTSTRAP": "joblens-kafka-bootstrap.ingest.svc.cluster.local:9092",
    "JOBLENS_CATALOG_URI": "http://lakekeeper.lakehouse.svc.cluster.local:8181/catalog",
    "JOBLENS_CATALOG_WAREHOUSE": "joblens",
    "JOBLENS_S3_ENDPOINT": "http://seaweedfs-all-in-one.lakehouse.svc.cluster.local:8333",
    "JOBLENS_USD_VND": "26000",
}
# Khóa S3 trong Secret "joblens-s3" (deploy/lib.sh: s3_secret) – driver nhận qua envFrom, executor qua secretKeyRef
S3_SECRET = "joblens-s3"
S3_SECRET_KEYS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION"]


def spark_submit_script(
    job: str,
    args: list[str] | tuple[str, ...] = (),
    *,
    driver_memory: str = "1g",
    executor_memory: str = "1g",
) -> str:
    """Script bash chạy trong pod driver. Pod phải có biến POD_IP, POD_NAME, JOBLENS_SPARK_IMAGE (task đặt sẵn).

    Số executor đọc lúc chạy từ biến JOBLENS_SPARK_EXECUTORS (Airflow Variable joblens_spark_executors):
    0 → local mode (`--master local[2]`, mọi thứ trong pod này – máy 8 GB, JOBLENS_PROFILE=lite);
    ≥ 1 → client mode trên K8s, pod này là driver, executor là pod riêng.
    """
    if not job.isidentifier():
        raise ValueError(f"Tên job không hợp lệ: {job!r}")
    app = "joblens-" + job.replace("_", "-")
    conf = {
        "spark.kubernetes.namespace": NAMESPACE,
        "spark.kubernetes.container.image": "${JOBLENS_SPARK_IMAGE}",
        "spark.kubernetes.container.image.pullPolicy": "IfNotPresent",
        # client mode: executor gọi ngược về driver theo IP của pod; executor gắn ownerReference vào pod driver
        # nên tự bị xóa khi Airflow xóa pod
        "spark.driver.host": "${POD_IP}",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.port": "7078",
        "spark.blockManager.port": "7079",
        "spark.kubernetes.driver.pod.name": "${POD_NAME}",
        "spark.kubernetes.authenticate.caCertFile": f"{SA_DIR}/ca.crt",
        "spark.kubernetes.authenticate.oauthTokenFile": f"{SA_DIR}/token",
        "spark.executor.instances": "${JOBLENS_SPARK_EXECUTORS}",
        "spark.executor.memory": executor_memory,
        "spark.kubernetes.executor.request.cores": "250m",
        "spark.kubernetes.executor.label.joblens-task": job,
    }
    for k in [*CLUSTER_ENV, "JOBLENS_S3_REGION"]:
        conf[f"spark.executorEnv.{k}"] = "${" + k + "}"
    for k in S3_SECRET_KEYS:
        conf[f"spark.kubernetes.executor.secretKeyRef.{k}"] = f"{S3_SECRET}:{k}"

    job_tail = [f"{JOB_DIR}/{job}.py", *(shlex.quote(a) for a in args)]
    local = [
        SPARK_SUBMIT,
        "--master 'local[2]'",
        f"--name {app}",
        f"--driver-memory {driver_memory}",
        '--conf "spark.sql.shuffle.partitions=4"',
        *job_tail,
    ]
    parts = [
        SPARK_SUBMIT,
        "--master k8s://https://kubernetes.default.svc:443",
        "--deploy-mode client",
        f"--name {app}",
        f"--driver-memory {driver_memory}",
    ]
    # giá trị bọc nháy kép để bash vẫn thay ${...}
    parts += [f'--conf "{k}={v}"' for k, v in conf.items()]
    parts += [
        f'--py-files "$(ls {JOB_DIR}/*.py | paste -sd, -)"',
        *job_tail,
    ]
    sep = " \\\n  "
    return (
        "set -euo pipefail\n"
        'JOBLENS_SPARK_EXECUTORS="${JOBLENS_SPARK_EXECUTORS:-1}"\n'
        'if [ "$JOBLENS_SPARK_EXECUTORS" = "0" ]; then\n'
        "  exec " + sep.join(local) + "\n"
        "fi\n"
        "exec " + sep.join(parts) + "\n"
    )
