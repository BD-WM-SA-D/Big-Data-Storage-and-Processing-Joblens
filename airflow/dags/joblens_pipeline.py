"""
### joblens_pipeline – điều phối tầng batch của nền tảng JobLens (IT4931)

```
silver_job_postings ──┬──> dbt_build_gold        (Trino: seed + gold + kiểm thử dữ liệu)
                      └──> iceberg_maintenance   (compaction + dọn snapshot)
```

- **silver_job_postings** – Spark đọc phần bronze mới (mốc `joblens.bronze_watermark`), chuẩn hóa, `MERGE INTO`
  `silver.job_postings` và ghi ảnh chụp ngày vào `silver.job_postings_snapshot`.
- **dbt_build_gold** – `dbt build` trên Trino: `gold.dim_location`, `gold.mart_salary_role_region` và test dữ liệu;
  test lỗi thì task đỏ, dữ liệu gold sai không được coi là xong.
- **iceberg_maintenance** – gộp file nhỏ do streaming sinh ra, gộp manifest, xóa snapshot cũ (giữ 10 bản gần nhất).

Job streaming `bronze_jobs` (Kafka → bronze) chạy liên tục bên ngoài DAG; DAG chỉ xử lý phần batch phía sau.
Mỗi task là một pod trong namespace `compute`. Image lấy từ Airflow Variable `joblens_spark_image`,
`joblens_dbt_image` – khai báo qua Helm values (`deploy/airflow/install.sh`), không nhập tay trên giao diện.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

import joblens_spark as js

SPARK_IMAGE = "{{ var.value.joblens_spark_image }}"
DBT_IMAGE = "{{ var.value.joblens_dbt_image }}"


def _field_env(name: str, path: str) -> k8s.V1EnvVar:
    return k8s.V1EnvVar(
        name=name,
        value_from=k8s.V1EnvVarSource(
            field_ref=k8s.V1ObjectFieldSelector(field_path=path)
        ),
    )


def spark_task(
    task_id: str, job: str, args: list[str] | None = None, **kw
) -> KubernetesPodOperator:
    """Task chạy một Spark job (pod này là driver, client mode)."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"joblens-{task_id.replace('_', '-')}",
        namespace=js.NAMESPACE,
        service_account_name=js.SERVICE_ACCOUNT,
        image=SPARK_IMAGE,
        image_pull_policy="IfNotPresent",
        cmds=["bash", "-c"],
        arguments=[js.spark_submit_script(job, args or [], **kw)],
        env_vars=[
            _field_env("POD_IP", "status.podIP"),
            _field_env("POD_NAME", "metadata.name"),
            k8s.V1EnvVar(name="JOBLENS_SPARK_IMAGE", value=SPARK_IMAGE),
            *(k8s.V1EnvVar(name=k, value=v) for k, v in js.CLUSTER_ENV.items()),
        ],
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=js.S3_SECRET))
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "1536Mi"},
            limits={"memory": "2Gi"},
        ),
        # cùng nhãn với driver của submit.sh để scripts/demo-ui.sh spark mở được Spark UI (cổng 4040)
        labels={"spark-role": "driver", "app.kubernetes.io/part-of": "joblens"},
        get_logs=True,
        on_finish_action="delete_succeeded_pod",  # pod lỗi giữ lại để xem log
        startup_timeout_seconds=300,
    )


with DAG(
    dag_id="joblens_pipeline",
    description="bronze → silver (Spark) → gold (dbt/Trino) + bảo trì Iceberg",
    schedule="0 * * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["joblens", "it4931"],
    doc_md=__doc__,
) as dag:
    silver = spark_task("silver_job_postings", "silver_job_postings")

    gold = KubernetesPodOperator(
        task_id="dbt_build_gold",
        name="joblens-dbt-build",
        namespace=js.NAMESPACE,
        image=DBT_IMAGE,
        image_pull_policy="IfNotPresent",
        arguments=["build"],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"memory": "768Mi"},
        ),
        labels={"app.kubernetes.io/part-of": "joblens"},
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        startup_timeout_seconds=300,
    )

    maintenance = spark_task("iceberg_maintenance", "maintenance_iceberg")

    silver >> [gold, maintenance]
