"""
### joblens_pipeline – điều phối tầng batch của nền tảng JobLens (IT4931)

```
silver_job_postings → silver_job_history → bronze_reviews → gx_validate_silver ─┬→ mining_enabled → mining_skills
                                                                                │    → mining_dedup → mining_absa
                                                                                │    → dbt_build_gold (none_failed)
                                                                                └→ spark_analytics → iceberg_maintenance
```

- **silver_job_postings** – Spark đọc phần bronze mới (mốc `joblens.bronze_watermark`), chuẩn hóa, `MERGE INTO`
  `silver.job_postings` và ghi ảnh chụp ngày vào `silver.job_postings_snapshot`.
- **silver_job_history** – Spark tính lại lịch sử phiên bản (SCD loại 2) của các tin có bản crawl mới, từ mọi lần
  crawl trong bronze, rồi một lệnh `MERGE INTO` vào `silver.job_postings_history` (chỉ ghi dòng thay đổi).
- **bronze_reviews** – Spark đọc review thô `reviews.raw.*.v1` → `bronze.reviews_raw` (MERGE chỉ thêm, không trùng);
  tạo sẵn 3 bảng silver mà image Web Mining ghi (rỗng nếu chưa chạy image).
- **mining_enabled → mining_skills → mining_dedup → mining_absa** – image mô hình của Web Mining (ADR 0001 D3):
  `skill-extractor` → `silver.job_skills`, `job-dedup` → `silver.job_dedup`, `absa-scorer` → `silver.review_aspects`.
  Variable `joblens_mining_mode` = `off` (mặc định) thì bỏ qua cả ba, gold vẫn chạy; `stub`/`full` do
  `deploy/mining/install.sh` đặt. Repo này chỉ lên lịch và cấp cấu hình, không ghi các bảng đó.
- **gx_validate_silver** – Great Expectations kiểm tra silver (khóa, danh mục, khoảng lương, mã tỉnh, độ phủ);
  có kỳ vọng hỏng thì task đỏ và hai task sau không chạy – gold giữ bản cũ, không bị ghi đè bằng dữ liệu sai.
- **dbt_build_gold** – `dbt build` trên Trino: `gold.dim_location`, `gold.mart_salary_role_region`, hai mart tăng dần
  `gold.mart_hiring_weekly`, `gold.mart_job_changes_weekly` (chỉ tính lại tuần có thay đổi), 4 bảng từ kết quả Web
  Mining (`dim_skill`, `mart_skill_demand_monthly`, `dim_company`, `mart_company_aspect_sentiment`) và test dữ liệu;
  test lỗi thì task đỏ, dữ liệu gold sai không được coi là xong.
- **spark_analytics** – phân tích bằng Spark (pivot, hàm tổng hợp tùy biến, broadcast/sort-merge join, window,
  thống kê, MLlib) → MongoDB `analytics_*`; kế hoạch thực thi lưu ở `s3://joblens/reports/spark-plans/`.
- **iceberg_maintenance** – gộp file nhỏ do streaming sinh ra, gộp manifest, xóa snapshot cũ (giữ 10 bản gần nhất).

Job streaming `bronze_jobs` (Kafka → bronze) chạy liên tục bên ngoài DAG; DAG chỉ xử lý phần batch phía sau.
Mỗi task là một pod trong namespace `compute`. Image lấy từ Airflow Variable `joblens_spark_image`,
`joblens_dbt_image` – khai báo qua Helm values (`deploy/airflow/install.sh`), không nhập tay trên giao diện.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

import joblens_spark as js

SPARK_IMAGE = "{{ var.value.joblens_spark_image }}"
DBT_IMAGE = "{{ var.value.joblens_dbt_image }}"
# 0 = Spark local mode trong pod task (máy 8 GB, JOBLENS_PROFILE=lite); mặc định 1 executor
SPARK_EXECUTORS = "{{ var.value.get('joblens_spark_executors', '1') }}"
# Image mô hình Web Mining (deploy/mining/install.sh): off | stub | full – off thì 3 task mô hình bị bỏ qua
MINING_MODE = "{{ var.value.get('joblens_mining_mode', 'off') }}"
# Chỉ đưa tin crawl thật vào mô hình (--sources, WM 0.1.1/0.2.1): bỏ vietjobs và dữ liệu mô phỏng synthetic – bản mô
# phỏng trùng tiêu đề + công ty với tin VietJobs gốc nên job-dedup sẽ gộp chúng
MINING_SOURCES = "{{ var.value.get('joblens_mining_sources', 'topcv,itviec') }}"
# Chạy tăng dần: chỉ tin có updated_ts (do silver_job_postings đặt lúc MERGE) từ lần chạy thành công trước. Gối lên
# phần của lần trước một chút là chủ ý – image ghi bằng "xóa theo khóa rồi ghi" nên xử lý lại không sinh trùng.
# Lần chạy đầu (chưa có lần thành công) truyền chuỗi rỗng → image xử lý toàn bộ.
CHANGED_SINCE = (
    "{{ prev_start_date_success.isoformat() if prev_start_date_success else '' }}"
)


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
            k8s.V1EnvVar(name="JOBLENS_SPARK_EXECUTORS", value=SPARK_EXECUTORS),
            *(k8s.V1EnvVar(name=k, value=v) for k, v in js.CLUSTER_ENV.items()),
        ],
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=js.S3_SECRET)),
            # MongoDB (deploy/mongodb); optional → task không cần MongoDB vẫn chạy khi chưa cài
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name=js.MONGO_SECRET, optional=True)
            ),
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


def mining_enabled(mode: str) -> bool:
    return mode in ("stub", "full")


def mining_task(
    task_id: str,
    image_var: str,
    input_table: str,
    output_table: str,
    extra_args: list[str] | None = None,
    memory: str = "1Gi",
) -> KubernetesPodOperator:
    """Task chạy image mô hình của Web Mining theo hợp đồng ADR 0001 D3. Image đọc/ghi Iceberg bằng pyiceberg qua
    REST catalog (biến JOBLENS_*), chỉ ghi bảng của WM; repo này chỉ cấp cấu hình, bảng và lịch chạy."""
    return KubernetesPodOperator(
        task_id=task_id,
        name=f"joblens-{task_id.replace('_', '-')}",
        namespace=js.NAMESPACE,
        image="{{ var.value.joblens_" + image_var + "_image }}",
        image_pull_policy="IfNotPresent",  # image nạp thẳng vào k3d (deploy/mining/install.sh), không có registry
        arguments=[
            "--input-table",
            input_table,
            "--output-table",
            output_table,
            "--run-id",
            "{{ run_id }}",
            "--sources",
            MINING_SOURCES,
            *(extra_args or []),
        ],  # fmt: skip
        env_vars=[k8s.V1EnvVar(name=k, value=v) for k, v in js.CLUSTER_ENV.items()],
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=js.S3_SECRET))
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"},
            limits={"memory": memory},
        ),
        labels={"app.kubernetes.io/part-of": "joblens", "joblens/owner": "web-mining"},
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
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

    history = spark_task("silver_job_history", "silver_job_history")

    # review thô Kafka → bronze.reviews_raw; tạo sẵn bảng silver của WM (rỗng) để dbt luôn có nguồn
    reviews = spark_task("bronze_reviews", "bronze_reviews")

    mining_gate = ShortCircuitOperator(
        task_id="mining_enabled",
        python_callable=mining_enabled,
        op_kwargs={"mode": MINING_MODE},
        # chỉ bỏ qua task ngay sau; các task sau tự tính theo trigger rule → dbt_build_gold (none_failed) vẫn chạy
        ignore_downstream_trigger_rules=False,
    )
    # skill-extractor bản ml chạy PhoBERT trên CPU (image 3,4 GB) → giới hạn 4 Gi; hai job còn lại không dùng torch
    skills = mining_task(
        "mining_skills", "skill_extractor", "silver.job_postings", "silver.job_skills",
        extra_args=["--changed-since", CHANGED_SINCE], memory="4Gi",
    )  # fmt: skip
    # dedup cần cả kho tin crawl để so cặp → không chạy tăng dần (WM không nhận --changed-since cho job này)
    dedup = mining_task(
        "mining_dedup", "job_dedup", "silver.job_postings", "silver.job_dedup"
    )
    # absa: KHÔNG truyền --changed-since – mốc của job này là crawl_ts (thời điểm crawl), mà review crawl từ trước
    # rồi mới gửi vào Kafka (replay) có crawl_ts cũ hơn lần chạy trước nên sẽ bị bỏ sót. Review ít (vài trăm) nên
    # xử lý lại toàn bộ mỗi lần; đề nghị WM đổi mốc sang ingest_ts (cột của bronze) – issue #2.
    absa = mining_task(
        "mining_absa", "absa_scorer", "bronze.reviews_raw", "silver.review_aspects"
    )

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
        trigger_rule="none_failed",  # chạy cả khi 3 task mô hình bị bỏ qua (chế độ off), không chạy khi có lỗi
    )

    quality = spark_task("gx_validate_silver", "gx_silver")

    analytics = spark_task("spark_analytics", "analytics_spark")

    maintenance = spark_task("iceberg_maintenance", "maintenance_iceberg")

    # tuần tự để vừa máy 8 GB: không chạy hai job Spark cùng lúc (dbt và image stub nhẹ, chạy song song với Spark)
    silver >> history >> reviews >> quality >> [mining_gate, analytics]
    mining_gate >> skills >> dedup >> absa >> gold
    analytics >> maintenance
