"""Kiểm thử DAG Airflow joblens_pipeline và lệnh spark-submit do task sinh ra (airflow/dags/)."""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DAGS = ROOT / "airflow" / "dags"
sys.path.insert(0, str(DAGS))
import joblens_spark as js  # noqa: E402


def test_spark_submit_is_client_mode_in_compute():
    s = js.spark_submit_script("silver_job_postings")
    assert (
        "--deploy-mode client" in s and "k8s://https://kubernetes.default.svc:443" in s
    )
    assert '"spark.kubernetes.namespace=compute"' in s
    assert '"spark.driver.host=${POD_IP}"' in s  # executor gọi ngược về pod driver
    assert (
        '"spark.kubernetes.driver.pod.name=${POD_NAME}"' in s
    )  # executor bị xóa theo pod driver
    assert s.rstrip().endswith("/opt/joblens/silver_job_postings.py")
    # khóa S3 không nằm trong lệnh – executor đọc từ Secret
    assert "secretKeyRef.AWS_SECRET_ACCESS_KEY=joblens-s3:AWS_SECRET_ACCESS_KEY" in s
    assert not re.search(r"AWS_SECRET_ACCESS_KEY=\$", s)


def test_spark_submit_quotes_job_args_and_rejects_bad_job():
    s = js.spark_submit_script(
        "maintenance_iceberg", ["--tables", "bronze.jobs_raw; rm -rf /"]
    )
    assert "'bronze.jobs_raw; rm -rf /'" in s
    with pytest.raises(ValueError):
        js.spark_submit_script("silver; rm -rf /")


@pytest.mark.skipif(shutil.which("bash") is None, reason="cần bash")
def test_spark_submit_script_is_valid_bash():
    s = js.spark_submit_script("silver_job_postings", ["--full"], executors=2)
    subprocess.run(["bash", "-n", "-c", s], check=True)


def test_dag_jobs_exist_in_spark_image():
    src = (DAGS / "joblens_pipeline.py").read_text(encoding="utf-8")
    jobs = re.findall(r'spark_task\(\s*"[a-z_]+",\s*"([a-z_]+)"', src)
    assert set(jobs) == {"silver_job_postings", "maintenance_iceberg"}
    for job in jobs:
        assert (ROOT / "spark-jobs" / f"{job}.py").exists()


def test_every_dag_file_is_mounted_by_helm_values():
    values = yaml.safe_load(
        (ROOT / "deploy" / "airflow" / "values.yaml").read_text(encoding="utf-8")
    )
    mounted = {
        m["subPath"] for m in values["volumeMounts"] if m["name"] == "joblens-dags"
    }
    assert mounted == {p.name for p in DAGS.glob("*.py")}


def test_dag_parses_with_airflow():
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS))
    assert bag.import_errors == {}
    dag = bag.dags["joblens_pipeline"]
    assert dag.task_dict["silver_job_postings"].downstream_task_ids == {
        "dbt_build_gold",
        "iceberg_maintenance",
    }
    assert dag.max_active_runs == 1 and not dag.catchup
    for t in dag.tasks:
        assert t.namespace == "compute"
        assert (
            "var.value.joblens_" in t.image
        )  # tag image lấy từ Airflow Variable, không cứng trong DAG
