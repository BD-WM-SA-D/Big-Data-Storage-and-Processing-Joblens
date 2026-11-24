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
    s = js.spark_submit_script("silver_job_postings", ["--full"])
    subprocess.run(["bash", "-n", "-c", s], check=True)


@pytest.mark.skipif(shutil.which("bash") is None, reason="cần bash")
@pytest.mark.parametrize("executors,expect", [("0", "local[2]"), ("2", "k8s://")])
def test_executors_variable_picks_local_or_k8s(tmp_path, executors, expect):
    # thay spark-submit bằng script in tham số để xem nhánh nào được chạy
    fake = tmp_path / "spark-submit"
    fake.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@"\n')
    fake.chmod(0o755)
    s = js.spark_submit_script("silver_job_postings", ["--full"]).replace(
        js.SPARK_SUBMIT, str(fake)
    )
    env = {
        "PATH": "/usr/bin:/bin",
        "JOBLENS_SPARK_EXECUTORS": executors,
        "POD_IP": "10.0.0.1",
        "POD_NAME": "p",
        "JOBLENS_SPARK_IMAGE": "img",
        "JOBLENS_S3_REGION": "us-east-1",
        **js.CLUSTER_ENV,
    }
    out = subprocess.run(
        ["bash", "-c", s], env=env, capture_output=True, text=True, check=True
    ).stdout
    assert expect in out and out.rstrip().endswith("--full")
    if executors != "0":
        assert "spark.executor.instances=2" in out


def test_dag_jobs_exist_in_spark_image():
    src = (DAGS / "joblens_pipeline.py").read_text(encoding="utf-8")
    jobs = re.findall(r'spark_task\(\s*"[a-z_]+",\s*"([a-z_]+)"', src)
    assert set(jobs) == {
        "silver_job_postings",
        "silver_job_history",
        "bronze_reviews",
        "gx_silver",
        "analytics_spark",
        "maintenance_iceberg",
    }
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
        "silver_job_history"
    }
    assert dag.task_dict["silver_job_history"].downstream_task_ids == {"bronze_reviews"}
    assert dag.task_dict["bronze_reviews"].downstream_task_ids == {"gx_validate_silver"}
    assert dag.task_dict["gx_validate_silver"].downstream_task_ids == {
        "mining_enabled",
        "spark_analytics",
    }
    chain = [
        "mining_enabled",
        "mining_skills",
        "mining_dedup",
        "mining_absa",
        "dbt_build_gold",
    ]
    for a, b in zip(chain, chain[1:], strict=False):
        assert dag.task_dict[a].downstream_task_ids == {b}
    assert dag.task_dict["dbt_build_gold"].trigger_rule == "none_failed"
    assert dag.task_dict["mining_enabled"].ignore_downstream_trigger_rules is False
    assert dag.task_dict["spark_analytics"].downstream_task_ids == {
        "iceberg_maintenance"
    }
    assert dag.max_active_runs == 1 and not dag.catchup
    for t in dag.tasks:
        if t.task_id == "mining_enabled":
            continue  # ShortCircuitOperator chạy trong Airflow, không phải pod
        assert t.namespace == "compute"
        assert (
            "var.value.joblens_" in t.image
        )  # tag image lấy từ Airflow Variable, không cứng trong DAG


def test_mining_tasks_follow_d3_contract():
    """Image WM nhận đúng tham số ADR 0001 D3 và chỉ ghi bảng của WM; chế độ off thì bị bỏ qua."""
    src = (DAGS / "joblens_pipeline.py").read_text(encoding="utf-8")
    calls = re.findall(
        r'mining_task\(\s*"(\w+)",\s*"(\w+)",\s*"([\w.]+)",\s*"([\w.]+)"', src
    )
    assert {c[3] for c in calls} == {
        "silver.job_skills",
        "silver.job_dedup",
        "silver.review_aspects",
    }
    assert (
        "mining_absa",
        "absa_scorer",
        "bronze.reviews_raw",
        "silver.review_aspects",
    ) in calls
    for flag in ('"--input-table"', '"--output-table"', '"--run-id"'):
        assert flag in src
    sys.path.insert(0, str(DAGS))
    ns: dict = {}
    exec(  # noqa: S102 – chỉ lấy hàm thuần, không cần cài Airflow
        re.search(r"def mining_enabled\(.*?\n\n", src, re.S).group(0), ns
    )
    assert [ns["mining_enabled"](m) for m in ("off", "stub", "full", "")] == [
        False,
        True,
        True,
        False,
    ]


def test_mining_tasks_filter_sources_and_only_skills_is_incremental():
    """WM 0.1.1/0.2.1: chỉ đưa tin crawl vào mô hình; chỉ skill-extractor chạy tăng dần (absa lọc theo crawl_ts
    sẽ bỏ sót review gửi muộn, dedup cần cả kho tin)."""
    src = (DAGS / "joblens_pipeline.py").read_text(encoding="utf-8")
    assert '"--sources",\n            MINING_SOURCES,' in src
    assert "'joblens_mining_sources', 'topcv,itviec'" in src
    calls = {
        m.group(1): m.group(0)
        for m in re.finditer(r'mining_task\(\s*"(\w+)".*?\)', src, re.S)
    }
    assert (
        "--changed-since" in calls["mining_skills"]
        and 'memory="4Gi"' in calls["mining_skills"]
    )
    assert (
        "--changed-since" not in calls["mining_dedup"]
        and "--changed-since" not in calls["mining_absa"]
    )
    assert "prev_start_date_success" in src
