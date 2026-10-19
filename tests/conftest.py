"""Cấu hình chung cho test: chạy theo giờ UTC và cho Spark worker dùng đúng Python của môi trường test."""

import os
import sys
import time
from pathlib import Path

os.environ["TZ"] = "UTC"
time.tzset()
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ.pop("JAVA_TOOL_OPTIONS", None)

# Spark worker import được normalize, bronze_jobs… (giống PYTHONPATH=/opt/joblens trong image)
spark_jobs = str(Path(__file__).resolve().parents[1] / "spark-jobs")
sys.path.insert(0, spark_jobs)
os.environ["PYTHONPATH"] = os.pathsep.join(
    filter(None, [spark_jobs, os.environ.get("PYTHONPATH")])
)
