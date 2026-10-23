# Image chạy dbt (tầng gold) trong cụm – dùng bởi task dbt_build của DAG Airflow joblens_pipeline.
# Phiên bản khớp uv.lock: dbt-core 1.12.6, dbt-trino 1.10.6 (joblens/versions.md: dbt Core 1.x).
# Build bằng deploy/airflow/install.sh:  docker build -f dbt/Dockerfile dbt   (context = thư mục dbt/)
# Profile đọc địa chỉ Trino từ biến môi trường DBT_TRINO_HOST / DBT_TRINO_PORT (profiles.yml).
FROM python:3.12.11-slim

RUN pip install --no-cache-dir dbt-core==1.12.6 dbt-trino==1.10.6 \
    && useradd --create-home --uid 1000 dbt
COPY --chown=dbt:dbt . /opt/dbt
WORKDIR /opt/dbt
USER dbt
ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DBT_PROFILES_DIR=/opt/dbt \
    DBT_TRINO_HOST=trino.serving.svc.cluster.local \
    DBT_TRINO_PORT=8080 \
    DBT_THREADS=1
# Kiểm tra cú pháp project khi build (không cần kết nối Trino)
RUN dbt parse --no-partial-parse
ENTRYPOINT ["dbt"]
CMD ["build"]
