# Image chạy các Spark job của JobLens trên Kubernetes (spark-submit --master k8s://, docs/project-plan/03).
# Spark 4.1.3 (JDK 17) + Iceberg 1.11.0 + connector Kafka – phiên bản theo joblens/versions.md.
# Thư viện tải sẵn vào image để job không phải tải Maven lúc chạy (tái lập được, chạy được khi offline).
# Build bằng deploy/spark/submit.sh từ GỐC repo (docker build -f spark-jobs/Dockerfile .) – context lọc bởi .dockerignore.
# Gắn tag version-gitsha, không dùng latest.
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

ARG SPARK_VERSION=4.1.3
ARG ICEBERG_VERSION=1.11.0
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.12.1
ARG MAVEN=https://repo1.maven.org/maven2

USER root
RUN set -eux; cd /opt/spark/jars; \
    for coord in \
      "org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/${ICEBERG_VERSION}/iceberg-spark-runtime-4.1_2.13-${ICEBERG_VERSION}.jar" \
      "org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "org/apache/spark/spark-sql-kafka-0-10_2.13/${SPARK_VERSION}/spark-sql-kafka-0-10_2.13-${SPARK_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.13/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.13-${SPARK_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    do \
      f="$(basename "$coord")"; \
      curl -fsSL -o "$f" "${MAVEN}/${coord}"; \
      echo "$(curl -fsSL "${MAVEN}/${coord}.sha1" | cut -c1-40)  $f" | sha1sum -c -; \
    done; \
    chmod 644 /opt/spark/jars/*.jar; \
    pip install --no-cache-dir boto3==1.43.114 confluent-kafka==2.16.0 great-expectations==1.23.2 \
      pymongo==4.18.3 pyarrow==25.0.1
# confluent-kafka: replay VietJobs không cần Spark; great-expectations: task gx_validate_silver (gx_silver.py);
# pymongo: ghi MongoDB (stream_province_windows.py, analytics_spark.py); pyarrow: pandas UDF (analytics_spark.py)

# Mã job + bảng ánh xạ địa điểm (nguồn duy nhất: dbt/seeds/locations_2025.csv)
COPY --chown=spark:spark spark-jobs/*.py /opt/joblens/
COPY --chown=spark:spark dbt/seeds/locations_2025.csv /opt/joblens/locations_2025.csv
ENV PYTHONPATH=/opt/joblens \
    JOBLENS_LOCATIONS_CSV=/opt/joblens/locations_2025.csv
USER spark
