# ------------------------------------------------------------
# Spark JAR Vendor Script (Spark 3.5.x / Scala 2.12)
#
# Purpose: Avoids runtime '--packages' resolution and network drift.
#
# ------------------------------------------------------------
#!/usr/bin/env bash
set -euo pipefail

JAR_DIR="${1:-./spark/jars}"
mkdir -p "${JAR_DIR}"

MAVEN=https://repo1.maven.org/maven2
DELTA_VER="3.3.0"
SPARK_MAJOR="3.5.0"          # matches Spark 3.5.2 line
KAFKA_CLIENT_VER="3.5.0"
HADOOP_AWS_VER="3.3.4"
AWS_SDK_BUNDLE_VER="1.12.300"
POOL2_VER="2.11.1"

download () {
  local url="$1" out="$2"
  if [[ ! -f "$out" ]]; then
    echo "[download] $url -> $out"
    curl -fsSL "$url" -o "$out"
  else
    echo "[ok] already present: $(basename "$out")"
  fi
}

# --- Delta Lake ---
download "${MAVEN}/io/delta/delta-spark_2.12/${DELTA_VER}/delta-spark_2.12-${DELTA_VER}.jar" \
         "${JAR_DIR}/delta-spark_2.12-${DELTA_VER}.jar"
download "${MAVEN}/io/delta/delta-storage/${DELTA_VER}/delta-storage-${DELTA_VER}.jar" \
         "${JAR_DIR}/delta-storage-${DELTA_VER}.jar"

# --- Spark Kafka integration ---
download "${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_MAJOR}/spark-sql-kafka-0-10_2.12-${SPARK_MAJOR}.jar" \
         "${JAR_DIR}/spark-sql-kafka-0-10_2.12-${SPARK_MAJOR}.jar"
download "${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_MAJOR}/spark-token-provider-kafka-0-10_2.12-${SPARK_MAJOR}.jar" \
         "${JAR_DIR}/spark-token-provider-kafka-0-10_2.12-${SPARK_MAJOR}.jar"
download "${MAVEN}/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar" \
         "${JAR_DIR}/kafka-clients-3.5.0.jar"

# --- Hadoop + AWS SDK ---
download "${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar" \
         "${JAR_DIR}/hadoop-aws-3.3.4.jar"
download "${MAVEN}/com/amazonaws/aws-java-sdk-bundle/1.12.300/aws-java-sdk-bundle-1.12.300.jar" \
         "${JAR_DIR}/aws-java-sdk-bundle-1.12.300.jar"

# --- Apache Commons Pool2 (needed by Kafka consumer) ---
download "${MAVEN}/org/apache/commons/commons-pool2/${POOL2_VER}/commons-pool2-${POOL2_VER}.jar" \
         "${JAR_DIR}/commons-pool2-${POOL2_VER}.jar"

echo "[done] JARs ready in ${JAR_DIR}"
