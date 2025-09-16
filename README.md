# data-pipeline-challenge
The Real-Time Learning Analytics Pipeline Challenge
This project builds a containerized, end-to-end real-time + batch data pipeline for LMS events, using:
* Python Producer → Kafka → Spark Streaming → MinIO (Delta Lake Bronze) → Airflow DAG → Spark Batch → MinIO (Delta Lake Silver) → Analytics Notebook
* Tech stack: Spark 3.5.2, Delta Lake 3.3.0, Kafka (Confluent), MinIO (S3A), Airflow, Postgres, Redis, Docker Compose
* Apple Silicon: Spark containers run under `linux/amd64` (emulation) for stability; everything else runs natively on `arm64`.
* No runtime Ivy: All required JARs are pre-downloaded and auto-loaded via `spark-defaults.conf`.
  * If you use `--packages` in `spark-submit`, Spark talks to Maven Central through Ivy, downloads jars into a hidden cache inside the container, and wires them into the classpath. While it can work fine, it has drawbacks that I've experienced:
    * It’s slow (resolves every time unless cached).
    * It can drift if the upstream version gets re-published.
    * On Mac ARM64 under emulation, sometimes it fails outright.
       * On Mac (ARM64), Ivy sometimes tries to resolve the Scala build or an incorrect binary variant. By pre-downloading JARs, you avoid that.

If you’re on Windows or Linux, most steps are the same (Docker Desktop / Compose). This project was built and tested on Mac ARM64.
Project description: [here](https://neurealities.atlassian.net/wiki/spaces/PD/pages/567410703/Data+Pipelines+Learning+Project)

# 1. Prerequisites
Already have these? Great — skip the corresponding step.

1.1 Homebrew (package manager)
```
# Add brew to your PATH

/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew --version
```
1.2 Java 11 (required for local PySpark in the validation step) 
```
# Install Java 11 (Temurin is best for macOS; Linux/Windows users can install openjdk-11)

brew install --cask temurin@11
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
java -version   # should show 11.x
```
1.3 Docker Desktop
* [Download Docker Desktop and install](https://docs.docker.com/desktop/)
* Launch Docker Desktop once so it initializes.
* In Settings → Resources, allocate at least 8 CPUs and 12 GB RAM. (EG Note: This is a starting point. It hasn't yet proven sufficient to run all processes simultaneousl, so I'm still testing)

# 2. Clone & Prepare the Project
2.1 Get the code
```
git clone https://github.com/neuRealities/data-pipeline-challenge.git data-pipeline-challenge
cd data-pipeline-challenge
```
2.2 Copy environment template
```
cp .env.example .env
```
2.3 Pre-download Spark JARs (no runtime downloads)
```
./spark/download_jars.sh
ls -1 spark/jars
```
Why this matters: Spark will find these jars automatically via `spark/conf/spark-defaults.conf`, so `spark-submit` won’t try to download anything at runtime.

# 3. Build & Start Services
3.1 Infrastructure stack
```
docker compose up -d zookeeper kafka minio postgres redis spark-master spark-worker
```
3.2 MinIO bucket (automated)
```
docker compose run --rm minio-setup
```
This creates the datalake bucket — no UI clicks required.

3.3 Initialize Airflow DB + admin user
```
docker compose --profile init up --abort-on-container-exit airflow-init
```
3.4 Build custom image (Dockerfile.airflow-worker)
```
docker compose build airflow-webserver airflow-scheduler airflow-worker
```
3.5 Start Airflow
```
docker compose up -d airflow-webserver airflow-scheduler airflow-worker
```
3.6 Check services
```
docker compose ps
```
3.7 Sanity check - JARs visible in Spark Master
```
docker compose exec spark-master ls /opt/extra-jars
```
3.8 Sanity check - Airflow worker has Java & Spark wired
```
docker compose exec airflow-worker bash -lc 'java -version; which java; echo $JAVA_HOME'
docker compose exec airflow-worker bash -lc 'spark-submit --version'
```
Services started:
* Kafka + Zookeeper
* MinIO (S3A) + MinIO Console
* Spark master/worker (Spark 3.5.2, forced `linux/amd64` on Apple Silicon)
* Airflow (webserver, scheduler, worker, db, redis)
* Producer (Kafka event generator)

UIs
* Airflow: [http://localhost:8080](http://localhost:8080/) (admin/admin)
* Spark Master: [http://localhost:8081](http://localhost:8081/) (admin/admin)
* MinIO Console: [http://localhost:9001](http://localhost:9001/) (minioadmin/minioadmin)

(See `access-info.txt` for ports and default credentials.)

# 4. Run Pipeline
4.1 Producer → Kafka
```
docker compose --profile producer up -d ct-producer
```
This container emits LMS events into Kafka topic `lms_events`.

4.2 See messages reaching Kafka
```
docker compose exec kafka \
  kafka-console-consumer \
    --bootstrap-server kafka:29092 \
    --topic lms_events \
    --from-beginning \
    --max-messages 5
```
4.3 Bronze Writer (streaming job to Delta Lake)
```
docker compose exec spark-master \
  spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/bronze_writer.py
```
Leave this running to continuously write Bronze data. (EG Note: Still working on getting the bronze writer to run properly alongside the next pipeline step. For now, I let it run for a few minutes then pause it manually so the rest works. Goal is to have it running continuously.)

# 5. Run Silver Transform via Airflow
5.1 Open Airflow: [http://localhost:8080](http://localhost:8080/)

5.2 In the DAG list, unpause bronze_to_silver_lms

5.3 Trigger a run

5.4 Follow tasks:
* `ensure_buckets` (creates datalake/* buckets if missing)
* `precheck_bronze_exists` (logs Bronze path contents)
* `silver_transform` (runs Spark batch job)
* `postcheck_silver_exists` (logs Silver path contents)

Where data lands:
* Silver: `s3a://datalake/silver/user_progress`
* Checkpoints: `s3a://datalake/checkpoints/silver_transform`

# 6. Validate the Pipeline (Notebook)
6.1 Create a Python virtual environment
```
python3 -m venv .venv 
source .venv/bin/activate 
pip install --upgrade pip
```
Why?
* Keeps this project’s Python packages separate.
* Prevents version conflicts with other projects.
* Easy reset: just delete .venv/ and recreate.

Each new terminal session:
```
source .venv/bin/activate
```
Deactivate when finished:
```
deactivate
```
6.2 Install required Python packages
```
pip install jupyterlab==4.2.5 pyspark==3.5.3 delta-spark==3.3.0 pandas==2.2.2 matplotlib==3.8.4
```
6.3 Run Jupyter
```
python3 -m jupyterlab notebooks/validate.ipynb
```
This opens the notebook in your browser. Run each cell top-to-bottom.
# 7. Stop the Stack
```
docker compose down
```
Nuke everything
```
docker compose down -v --remove-orphans || true
docker network prune -f
docker container prune -f
docker volume prune -f
docker image prune -f
docker builder prune -f
```
