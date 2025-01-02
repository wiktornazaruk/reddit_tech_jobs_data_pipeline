# Reddit Tech Jobs Data Pipeline

Scrapes job postings from the r/dataengineeringjobs subreddit and stores them in a PostgreSQL database.

## Pipeline Design

![pipline design image](/images/reddit_etl_pipeline.png "Pipeline Design")

## Requirements
- Python 3.x
- Python packages: requests pandas beautifulsoup4 psycopg2-binary
- Docker 
- Airflow

## Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/wiktornazaruk/reddit_tech_jobs_data_pipeline.git
   cd reddit_tech_jobs_data_pipeline
   ```

2. Create virtual environment and activate it (Optional):

    ```sh
    python -m venv venv
    source venv/bin/activate
    ```
3. Install the required Python packages:
   ```sh
   pip install requests pandas beautifulsoup4 psycopg2-binary
   ```

## Important links and Code

### Docker

[Install Docker](https://docs.docker.com/get-started/get-docker/)

### Airflow

[Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#)

Useful commands:

```sh
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.4/docker-compose.yaml'

mkdir -p ./dags ./logs ./plugins ./config

echo -e "AIRFLOW_UID=$(id -u)" > .env

sudo systemctl start docker

sudo docker compose up airflow-init

sudo docker compose up

sudo docker container ls #for getting postgres_id

sudo docker inspect {postgres_id} #useful for copying ip address for postgres connection configuration

sudo docker compose down
```


### PGAdmin (Postgres)

Add code below to docker-compose.yaml file

```
postgres:
    image: postgres:13
    environment:
    POSTGRES_USER: airflow
    POSTGRES_PASSWORD: airflow
    POSTGRES_DB: airflow
    volumes:
    - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
    test: ["CMD", "pg_isready", "-U", "airflow"]
    interval: 10s
    retries: 5
    start_period: 5s
    restart: always
    ports:
    - "5432:5432"

pgadmin:
    container_name: pgadmin4_container2
    image: dpage/pgadmin4
    restart: always
    environment:
    PGADMIN_DEFAULT_EMAIL: admin@admin.com
    PGADMIN_DEFAULT_PASSWORD: root
    ports:
    - "5050:80"
```
