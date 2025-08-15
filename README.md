# Dockerized Airflow Stock Data Pipeline

Kindly View the working Demonstration of this project from this video, (Its from my Youtube Channel): https://youtu.be/g7HrtCQn0Ds


This project demonstrates a fully Dockerized **Airflow + PostgreSQL** pipeline that automatically fetches stock market data from [Alpha Vantage](https://www.alphavantage.co/) and updates a PostgreSQL database.  

The pipeline is modular, scalable, and includes robust error handling.

---

## Features

- Fetch stock data for multiple symbols (e.g., IBM, AAPL, GOOGL) from Alpha Vantage.
- Update PostgreSQL table (`stock_data`) with daily stock prices.
- Error handling and retry logic for API failures and missing data.
- Fully Dockerized with Docker Compose.
- Environment variables for secure API keys and database credentials.
- Idempotent pipeline design.

---

## Prerequisites

- Docker & Docker Compose installed
- Python (for local scripts, optional)
- An **Alpha Vantage API key** stored in a `.env` file

Example `.env`:

```env
# API Configuration
API_KEY=YOUR_ALPHA_VANTAGE_API_KEY

# Database Configuration
DB_USER=airflow
DB_PASSWORD=airflow
DB_NAME=stocks
AIRFLOW_DB_NAME=airflow
```

**Important:** `.env` is ignored in this repository for security.

## Getting Started

### 1️⃣ Stop and remove all running containers

```bash
docker-compose down
```

### 2️⃣ Remove the PostgreSQL volume
Check existing volumes:

```bash
docker volume ls
```

Then remove the Airflow/Postgres volume (replace `project_pgdata` with your actual volume name):

```bash
docker volume rm project_pgdata
```

### 3️⃣ Rebuild containers

```bash
docker-compose build --no-cache
```

### 4️⃣ Start the containers

```bash
docker-compose up -d
```

* `-d` runs in detached mode.
* This will start PostgreSQL first, then Airflow.

### 5️⃣ Initialize Airflow DB
Check your Airflow container name:

```bash
docker ps
```

Then run inside the Airflow container:

```bash
docker exec -it <airflow_container_name> airflow db init
```

Usually, your `docker-compose.yml` command does this automatically, but running manually ensures a fresh start.

### 6️⃣ Create Airflow admin user

```bash
docker exec -it <airflow_container_name> airflow users create \
    --username airflow --password airflow \
    --firstname Airflow --lastname User \
    --role Admin \
    --email admin@example.com
```

### 7️⃣ Verify DAGs in UI
* Open: http://localhost:8080
* You should see your `stock_data_pipeline` DAG listed.

### 8️⃣ Trigger DAG manually (optional)

```bash
docker exec -it <airflow_container_name> airflow dags trigger stock_data_pipeline
```

Then watch logs:

```bash
docker logs -f <airflow_container_name>
```

### 9️⃣ Verify database
Connect to PostgreSQL to check that `stocks` table exists and contains records:

```bash
docker exec -it <postgres_container_name> psql -U airflow -d stocks
```

```sql
SELECT * FROM stock_data LIMIT 10;
```

## Next Steps

* The DAG is scheduled `@daily` to automatically fetch every day.
* Add more stock symbols or customize pipeline logic as needed.
* You can enhance error handling and database concurrency logic for multiple symbols.

## Notes

* `.env` file is ignored for security—make sure you create your own locally.
* The pipeline is idempotent: running it multiple times will update existing records without duplication.

