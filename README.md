# Batch Processing with PySpark Assignment

## Overview

Project ini menjalankan pipeline data menggunakan PySpark yang di-deploy
lewat Docker container (spark-master). Dataset disimpan dalam format
Parquet dan di-mount dari local Windows (WSL path `/mnt/e/...`) ke
container.

## Tech Stack

-   PySpark
-   Docker & Docker Compose
-   Parquet files
-   WSL (Windows bind mount)

## Project Structure

    Assignment-Batch-Processing-with-PySpark/
    ├── airflow/
    │   └── data/            # source CSV data (orders, order_items)
    ├── spark/
    │   ├── scripts/         # PySpark batch job (assignment_day_44.py)
    │   └── data/            # output parquet (fact_orders.parquet)
    ├── docker-compose.yaml
    └── README.md

## How to Run

1.  Start container

``` bash
docker compose up -d
```

2.  Masuk ke spark container

``` bash
docker exec -it spark-master bash
```

3.  Jalankan spark job / pyspark script sesuai assignment.

## Notes (Important)

Folder `/spark/data` adalah bind mount dari Windows (`/mnt/e/...`), jadi:
- Permission `chmod` di dalam container tidak akan berpengaruh
- Permission harus diatur dari host (Windows / WSL), bukan dari container

Ini normal behavior kalau pakai Docker + WSL bind mount.

## Output

Hasil transformasi disimpan dalam format Parquet di folder:
`spark/data/fact_orders.parquet`

Folder ini merupakan bind mount ke host, sehingga file Parquet bisa langsung diakses dari Windows dan di-versioning ke GitHub (kecuali file besar, sebaiknya di-ignore via `.gitignore`).

