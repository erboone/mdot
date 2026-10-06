# mdot
MERFISH data orchestration tool
Apache Airflow pipeline for automatically shuffling large datasets between hosts in our laboratory.

## Permissions
Include the path to the rclone config file; for folks in platform 3, email Eric Boone (erboone@health.ucsd.edu) for a config file for our servers.

## Startup
Start server
```bash
docker compose up -d --build
```

Start worker
```bash
QUEUE=<queue_name> docker compose up -d --build --profile worker
```