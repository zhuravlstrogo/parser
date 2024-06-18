docker compose up airflow-init &&
docker compose up &&
docker exec -it  --user root work-airflow-worker-1 bash