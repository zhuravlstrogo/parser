# https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
#  sudo chmod 777 /var/run/docker.sock
# mkdir logs dags plugins
# sudo chmod -R 777 logs dags plugins
docker compose up airflow-init &&
docker compose up -d &&
docker exec -it  --user root work-airflow-worker-1 bash