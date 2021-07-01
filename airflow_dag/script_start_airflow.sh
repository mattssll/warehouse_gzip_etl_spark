echo "initializing airflow database, using mysql by default"
airflow db init
echo "creating airflow username: admin, password:admin"
airflow users create \
    --username admin \
    --firstname first \
    --lastname name \
    --role Admin \
    --email lastname@gmail.com \
    --password admin
echo "successfully created user - username: admin, password:admin"

echo "starting airflow webserver on detached on port 8080"
airflow webserver --port 8080 -D
echo "airflow webserver was successfully started"
echo "starting airflow scheduler on detached mode"
airflow scheduler -D
echo "airflow scheduler was successfully started"
echo "your airflow is up and running, go to localhost:8080 to see it runnning"