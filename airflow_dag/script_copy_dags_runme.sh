SCRIPTFILE=`realpath $0`
SCRIPTPATH=`dirname $SCRIPT`

export AIRFLOW__CORE__KILLED_TASK_CLEANUP_TIME=604800 # so our download can finish without airflow killing the operator
rm -r ./env/lib/python3.8/site-packages/airflow/example_dags/*.py | true
sleep 1
cp ./airflow_dag/run_spark_app.py ./env/lib/python3.8/site-packages/airflow/example_dags/
