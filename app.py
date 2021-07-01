import os
import sys
import subprocess
from subprocess import call


def get_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))
path = get_script_path()

def run_script_copy_dags() -> None:
    try:
        with open(f'{path}/airflow_dag/script_copy_dags_runme.sh', 'rb') as file:
            script = file.read()
        rc = call(script, shell=True)
        print("ran copy dag script")
    except Exception as e:
        print("log: failed to run copy dag script: ", e)

def run_script_start_airflow() -> None:
    try:
        with open(f'{path}/airflow_dag/script_start_airflow.sh', 'rb') as file:
            script = file.read()
        rc = call(script, shell=True)
        print("ran start airflow script, it should be up and running")
    except Exception as e:
        print("log: failed to run start airflow script: ", e)


if __name__ == '__main__':
    run_script_copy_dags()
    run_script_start_airflow()
