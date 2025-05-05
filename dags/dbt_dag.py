from airflow import DAG 
from airflow.operators.bash import BashOprerator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime


project_dir = "~\Desktop\e2e_ELT\snowflake_dbt"
profile_dir = "~\.dbt"

# Default args for the DAG
default_args = {
    'owner' : 'dbt_airflow',
    'depends_on_past' : False,
    'retries' : 1
}

# Define the DAG
with DAG(
    'dbt_snowflake_dag',
    default_args = default_args,
    description = 'A simple pipeline to run dbt commands on Snowflake',
    schedule_interval = '@daily',
    start_date = datetime(2025,5,5),
    catchup = False,
) as dag:
    
    start = EmptyOperator(task_id = 'start_dag')


    with TaskGroup(group_id = 'dbt_tasks') as dbt_tasks:
        dbt_debug = BashOprerator(
            task_id = 'dbt_debug',
            bash_command = f'dbt debug --profile-dir {profile_dir} --project-dir {project_dir}',
        )
        dbt_run = BashOprerator(
            task_id = 'dbt_run',
            bash_command = f'dbt run --profile-dir {profile_dir} --project-dir {project_dir}',
        )
        dbt_test = BashOprerator(
            task_id = 'dbt_test',
            bash_command = f'dbt test --profile-dir {profile_dir} --project-dir {project_dir}',
        )

        dbt_debug >> dbt_run >> dbt_test
    
    end = EmptyOperator(task_id = 'end_dag')

    start >> dbt_tasks >> end