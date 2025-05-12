from airflow.decorators import dag, task
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime
from airflow.models import Variable
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
from include.dbt.fraud.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from airflow.models.baseoperator import chain
from cosmos.constants import TestBehavior

AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW = Variable.get('AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW')
AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW = Variable.get('AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW')
AIRBYTE_JOB_ID_RAW_TO_STAGING = Variable.get('AIRBYTE_JOB_ID_RAW_TO_STAGING')

@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['airbyte', 'risk'],
)

def customer_metrics():
    load_customer_transactions_raw = AirbyteTriggerSyncOperator(
        task_id='load_customer_transactions_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_LOAD_CUSTOMER_TRANSACTIONS_RAW
    )

    load_labeled_transactions_raw = AirbyteTriggerSyncOperator(
        task_id='load_labeled_transactions_raw',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_LOAD_LABELED_TRANSACTIONS_RAW
    )

    write_to_staging = AirbyteTriggerSyncOperator(
        task_id='write_to_staging',
        airbyte_conn_id='airbyte',
        connection_id=AIRBYTE_JOB_ID_RAW_TO_STAGING
    )

    @task
    def airbyte_jobs_done():
        return True
    

    audit = DbtTaskGroup(
        group_id='audit',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_ALL,
            select=['resource_type:test']
        )
    )
    
    publish = DbtTaskGroup(
        group_id='publish',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['source:fraud+']
        )
    )

    chain(
        [load_customer_transactions_raw, load_labeled_transactions_raw],
        write_to_staging,
        airbyte_jobs_done(),
        audit,
        publish
    )

customer_metrics()