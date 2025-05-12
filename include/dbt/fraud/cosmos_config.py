from cosmos.config import ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from pathlib import Path

DBT_CONFIG = ProfileConfig(profile_name='fraud',
                               target_name='dev',
                               profile_mapping=SnowflakeUserPasswordProfileMapping(conn_id='snowflake_conn',
                                                                            profile_args={
                                                                                'database': 'data_eng_dbt',
                                                                                'schema': 'prod'
                                                                            },
                               ))

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/opt/airflow/include/dbt/fraud/',
)