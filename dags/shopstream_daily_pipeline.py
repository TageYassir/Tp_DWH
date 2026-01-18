"""
shopstream_daily_pipeline.py
DAG Airflow pour le pipeline quotidien ShopStream

Ce DAG orchestre :
1. Extraction PostgreSQL vers S3
2. Chargement S3 vers Snowflake Staging
3. Transformations dbt (staging vers core vers marts)
4. Tests de qualité dbt

Emplacement : airflow/dags/shopstream_daily_pipeline.py
Auteur : Data Engineering Team
Date : 2025-11-27
"""
from datetime import datetime, timedelta
from airflow import DAG
# use provider-standard imports to avoid deprecation warnings
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Robust SnowflakeOperator import with fallback to a lightweight operator using SnowflakeHook.
try:
    from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
except Exception:
    try:
        # older provider naming (if present)
        from airflow.providers.snowflake.operators.snowflake_operator import SnowflakeOperator
    except Exception:
        # Fallback: define a minimal operator that uses SnowflakeHook to run SQL.
        # use recommended BaseOperator import to avoid deprecation warnings
        from airflow.sdk.bases.operator import BaseOperator
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        class SnowflakeOperator(BaseOperator):
            """
            Minimal fallback SnowflakeOperator that executes SQL via SnowflakeHook.
            Accepts the same 'snowflake_conn_id' and 'sql' args used below.
            """
            template_fields = ("sql",)

            def __init__(self, *, sql: str, snowflake_conn_id: str = "snowflake_default", autocommit: bool = False, **kwargs):
                super().__init__(**kwargs)
                self.sql = sql
                self.snowflake_conn_id = snowflake_conn_id
                self.autocommit = autocommit

            def execute(self, context):
                hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
                self.log.info("Running Snowflake SQL via fallback SnowflakeOperator")
                hook.run(self.sql, autocommit=self.autocommit)
                self.log.info("Snowflake SQL execution finished")

# Configuration du DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 1),
    'email': ['alerts@shopstream.com'],
    # disable automatic email on failure to avoid SMTP ConnectionRefused while SMTP not configured
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

dag = DAG(
    'shopstream_daily_pipeline',
    default_args=default_args,
    description='Pipeline quotidien ShopStream : PostgreSQL vers S3 vers Snowflake vers dbt vers BI',
    schedule='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,
    tags=['production', 'daily', 'shopstream']
)

# Tâches Python
def extract_postgres_to_s3(**context):
    """Extrait les données PostgreSQL et les pousse dans S3.

    Cette version passe explicitement les paramètres de connexion au script export_to_s3.py
    via des arguments CLI (priorité) — ce qui évite que le script utilise un 'localhost' en dur.
    """
    import subprocess
    import sys
    import os

    def _running_in_docker():
        try:
            if os.path.exists('/.dockerenv'):
                return True
            with open('/proc/1/cgroup', 'rt') as f:
                content = f.read()
                if 'docker' in content or 'kubepods' in content or 'containerd' in content:
                    return True
        except Exception:
            pass
        return False

    execution_date = context['ds']  # Date d'exécution (YYYY-MM-DD)
    print(f"Extraction PostgreSQL vers S3 pour {execution_date}")

    script_path = '/opt/airflow/scripts/export_to_s3.py'
    if not os.path.exists(script_path):
        raise FileNotFoundError(
            f"Export script not found inside container at {script_path}. "
            "Verify docker-compose mounts ${AIRFLOW_PROJ_DIR}/scripts to /opt/airflow/scripts"
        )

    # Résolution de l'hôte Postgres : préférence explicite via DB_HOST, sinon host.docker.internal si dans Docker
    explicit_db_host = os.environ.get('DB_HOST')
    if explicit_db_host:
        container_db_host = explicit_db_host
        print(f"Using DB_HOST from environment: {container_db_host}")
    else:
        if _running_in_docker():
            container_db_host = os.environ.get('DB_HOST', 'host.docker.internal')
            print("Detected Docker runtime. Defaulting to host.docker.internal for Postgres host "
                  f"(override with DB_HOST env var). Using: {container_db_host}")
        else:
            container_db_host = os.environ.get('DB_HOST', 'localhost')
            print("Not running in Docker. Defaulting to localhost for Postgres host "
                  f"(override with DB_HOST env var). Using: {container_db_host}")

    container_db_port = os.environ.get('DB_PORT', '5432')
    container_db_user = os.environ.get('DB_USER', '')
    container_db_name = os.environ.get('DB_NAME', '')
    # Do NOT print DB_PASSWORD in logs
    has_db_password = bool(os.environ.get('DB_PASSWORD'))

    # Build CLI command with explicit args so export_to_s3.py cannot fallback to a hardcoded localhost.
    cmd = [
        sys.executable, '-u', script_path,
        '--db-host', container_db_host,
        '--db-port', container_db_port,
        '--db-user', container_db_user,
        '--db-name', container_db_name,
        '--date', execution_date
    ]

    # If there is a password, pass via env (safer than CLI). We still pass everything for clarity.
    env = {
        **os.environ,
        "PYTHONUNBUFFERED": "1",
        "DB_HOST": container_db_host,
        "DB_PORT": container_db_port,
        "DB_USER": container_db_user,
        "DB_NAME": container_db_name,
    }

    print("Running export command: " + " ".join(cmd))
    print(f"Effective DB host: {container_db_host}, port: {container_db_port}, user: {container_db_user}, db: {container_db_name}, has_password: {has_db_password}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
    )

    # Always print outputs for diagnostics
    if result.stdout:
        print("export stdout:")
        print(result.stdout)
    if result.stderr:
        print("export stderr:")
        print(result.stderr)

    if result.returncode != 0:
        raise Exception(
            f"Erreur lors de l'export : returncode={result.returncode}\n"
            f"stdout:\n{result.stdout or '<empty>'}\n\nstderr:\n{result.stderr or '<empty>'}"
        )

    print("Export terminé avec succès")
    """Extrait les données PostgreSQL et les pousse dans S3.

    Modification importante : si ce code tourne DANS un conteneur Docker,
    il préfère par défaut se connecter à la base PostgreSQL située HORS du conteneur
    en utilisant l'hôte `host.docker.internal` (ou la valeur fournie via la variable d'environnement DB_HOST).
    Ceci évite d'utiliser une instance Postgres locale au sein du même conteneur.
    """
    import subprocess
    import sys
    import os

    def _running_in_docker():
        # Heuristiques simples pour détecter l'exécution dans Docker
        try:
            if os.path.exists('/.dockerenv'):
                return True
            # /proc/1/cgroup check
            with open('/proc/1/cgroup', 'rt') as f:
                content = f.read()
                if 'docker' in content or 'kubepods' in content or 'containerd' in content:
                    return True
        except Exception:
            pass
        return False

    execution_date = context['ds']  # Date d'exécution (YYYY-MM-DD)

    print(f"Extraction PostgreSQL vers S3 pour {execution_date}")

    # Path inside container (ensure docker-compose mounts the host scripts folder to /opt/airflow/scripts)
    script_path = '/opt/airflow/scripts/export_to_s3.py'

    # Fail fast if the script is not present
    if not os.path.exists(script_path):
        raise FileNotFoundError(
            f"Export script not found inside container at {script_path}. "
            "Verify docker-compose mounts ${AIRFLOW_PROJ_DIR}/scripts to /opt/airflow/scripts"
        )

    # Run using the same Python interpreter as Airflow and unbuffered mode for real-time logs
    cmd = [sys.executable, '-u', script_path]

    print(f"Running export command: {' '.join(cmd)}")

    # Prefer an external Postgres host when running inside Docker.
    # - If DB_HOST is explicitly set in the environment, use it.
    # - Otherwise, if running inside Docker, use 'host.docker.internal' (works on Docker Desktop;
    #   for Linux you may need to add extra_hosts: ["host.docker.internal:host-gateway"] in docker-compose).
    # - If not running in Docker, default to 'localhost'.
    explicit_db_host = os.environ.get('DB_HOST')
    if explicit_db_host:
        container_db_host = explicit_db_host
        print(f"Using DB_HOST from environment: {container_db_host}")
    else:
        if _running_in_docker():
            container_db_host = os.environ.get('DB_HOST', 'host.docker.internal')
            print("Detected Docker runtime. Defaulting to host.docker.internal for Postgres host "
                  f"(override with DB_HOST env var). Using: {container_db_host}")
        else:
            container_db_host = os.environ.get('DB_HOST', 'localhost')
            print("Not running in Docker. Defaulting to localhost for Postgres host "
                  f"(override with DB_HOST env var). Using: {container_db_host}")

    env = {
        **os.environ,
        "PYTHONUNBUFFERED": "1",
        "DB_HOST": container_db_host,
        "DB_PORT": os.environ.get("DB_PORT", "5432"),
        # pass through DB_USER/DB_PASSWORD/DB_NAME if present (export_to_s3.py should consume them)
        "DB_USER": os.environ.get("DB_USER", ""),
        "DB_PASSWORD": os.environ.get("DB_PASSWORD", ""),
        "DB_NAME": os.environ.get("DB_NAME", ""),
    }

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
    )

    # Always print outputs for diagnostics
    if result.stdout:
        print("export stdout:")
        print(result.stdout)
    if result.stderr:
        print("export stderr:")
        print(result.stderr)

    if result.returncode != 0:
        raise Exception(
            f"Erreur lors de l'export : returncode={result.returncode}\n"
            f"stdout:\n{result.stdout or '<empty>'}\n\nstderr:\n{result.stderr or '<empty>'}"
        )

    print("Export terminé avec succès")

def run_dbt_models(**context):
    """Exécute les transformations dbt"""
    import subprocess

    print("Exécution des modèles dbt")

    # MODIFIEZ LE CHEMIN SELON VOTRE INSTALLATION
    result = subprocess.run(
        ['dbt', 'run', '--project-dir', '/ShopStreamTP/shopstream_dbt'],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        raise Exception(f"Erreur dbt run : {result.stderr}")

    print(result.stdout)
    print("Transformations dbt terminées")

def run_dbt_tests(**context):
    """Exécute les tests de qualité dbt"""
    import subprocess

    print("Exécution des tests dbt")

    # MODIFIEZ LE CHEMIN SELON VOTRE INSTALLATION
    result = subprocess.run(
        ['dbt', 'test', '--project-dir', '/ShopStreamTP/shopstream_dbt'],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Certains tests ont échoué : {result.stderr}")
        # On ne fait pas échouer le DAG, juste un warning

    print(result.stdout)
    print("Tests dbt terminés")

def send_success_notification(**context):
    """Envoie une notification de succès"""
    print("Pipeline terminé avec succès")
    print(f"Execution date : {context['ds']}")
    # Ici : appel à Slack/Teams/Email via webhook

# Définition des tâches

# Tâche 1 : Extraction PostgreSQL vers S3
task_extract = PythonOperator(
    task_id='extract_postgres_to_s3',
    python_callable=extract_postgres_to_s3,
    dag=dag
)

# Tâche 2 : Attendre que les fichiers soient présents dans S3
task_wait_s3_users = S3KeySensor(
    task_id='wait_s3_users_file',
    bucket_name='shopstream-datalake-yassir',  # MODIFIEZ avec votre bucket
    bucket_key='raw/postgres/users/{{ ds }}/users_{{ ds_nodash }}.csv',
    aws_conn_id='aws_default',
    timeout=600,
    poke_interval=30,
    dag=dag
)

task_wait_s3_orders = S3KeySensor(
    task_id='wait_s3_orders_file',
    bucket_name='shopstream-datalake-yassir',  # MODIFIEZ avec votre bucket
    bucket_key='raw/postgres/orders/{{ ds }}/orders_{{ ds_nodash }}.csv',
    aws_conn_id='aws_default',
    timeout=600,
    poke_interval=30,
    dag=dag
)

# Tâche 3 : Chargement S3 vers Snowflake STAGING
task_load_staging_users = SnowflakeOperator(
    task_id='load_staging_users',
    snowflake_conn_id='snowflake_default',
    sql="""
        USE WAREHOUSE LOADING_WH;
        USE SCHEMA SHOPSTREAM_DWH. STAGING;
        
        TRUNCATE TABLE stg_users;
        
        COPY INTO stg_users (id, email, first_name, last_name, country, plan_type, created_at, last_login, is_active)
        FROM @RAW. s3_raw_stage/postgres/users/{{ ds }}/
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)

task_load_staging_orders = SnowflakeOperator(
    task_id='load_staging_orders',
    snowflake_conn_id='snowflake_default',
    sql="""
        USE WAREHOUSE LOADING_WH;
        USE SCHEMA SHOPSTREAM_DWH. STAGING;
        
        TRUNCATE TABLE stg_orders;
        
        COPY INTO stg_orders (id, user_id, created_at, total_amount, status, country, payment_method)
        FROM @RAW.s3_raw_stage/postgres/orders/{{ ds }}/
        FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
        ON_ERROR = 'CONTINUE';
    """,
    dag=dag
)

# Tâche 4 : Exécution de dbt (staging vers core vers marts)
task_dbt_run = PythonOperator(
    task_id='dbt_run_models',
    python_callable=run_dbt_models,
    dag=dag
)

# Tâche 5 : Tests dbt
task_dbt_test = PythonOperator(
    task_id='dbt_test_models',
    python_callable=run_dbt_tests,
    dag=dag
)

# Tâche 6 : Génération de la documentation dbt
task_dbt_docs = BashOperator(
    task_id='dbt_generate_docs',
    bash_command='cd /chemin/vers/ShopStreamTP/shopstream_dbt && dbt docs generate',  # MODIFIEZ le chemin
    dag=dag
)

# Tâche 7 : Notification de succès
task_success = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    dag=dag
)

# Définition des dépendances (DAG)

# Phase 1 : Extraction
task_extract >> [task_wait_s3_users, task_wait_s3_orders]

# Phase 2 : Chargement Staging
task_wait_s3_users >> task_load_staging_users
task_wait_s3_orders >> task_load_staging_orders

# Phase 3 : Transformations dbt
[task_load_staging_users, task_load_staging_orders] >> task_dbt_run

# Phase 4 : Tests et documentation
task_dbt_run >> task_dbt_test
task_dbt_test >> task_dbt_docs

# Phase 5 : Notification
task_dbt_docs >> task_success