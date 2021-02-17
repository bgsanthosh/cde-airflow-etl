from airflow import DAG
from datetime import datetime, timedelta
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'santhosh',
    'depends_on_past': False,
    'email': ['santhosh@cloudera.com'],
    'start_date': datetime(2020,12,7,8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'cde-airflow-dag', default_args=default_args, catchup=False, schedule_interval="*/7 * * * *", is_paused_upon_creation=False)


hive_vw_query = """
create database if not exists lr_provider;
create external table provider(Provider string, PotentialFraud  string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '{{ test_data_bucket }}/test_result/airflow/provider';
create table if not exists lr_insurance_fraud.hive_provider(Provider string, PotentialFraud  string);
insert into lr_insurance_fraud.hive_provider select * from provider;
"""


start = DummyOperator(task_id='start', dag=dag)


process_write_s3_job = CDEJobRunOperator(
    task_id='airflow_spark_job',
    dag=dag,
    job_name='airflow-spark-job',
    variables={
        'dataSetBucketPath': '{{ test_data_bucket }}/insurance_data',
        'writeBucketPath': '{{ test_data_bucket }}/test_result/airflow',
    }

)


hive_cdw_job = CDWOperator(
    task_id='hive-cdw-job',
    dag=dag,
    cli_conn_id='hive-vw-connid',
    hql=hive_vw_query,
    schema='lr_provider',
    use_proxy_user=False,
    query_isolation=False

)



end = DummyOperator(task_id='end', dag=dag)

start >> process_write_s3_job >> hive_cdw_job >> end
