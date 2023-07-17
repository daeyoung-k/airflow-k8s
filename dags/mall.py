from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
from batch import (
    PROD_EC2_mall
)
# from utils.alert import SlackAlert


# alert = SlackAlert('airflow_batch') # 메세지를 보낼 슬랙 채널명을 파라미터로 넣어줍니다.

default_args = {
    'owner': 'solee',
    'depends_on_past': False, #DAG 실패시 진행 여부
    'start_date': datetime(2021, 5, 25), #시작날짜,
    'retries': 1, # 실패시 재실행 수
    'retry_delay': timedelta(minutes=2), #재실행시 딜레이 시간, #minutes=5 -> 실패 시 2분후 다시 실행
    # 'on_success_callback': alert.slack_success_alert,
    # 'on_failure_callback': alert.slack_fail_alert,
}


dag = DAG(dag_id='24mall_crawl',
          default_args=default_args,
          catchup=False, #지난 날짜 진행 여부
          schedule_interval="*/5 * * * *", #실행스케줄러, cron기능
          tags=["이사몰 스크래핑"]
          )


def _sleep():
    print(str(datetime.now()))


def _mcygclean():
    PROD_EC2_mall._mcygclean()

def _boi():
    PROD_EC2_mall._boi()

def _boi_company():
    PROD_EC2_mall._boi_company()

# def _24mall():
#     PROD_EC2_mall._24mall()


# def _aircon_matching():
#     aircon_matching.aircon_matching()


wait_this = PythonOperator(
    task_id='wait',
    python_callable=_sleep,
    dag=dag,
)

mcygclean = PythonOperator(
    task_id='mcygclean',
    python_callable=_mcygclean,
    dag=dag,
)

boi = PythonOperator(
    task_id='boi',
    python_callable=_boi,
    dag=dag,
)

boi_company = PythonOperator(
    task_id='boi_company',
    python_callable=_boi_company,
    dag=dag,
)

# aircon = PythonOperator(
#     task_id='aircon_matching',
#     python_callable=_aircon_matching,
#     dag=dag
# )


# _24mall = PythonOperator(
#     task_id='24mall',
#     python_callable=_24mall,
#     dag=dag,
# )

wait_this >> [mcygclean, boi, boi_company]
