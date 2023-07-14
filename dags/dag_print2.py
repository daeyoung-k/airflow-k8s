import pendulum
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

# 한국 시간 timezone 설정
kst = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'kane',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
}
# 한국 시간 2021년 1월 1일 시작, 오전 8시마다 실행되는 DAG 설정
dag = DAG(
    dag_id="print2",
    default_args=default_args,
    start_date=datetime(2023, 7, 7, tzinfo=kst),
    schedule_interval="*/5 * * * *",
)


def print_hello():
    print('쿠버네티스 airflow')
    return 0


def print_hello2():
    print('쿠버네티스 airflow2')
    return 0

def print_hello3():
    print('쿠버네티스 airflow3')
    return 0

def print_hello4():
    print('쿠버네티스 airflow4')
    return 0


t1 = PythonOperator(
        task_id='print',
        python_callable=print_hello,
        dag=dag
    )

t2 = PythonOperator(
        task_id='print2',
        python_callable=print_hello2,
        dag=dag
    )

t3 = PythonOperator(
        task_id='print3',
        python_callable=print_hello3,
        dag=dag
    )

t4 = PythonOperator(
        task_id='print4',
        python_callable=print_hello4,
        dag=dag
    )

t1 >> [t2, t3] >> t4
