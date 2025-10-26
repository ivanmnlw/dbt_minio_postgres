from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO

# A Dag represents a workflow, a collection of tasks
with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")
    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='s3-bucket-etl',
        bucket_key='Berkshire_Stocks_1977-2024.csv',
        aws_conn_id='minio_s3_conn',
        poke_interval = 30,
        timeout = 600
    )

    @task()
    def airflow():
        import pandas as pd
        df = pd.read_csv("/opt/airflow/data/online-retail-dataset.csv")
        return df
    
    @task()
    def extractMinio():
        import pandas as pd
        hook = S3Hook(aws_conn_id="minio_s3_conn")

        obj = hook.get_key(
            key="Berkshire_Stocks_1977-2024.csv",
            bucket_name="s3-bucket-etl"
        )

        data = obj.get()["Body"].read().decode("utf-8")

        df = pd.read_csv(StringIO(data))
        df.rename(columns={
                    "Company": "company",
                    "Year": "year",
                    "Cost ($ 000s)": "cost_dollar_in_thousands",
                    "Market ($ 000s)": "market_dollar_in_thousands"
                }, inplace=True)
        print("Hello")
        print("test")
        print("Yoo")

        print("New Branch Dev")

        df.to_csv("/opt/airflow/data/Berkshire_Stocks_1977-2024.csv", index=False)
        return df.head()

    # Set dependencies between tasks
    hello >> task1 >> airflow() >> extractMinio()