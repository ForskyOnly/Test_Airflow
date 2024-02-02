import json
from pendulum import datetime

from airflow.decorators import (
    dag,
    task,
) 


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1), #chose static date IN THE PAST
    catchup=False,
    default_args={
        "retries": 0,  # If a task fails, it will retry 2 times.
    },
    tags=["example"],
)  # If set, this tag is shown in the DAG view of the Airflow UI
def premier_dag():

    @task()
    def extract():

        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(
        multiple_outputs=True
    )  # multiple_outputs=True unrolls dictionaries into separate XCom values
    def transform(order_data_dict: dict):

        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    @task()
    def load(total_order_value: float):

        print(f"Total order value is: {total_order_value:.2f}")



    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])


premier_dag()

if __name__ == "__main__":

   premier_dag().test()