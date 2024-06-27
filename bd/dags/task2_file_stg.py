import pendulum
import csv
from airflow.decorators import dag, task
import vertica_python

conn_info = {
    'host': 'vertica',
    'port': '5433',
    'user': 'vertica',
    'password': 'vertica',
    'database': 'VMart',
    'schema': 'public',
}


@dag(
    schedule_interval='@once',
    start_date=pendulum.now(),
    catchup=False,
    tags=['file_stg'],
    is_paused_upon_creation=False
)
def task2_file_stg():
    def load_csv_to_vertica(file_path, table_name):
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                columns = next(reader)
                query = f"INSERT INTO public.{table_name} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
                for data in reader:
                    cursor.execute(query, data)
            connection.commit()

    def truncate(table_name):
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            truncate_query = f"TRUNCATE TABLE public.{table_name}"
            cursor.execute(truncate_query)
            connection.commit()

    @task
    def load_peoples():
        truncate("peoples")
        load_csv_to_vertica("dags/files/peoples.csv", "peoples")

    @task
    def load_chats():
        truncate("chats")
        load_csv_to_vertica("dags/files/chats.csv", "chats")

    @task
    def load_groups():
        truncate("groups")
        load_csv_to_vertica("dags/files/groups.csv", "groups")

    load_peoples() >> load_chats() >> load_groups()


file_to_stg = task2_file_stg()
