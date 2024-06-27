import pendulum
from airflow.decorators import dag, task
import vertica_python

conn_info = {
    'host': 'vertica',
    'port': '5433',
    'user': 'vertica',
    'password': 'vertica',
    'database': 'VMart',
}


def create_view_top_5_group(connection):
    query = """
        -- топ 5 групп по кол сообщений
        CREATE OR REPLACE VIEW top_5_groups AS
        SELECT DISTINCT msg_group_id,
                        count(chats.msg_from) as msg
        FROM public.chats
        GROUP BY msg_group_id
        ORDER BY msg DESC
        LIMIT 5;
    """
    connection.cursor().execute(query)
    connection.commit()


def create_view_people_top_wrote(connection):
    query = """
        -- находим людей которые писали в эти группы
        CREATE OR REPLACE VIEW peoples_who_wrote_in_top AS
        select msg_from from chats
            where chats.msg_group_id IN(select msg_group_id
                            from top_5_groups);
                    """
    connection.cursor().execute(query)
    connection.commit()


def create_view_users_with_age(connection):
    query = """
        CREATE OR REPLACE VIEW user_age_distribution AS
        SELECT
            hk_person_id,
            CASE
                WHEN DATEDIFF('year', date_of_birthday, CURRENT_DATE) BETWEEN 0 AND 17 THEN '0-17'
                WHEN DATEDIFF('year', date_of_birthday, CURRENT_DATE) BETWEEN 18 AND 24 THEN '18-24'
                WHEN DATEDIFF('year', date_of_birthday, CURRENT_DATE) BETWEEN 25 AND 34 THEN '25-34'
                WHEN DATEDIFF('year', date_of_birthday, CURRENT_DATE) BETWEEN 35 AND 44 THEN '35-44'
                WHEN DATEDIFF('year', date_of_birthday, CURRENT_DATE) BETWEEN 45 AND 54 THEN '45-54'
                WHEN DATEDIFF('year', date_of_birthday, CURRENT_DATE) BETWEEN 55 AND 64 THEN '55-64'
                ELSE '65+'
                END AS age_group
        FROM public.s_person_info;
        """
    connection.cursor().execute(query)
    connection.commit()


def truncate(connection, table_name):
    cursor = connection.cursor()
    truncate_query = f"TRUNCATE TABLE public.{table_name}"
    cursor.execute(truncate_query)
    connection.commit()


@dag(
    schedule_interval='@once',
    start_date=pendulum.now(),
    catchup=False,
    tags=['task2_dds_to_cdm'],
    is_paused_upon_creation=False
)
def task2_dds_to_cdm():
    @task
    def get_user_age_groups():
        query_result = """        
            SELECT
                CASE
                WHEN DATEDIFF('year', p.date_of_birthday, CURRENT_DATE) BETWEEN 0 AND 17 THEN '0-17'
                WHEN DATEDIFF('year', p.date_of_birthday, CURRENT_DATE) BETWEEN 18 AND 24 THEN '18-24'
                WHEN DATEDIFF('year', p.date_of_birthday, CURRENT_DATE) BETWEEN 25 AND 34 THEN '25-34'
                WHEN DATEDIFF('year', p.date_of_birthday, CURRENT_DATE) BETWEEN 35 AND 44 THEN '35-44'
                WHEN DATEDIFF('year', p.date_of_birthday, CURRENT_DATE) BETWEEN 45 AND 54 THEN '45-54'
                WHEN DATEDIFF('year', p.date_of_birthday, CURRENT_DATE) BETWEEN 55 AND 64 THEN '55-64'
                ELSE '65+'
                END AS age_group,
            COUNT(DISTINCT p.id) AS num_people
        FROM peoples_who_wrote_in_top pw
                 JOIN public.peoples p ON p.id = pw.msg_from
        GROUP BY date_of_birthday
        ORDER BY age_group;
        """
        with vertica_python.connect(**conn_info) as connection:
            create_view_top_5_group(connection)
            create_view_people_top_wrote(connection)
            create_view_users_with_age(connection)
            truncate(connection, 'user_age_groups')
            cursor = connection.cursor()
            cursor.execute(query_result)
            result = cursor.fetchall()
            print(f"RESULT -> {result}")
            return result

    @task
    def populate_view_table(data):
        insert_query = """
            INSERT INTO public.user_age_groups (age_group, user_count) VALUES (:age_group, :user_count)
        """
        with vertica_python.connect(**conn_info) as connection:
            cursor = connection.cursor()
            for row in data:
                cursor.execute(insert_query, {'age_group': row[0], 'user_count': row[1]})
            connection.commit()

    user_age_groups = get_user_age_groups()

    populate_view_table(user_age_groups)


migraion = task2_dds_to_cdm()
