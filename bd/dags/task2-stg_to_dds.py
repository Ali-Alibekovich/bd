import pendulum
import hashlib
from datetime import datetime
from airflow.decorators import dag, task
import vertica_python

vertica_conn_info = {
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
    tags=['stg_to_dds'],
    is_paused_upon_creation=False
)
def task2_stg_to_dds():
    def generate_hash(*args):
        return hashlib.sha256(''.join(map(str, args)).encode()).hexdigest()

    @task
    def migrate_data():
        source_conn = vertica_python.connect(**vertica_conn_info)
        target_conn = vertica_python.connect(**vertica_conn_info)

        with source_conn.cursor() as source_cursor, target_conn.cursor() as target_cursor:

            # Заполнение s_person_info
            source_cursor.execute("SELECT * FROM peoples")
            for row in source_cursor.fetchall():
                hk_person_id = generate_hash(row[0])
                insert_s_person_info = """
                    INSERT INTO s_person_info(hk_person_id, name, date_of_birthday, country, phone, email, source, load_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                target_cursor.execute(insert_s_person_info, (
                    hk_person_id, row[1], row[4], row[3], row[5], row[6], 'source_system', datetime.now()))

            # Заполнение s_group_info
            source_cursor.execute("SELECT * FROM groups")
            for row in source_cursor.fetchall():
                hk_group_id = generate_hash(row[0])
                insert_s_group_info = """
                    INSERT INTO s_group_info(hk_group_id, title, status, source, load_date)
                    VALUES (%s, %s, %s, %s, %s)
                """
                target_cursor.execute(insert_s_group_info,
                                      (hk_group_id, row[2], 'active', 'source_system', datetime.now()))

            # Заполнение s_chat_info
            source_cursor.execute("SELECT * FROM chats")
            for row in source_cursor.fetchall():
                hk_msg_id = generate_hash(row[0])
                insert_s_chat_info = """
                    INSERT INTO s_chat_info(hk_msg_id, text_msg, msg_from, msg_to, status, source, load_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                target_cursor.execute(insert_s_chat_info,
                                      (hk_msg_id, row[4], row[2], row[3], 'sent', 'source_system', datetime.now()))

            # Заполнение h_people
            source_cursor.execute("SELECT * FROM s_person_info")
            for row in source_cursor.fetchall():
                hk_person_id = generate_hash(row[0])
                insert_h_people = """
                    INSERT INTO public.h_people(hk_person_id, person_id, source, load_date)
                    VALUES (%s, %s, %s, %s)
                """
                target_cursor.execute(insert_h_people,
                                      (hk_person_id, row[0], 'source_system', datetime.now()))

            # Заполнение h_chats
            source_cursor.execute("SELECT * FROM chats")
            for row in source_cursor.fetchall():
                hk_msg_id = generate_hash(row[0])
                insert_h_chats = """
                    INSERT INTO h_chats(hk_msg_id, msg_id, msg_time, source, load_date)
                    VALUES (%s, %s, %s, %s, %s)
                """
                target_cursor.execute(insert_h_chats,
                                      (hk_msg_id, row[0], row[1], 'source_system', datetime.now()))

            # Заполнение l_groups
            source_cursor.execute("SELECT * FROM chats")
            for row in source_cursor.fetchall():
                hk_L_group_id = generate_hash(row[0], row[5])  # уникальный ключ группаид+сообщениеид
                hk_msg_id = generate_hash(row[0])
                hk_group_id = generate_hash(row[5])
                insert_l_groups = """
                    INSERT INTO L_groups(hk_L_group_id, hk_msg_id, hk_group_id, source, load_date)
                    VALUES (%s, %s, %s, %s, %s)
                """
                target_cursor.execute(insert_l_groups,
                                      (hk_L_group_id, hk_msg_id, hk_group_id, 'source_system', datetime.now()))

            # Заполнение h_groups
            source_cursor.execute("SELECT * FROM groups")
            for row in source_cursor.fetchall():
                hk_group_id = generate_hash(row[0])
                insert_h_groups = """
                    INSERT INTO h_groups(hk_group_id, group_id, created_date, source, load_date)
                    VALUES (%s, %s, %s, %s, %s)
                """
                target_cursor.execute(insert_h_groups,
                                      (hk_group_id, row[0], row[3], 'source_system', datetime.now()))

            # # Заполнение l_chat_owners
            # source_cursor.execute("SELECT * FROM h_groups")
            # for row in source_cursor.fetchall():
            #     hk_group_id = row[0]
            #     query_get_owner_id = """
            #         SELECT * FROM groups
            #     """
            #     result_id = target_cursor.execute(query_get_owner_id).fetchall()
            #     for result_id_s in result_id:
            #         if generate_hash(result_id_s[0]) == hk_group_id:
            #             owner_id = result_id_s[1]
            #             owner_id = generate_hash(owner_id)
            #             insert_l_chat_owners = """
            #                 INSERT INTO L_chat_owners(hk_L_owner_id, hk_person_id, hk_group_id, source, load_date)
            #                 VALUES (%s, %s, %s, %s, %s)
            #             """
            #             target_cursor.execute(insert_l_chat_owners,
            #                                   (owner_id, owner_id, hk_group_id, 'source_system', datetime.now()))
            #             break

            # Заполнение l_chat_owners
            source_cursor.execute("SELECT * FROM groups")
            for row in source_cursor.fetchall():
                hk_group_id = generate_hash(row[0])
                hk_owner_id = generate_hash(row[1])
                insert_l_chat_owners = """
                            INSERT INTO L_chat_owners(hk_L_owner_id, hk_person_id, hk_group_id, source, load_date)
                            VALUES (%s, %s, %s, %s, %s)
                        """
                target_cursor.execute(insert_l_chat_owners,
                                      (hk_owner_id, hk_owner_id, hk_group_id, 'source_system', datetime.now()))

            # Заполнение l_person_chat (какие люди в каких есть чатах)
            source_cursor.execute("SELECT * from chats")
            for row in source_cursor.fetchall():
                hk_l_person_chat = generate_hash(row[0], row[1])
                hk_person_id = generate_hash(row[2])
                hk_msg_id = generate_hash(row[0])
                insert_l_person_chat = """
                    INSERT INTO public.L_person_chat(hk_L_person_chat, hk_person_id, hk_msg_id, source, load_date)
                    VALUES (%s, %s, %s, %s, %s)
                """
                target_cursor.execute(insert_l_person_chat,
                                      (hk_l_person_chat, hk_person_id, hk_msg_id, 'source_system', datetime.now()))

        source_conn.commit()
        target_conn.commit()
        source_conn.close()
        target_conn.close()

    (
        migrate_data()
    )


migrate = task2_stg_to_dds()
