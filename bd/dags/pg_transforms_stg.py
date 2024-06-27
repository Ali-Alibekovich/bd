import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from sqlalchemy import create_engine, MetaData, select, insert, update, text, delete
from sqlalchemy.orm import sessionmaker

DATABASE_URL_SRC = Variable.get('POSTGRESQL_URI')
DATABASE_URL_DWH = Variable.get('POSTGRESQL_URI')


@dag(
    schedule_interval='@once',
    start_date=pendulum.now(),
    catchup=False,
    tags=['postgres_to_stg'],
    is_paused_upon_creation=False
)
def pg_transforms_stg():
    src_engine = create_engine(DATABASE_URL_SRC)
    dwh_engine = create_engine(DATABASE_URL_DWH)
    Session = sessionmaker(bind=src_engine)
    DwhSession = sessionmaker(bind=dwh_engine)
    src_metadata = MetaData()
    dwh_metadata = MetaData()

    src_metadata.reflect(bind=src_engine, schema='public')
    dwh_metadata.reflect(bind=dwh_engine, schema='stg')

    def load_table_data(table_name, stg_table_name):
        src_session = Session()
        dwh_session = DwhSession()

        source_table = src_metadata.tables[f'public.{table_name}']
        stg_table = dwh_metadata.tables[f'stg.{stg_table_name}']
        settings_table = dwh_metadata.tables['stg.settings']

        # Get last version
        query = select([settings_table.c.settings]).where(
            settings_table.c.setting_key == f'{stg_table_name}_last_version')
        result = dwh_session.execute(query).fetchone()

        if result:
            last_version = result['settings']['last_version']
        else:
            # Initialize settings if the table is empty
            last_version = 0
            initial_settings = insert(settings_table).values(
                setting_key=f'{stg_table_name}_last_version',
                settings={'last_version': last_version}
            )
            dwh_session.execute(initial_settings)
            dwh_session.commit()

        key_id = getattr(source_table.c, table_name + "_id")

        query = select([source_table])
        new_rows = src_session.execute(query).fetchall()

        for row in new_rows:
            obj_id = row[key_id]
            existing_record = dwh_session.execute(
                select([stg_table]).where(stg_table.c[table_name + "_id"] == obj_id)
            ).fetchone()

            if existing_record:
                # Update existing record with a new version
                update_stmt = update(stg_table).where(stg_table.c[table_name + "_id"] == obj_id).values(
                    **row,
                    when_updated=pendulum.now(),
                    version=last_version + 1
                )
                dwh_session.execute(update_stmt)
            else:
                # Insert new record
                insert_stmt = stg_table.insert().values(
                    **row,
                    when_updated=pendulum.now(),
                    version=last_version + 1
                )
                dwh_session.execute(insert_stmt)

        delete_stmt = delete(stg_table).where(stg_table.c.version <= last_version)
        dwh_session.execute(delete_stmt)

        # Update the settings table with the new version
        update_settings = update(settings_table).where(
            settings_table.c.setting_key == f'{stg_table_name}_last_version'
        ).values(
            settings={'last_version': last_version + 1}
        )
        dwh_session.execute(update_settings)

        dwh_session.commit()
        src_session.close()
        dwh_session.close()

    @task
    def category():
        load_table_data('category', 'pg_category')

    @task
    def dish():
        load_table_data('dish', 'pg_dish')

    @task
    def client():
        load_table_data('client', 'pg_client')

    category_task = category()
    dish_task = dish()
    client_task = client()

    [category_task, dish_task, client_task]


pg_tables_dag = pg_transforms_stg()
