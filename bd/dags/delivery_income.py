import json
from datetime import datetime
from airflow.decorators import dag, task
import pendulum
from airflow.models import Variable
from sqlalchemy import create_engine, MetaData, select, text, and_
from sqlalchemy.orm import sessionmaker

DATABASE_URL_DDS = Variable.get('POSTGRESQL_URI')
DATABASE_URL_CDM = Variable.get('POSTGRESQL_URI')


def period_start_for_date(d):
    if d.day < 21:
        if d.month == 1:
            return datetime(d.year - 1, 12, 21)
        else:
            return datetime(d.year, d.month - 1, 21)
    else:
        return datetime(d.year, d.month, 21)


def period_end_for_date(d):
    if d.day < 21:
        return datetime(d.year, d.month, 21)
    else:
        if d.month == 12:
            return datetime(d.year + 1, 1, 21)
        else:
            return datetime(d.year, d.month + 1, 21)


@dag(
    schedule_interval='@once',
    start_date=pendulum.now(),
    catchup=False,
    tags=['dds_cdm'],
    is_paused_upon_creation=False
)
def delivery_income():
    dds_engine = create_engine(DATABASE_URL_DDS)
    cdm_engine = create_engine(DATABASE_URL_CDM)
    DdsSession = sessionmaker(bind=dds_engine)
    CdmSession = sessionmaker(bind=cdm_engine)
    dds_metadata = MetaData()
    cdm_metadata = MetaData()

    dds_metadata.reflect(bind=dds_engine, schema='dds')
    cdm_metadata.reflect(bind=cdm_engine, schema='cdm')

    @task
    def process_deliveryman_income():
        dds_session = DdsSession()
        cdm_session = CdmSession()

        dm_fact_table = dds_metadata.tables['dds.dm_fact_table']
        dm_orders_table = dds_metadata.tables['dds.dm_orders']
        dm_time_table = dds_metadata.tables['dds.dm_time']
        dm_delivery_table = dds_metadata.tables['dds.dm_delivery']
        dm_deliveryman_table = dds_metadata.tables['dds.dm_deliveryman']
        deliveryman_income_table = cdm_metadata.tables['cdm.deliveryman_income']

        truncate_deliveryman_income(cdm_session, deliveryman_income_table)

        oldest_order_date = get_oldest_order_date(dds_session, dm_time_table, dm_orders_table)

        if not oldest_order_date:
            return  # No orders found

        current_period_start, current_period_end = calculate_periods(oldest_order_date)

        if current_period_end > datetime.now():
            return

        while current_period_end < datetime.now():
            income_data = calculate_income_data(dds_session, dm_deliveryman_table, dm_delivery_table,
                                               dm_orders_table, dm_time_table, current_period_start,
                                               current_period_end, dm_fact_table)

            store_income_data(cdm_session, deliveryman_income_table, income_data)

            current_period_start, current_period_end = update_periods(current_period_end)

        commit_sessions(dds_session, cdm_session)

    process_deliveryman_income()



def truncate_deliveryman_income(session, deliveryman_income_table):
    session.execute(deliveryman_income_table.delete())
    session.commit()


def get_oldest_order_date(session, dm_time_table, dm_orders_table):
    oldest_order_date_query = select([dm_time_table.c.time_mark]).select_from(
        dm_time_table.join(dm_orders_table, dm_time_table.c.id == dm_orders_table.c.time_id)
    ).where(dm_orders_table.c.status == 'CLOSE').order_by(dm_time_table.c.time_mark).limit(1)

    oldest_order_date_result = session.execute(oldest_order_date_query).fetchone()

    return oldest_order_date_result.time_mark if oldest_order_date_result else None


def calculate_periods(oldest_order_date):
    current_period_start = period_start_for_date(oldest_order_date)
    current_period_end = period_end_for_date(oldest_order_date)
    return current_period_start, current_period_end


def update_periods(current_period_end):
    current_period_start = current_period_end
    current_period_end = period_end_for_date(current_period_end)
    return current_period_start, current_period_end


def calculate_income_data(dds_session, dm_deliveryman_table, dm_delivery_table, dm_orders_table,
                          dm_time_table, current_period_start, current_period_end, dm_fact_table):
    query = select([
        dm_deliveryman_table.c.deliveryman_unique_id.label('deliveryman_id'),
        dm_deliveryman_table.c.name.label('deliveryman_name'),
        text('AVG(dm_delivery.rating) as rating'),
        text('SUM(dm_delivery.tips) as tips'),
        dm_orders_table.c.id.label('order_id'),
        dm_time_table.c.time_mark
    ]).select_from(
        dm_deliveryman_table.join(dm_delivery_table, dm_deliveryman_table.c.id == dm_delivery_table.c.deliveryman_id)
        .join(dm_orders_table, dm_delivery_table.c.order_id == dm_orders_table.c.id)
        .join(dm_time_table, dm_orders_table.c.time_id == dm_time_table.c.id)
    ).where(
        and_(
            dm_orders_table.c.status == 'CLOSE',
            dm_time_table.c.time_mark.between(current_period_start, current_period_end)
        )
    ).group_by(
        dm_deliveryman_table.c.id,
        dm_deliveryman_table.c.name,
        dm_orders_table.c.id,
        dm_time_table.c.time_mark
    )

    results = dds_session.execute(query).fetchall()

    income_data = {}

    for result in results:
        deliveryman_id = result['deliveryman_id']
        deliveryman_name = result['deliveryman_name']
        rating = result['rating']
        tips = result['tips']
        order_id = result['order_id']

        orders_total_cost = get_orders_total_cost(dds_session, dm_fact_table, order_id, dm_orders_table)

        if deliveryman_id not in income_data:
            income_data[deliveryman_id] = {
                'deliveryman_name': deliveryman_name,
                'year': current_period_start.year,
                'month': current_period_start.month,
                'orders_amount': 0,
                'orders_total_cost': 0,
                'rating': 0,
                'tips': 0,
                'rating_count': 0
            }

        income_data[deliveryman_id]['orders_amount'] += 1
        income_data[deliveryman_id]['orders_total_cost'] += orders_total_cost
        income_data[deliveryman_id]['tips'] += tips
        income_data[deliveryman_id]['rating'] += rating
        income_data[deliveryman_id]['rating_count'] += 1

    return income_data


def get_orders_total_cost(session, dm_fact_table, order_id, dm_orders_table):
    orders_total_cost_query = select([
        text('SUM(dm_fact_table.total_amount) as orders_total_cost')
    ]).select_from(
        dm_fact_table.join(dm_orders_table, dm_fact_table.c.order_id == dm_orders_table.c.id)
    ).where(
        dm_fact_table.c.order_id == order_id
    )

    orders_total_cost_result = session.execute(orders_total_cost_query).fetchone()
    return orders_total_cost_result['orders_total_cost'] if orders_total_cost_result else 0


def store_income_data(session, deliveryman_income_table, income_data):
    for deliveryman_id, data in income_data.items():
        if data['rating_count'] > 0:
            data['rating'] /= data['rating_count']

        deliveryman_income_record = {
            'deliveryman_id': deliveryman_id,
            'deliveryman_name': data['deliveryman_name'],
            'year': data['year'],
            'month': data['month'],
            'orders_amount': data['orders_amount'],
            'orders_total_cost': data['orders_total_cost'],
            'rating': data['rating'],
            'tips': data['tips']
        }

        insert_stmt = deliveryman_income_table.insert().values(**deliveryman_income_record)
        session.execute(insert_stmt)


def commit_sessions(dds_session, cdm_session):
    cdm_session.commit()
    dds_session.close()
    cdm_session.close()


cdm_delivery_income_dag = delivery_income()
