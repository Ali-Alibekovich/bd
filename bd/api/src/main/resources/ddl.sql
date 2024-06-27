create table deliveryman
(
    id   varchar primary key,
    name varchar not null
);

create table delivery
(
    id                 varchar primary key,
    deliveryman_id     varchar references deliveryman (id),
    order_id           varchar   not null,
    order_date_created timestamp not null,
    delivery_address   varchar   not null,
    delivery_time      timestamp not null,
    rating             int       not null check ( rating >= 1 and rating <= 5 ),
    tips               int       not null check ( tips >= 0 )
);