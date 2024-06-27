CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.dm_category
(
    id                 SERIAL PRIMARY KEY,
    category_unique_id INT          NOT NULL UNIQUE,
    name               VARCHAR(255) NOT NULL,
    percent            DECIMAL      NOT NULL,
    min_payment        DECIMAL      NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_clients
(
    id               SERIAL PRIMARY KEY,
    client_unique_id VARCHAR      NOT NULL UNIQUE,
    name             VARCHAR(255) NOT NULL,
    phone            VARCHAR(255) NOT NULL,
    birthday         DATE         NOT NULL,
    email            VARCHAR(255) NOT NULL,
    login            VARCHAR(255) NOT NULL,
    address          VARCHAR      NOT NULL,
    bonus_balance    DECIMAL      NOT NULL,
    category_id      INT          NOT NULL REFERENCES dds.dm_category (id)
);

CREATE TABLE IF NOT EXISTS dds.dm_restaurants
(
    id                   SERIAL PRIMARY KEY,
    restaurant_unique_id VARCHAR      NOT NULL UNIQUE,
    name                 VARCHAR(255) NOT NULL,
    phone                VARCHAR(255) NOT NULL,
    email                VARCHAR(255) NOT NULL,
    founding_day         DATE         NOT NULL,
    menu                 JSON         NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_dish
(
    id             SERIAL PRIMARY KEY,
    dish_unique_id VARCHAR      NOT NULL UNIQUE,
    name           VARCHAR(255) NOT NULL,
    price          DECIMAL      NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_time
(
    id        SERIAL PRIMARY KEY,
    time_mark TIMESTAMP NOT NULL,
    year      SMALLINT  NOT NULL CHECK ( year > 2022 ),
    month     SMALLINT  NOT NULL CHECK ( month BETWEEN 1 AND 12 ),
    day       SMALLINT  NOT NULL CHECK ( day BETWEEN 1 AND 31 ),
    time      TIME      NOT NULL,
    date      DATE      NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_orders
(
    id              SERIAL PRIMARY KEY,
    order_unique_id VARCHAR NOT NULL UNIQUE,
    user_id         INT     NOT NULL REFERENCES dds.dm_clients (id),
    restaurant_id   INT     NOT NULL REFERENCES dds.dm_restaurants (id),
    time_id         INT     NOT NULL REFERENCES dds.dm_time (id),
    status          VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_deliveryman
(
    id                    SERIAL PRIMARY KEY,
    deliveryman_unique_id VARCHAR      NOT NULL UNIQUE,
    name                  VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_delivery
(
    id                 SERIAL PRIMARY KEY,
    delivery_unique_id VARCHAR   NOT NULL UNIQUE,
    order_id           INT       NOT NULL references dds.dm_orders (id),
    deliveryman_id     INT       NOT NULL references dds.dm_deliveryman (id),
    delivery_address   VARCHAR   NOT NULL,
    delivery_time      TIMESTAMP NOT NULL,
    rating             INT       NOT NULL,
    tips               INT       NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.dm_fact_table
(
    id            SERIAL PRIMARY KEY,
    dish_id       INT            NOT NULL references dds.dm_dish (id),
    order_id      INT            NOT NULL REFERENCES dds.dm_orders (id),
    amount        INT            NOT NULL,
    price         NUMERIC(14, 2) NOT NULL,
    total_amount  NUMERIC(14, 2) NOT NULL,
    bonus_payment NUMERIC(14, 2) NOT NULL,
    bonus_grant   NUMERIC(14, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.settings
(
    id          SERIAL PRIMARY KEY,
    setting_key VARCHAR NOT NULL,
    settings    JSON    NOT NULL
);