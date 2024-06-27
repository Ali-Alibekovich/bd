CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.mongo_clients
(
    id           SERIAL PRIMARY KEY,
    obj_id       VARCHAR   NOT NULL,
    obj_val      TEXT      NOT NULL,
    when_updated TIMESTAMP NOT NULL DEFAULT now(),
    version      BIGINT
);

CREATE TABLE IF NOT EXISTS stg.mongo_restaurants
(
    id           SERIAL PRIMARY KEY,
    obj_id       VARCHAR   NOT NULL,
    obj_val      TEXT      NOT NULL,
    when_updated TIMESTAMP NOT NULL DEFAULT now(),
    version      BIGINT
);

CREATE TABLE IF NOT EXISTS stg.mongo_orders
(
    id           SERIAL PRIMARY KEY,
    obj_id       VARCHAR   NOT NULL,
    obj_val      TEXT      NOT NULL,
    when_updated TIMESTAMP NOT NULL DEFAULT now(),
    version      BIGINT
);

CREATE TABLE IF NOT EXISTS stg.pg_category
(
    id           SERIAL PRIMARY KEY,
    category_id  INT          NOT NULL,
    name         VARCHAR(255) NOT NULL,
    percent      DECIMAL      NOT NULL,
    min_payment  DECIMAL      NOT NULL,
    when_updated TIMESTAMP    NOT NULL DEFAULT now(),
    version      BIGINT
);

CREATE TABLE IF NOT EXISTS stg.pg_dish
(
    id           SERIAL PRIMARY KEY,
    dish_id      VARCHAR      NOT NULL,
    name         VARCHAR(255) NOT NULL,
    price        DECIMAL      NOT NULL,
    when_updated TIMESTAMP    NOT NULL DEFAULT now(),
    version      BIGINT
);

CREATE TABLE IF NOT EXISTS stg.pg_client
(
    id            SERIAL PRIMARY KEY,
    client_id     VARCHAR   NOT NULL,
    bonus_balance DECIMAL   NOT NULL,
    category_id   INT       NOT NULL,
    when_updated  TIMESTAMP NOT NULL DEFAULT now(),
    version       BIGINT
);

CREATE TABLE IF NOT EXISTS stg.api_deliveryman
(
    id           SERIAL PRIMARY KEY,
    obj_id       VARCHAR   NOT NULL,
    obj_val      TEXT      NOT NULL,
    when_updated TIMESTAMP NOT NULL DEFAULT now(),
    version      BIGINT
);

CREATE TABLE IF NOT EXISTS stg.api_delivery
(
    id           SERIAL PRIMARY KEY,
    obj_id       VARCHAR   NOT NULL,
    obj_val      TEXT      NOT NULL,
    when_updated TIMESTAMP NOT NULL DEFAULT now(),
    version      BIGINT
);

CREATE TABLE IF NOT EXISTS stg.settings
(
    id          SERIAL PRIMARY KEY,
    setting_key VARCHAR NOT NULL,
    settings    JSON    NOT NULL
);