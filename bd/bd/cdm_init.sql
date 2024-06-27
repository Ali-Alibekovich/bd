CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.deliveryman_income
(
    id                       SERIAL PRIMARY KEY,
    deliveryman_id           VARCHAR      NOT NULL,
    deliveryman_name         VARCHAR(255) NOT NULL,
    year                     INT          NOT NULL CHECK (year >= 1900 AND year <= EXTRACT(YEAR FROM CURRENT_DATE)),
    month                    INT          NOT NULL CHECK (month >= 1 AND month <= 12),
    orders_amount            INT          NOT NULL CHECK (orders_amount >= 0),
    orders_total_cost        NUMERIC      NOT NULL CHECK (orders_total_cost >= 0),
    rating                   NUMERIC      NOT NULL CHECK (rating >= 0 AND rating <= 10),
    company_commission       NUMERIC,
    deliveryman_order_income NUMERIC,
    tips                     NUMERIC      NOT NULL CHECK (tips >= 0)
);

CREATE OR REPLACE FUNCTION cdm.calculate_deliveryman_income() RETURNS TRIGGER AS
$$
BEGIN
    NEW.company_commission := NEW.orders_total_cost * 0.5;

    IF NEW.rating < 8 THEN
        NEW.deliveryman_order_income := GREATEST(NEW.orders_total_cost * 0.05, 400);
    ELSIF NEW.rating >= 8 THEN
        NEW.deliveryman_order_income := LEAST(NEW.orders_total_cost * 0.1, 1000);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_calculate_deliveryman_income
    BEFORE INSERT OR UPDATE
    ON cdm.deliveryman_income
    FOR EACH ROW
EXECUTE FUNCTION cdm.calculate_deliveryman_income();