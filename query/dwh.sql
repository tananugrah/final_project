-- Example DWH

DROP TABLE IF EXISTS dim_transaction1;
CREATE TABLE public.dim_transaction1 (
    id_transaction INT NOT NULL,
    id_customer INT NOT NULL,
    birthdate_customer DATE,
    gender_customer VARCHAR(10),
    country_customer VARCHAR(100),
    date_transaction DATE,
    product_transaction VARCHAR(100),
    amount_transaction INT NOT NULL
    );