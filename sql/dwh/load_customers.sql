INSERT INTO customers (
    customer_id,
    customer_name,
    city,
    registration_date,
    customer_status,
    customer_segment,
    email,
    phone_number,
    birth_date,
    occupation,
    income_band,
    kyc_status,
    batch_date
)
SELECT
    customer_id,
    customer_name,
    city,
    registration_date,
    customer_status,
    customer_segment,
    email,
    phone_number,
    birth_date,
    occupation,
    income_band,
    kyc_status,
    batch_date
FROM stg_customers
WHERE batch_date = %(batch_date)s
ON CONFLICT (customer_id)
DO UPDATE SET
    customer_name = EXCLUDED.customer_name,
    city = EXCLUDED.city,
    registration_date = EXCLUDED.registration_date,
    customer_status = EXCLUDED.customer_status,
    customer_segment = EXCLUDED.customer_segment,
    email = EXCLUDED.email,
    phone_number = EXCLUDED.phone_number,
    birth_date = EXCLUDED.birth_date,
    occupation = EXCLUDED.occupation,
    income_band = EXCLUDED.income_band,
    kyc_status = EXCLUDED.kyc_status,
    batch_date = EXCLUDED.batch_date;
