DELETE FROM stg_customers
WHERE batch_date = %(batch_date)s;

INSERT INTO stg_customers (
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
FROM (
    SELECT
        TRIM(customer_id) AS customer_id,
        TRIM(customer_name) AS customer_name,
        TRIM(city) AS city,
        NULLIF(TRIM(registration_date), '')::DATE AS registration_date,
        TRIM(customer_status) AS customer_status,
        TRIM(customer_segment) AS customer_segment,
        TRIM(email) AS email,
        TRIM(phone_number) AS phone_number,
        NULLIF(TRIM(birth_date), '')::DATE AS birth_date,
        TRIM(occupation) AS occupation,
        TRIM(income_band) AS income_band,
        TRIM(kyc_status) AS kyc_status,
        batch_date,

        ROW_NUMBER() OVER (
            PARTITION BY
                TRIM(customer_id),
                batch_date
            ORDER BY
                customer_id
        ) AS rn
    FROM bronze_customers
    WHERE batch_date = %(batch_date)s
) ranked
WHERE rn = 1;
