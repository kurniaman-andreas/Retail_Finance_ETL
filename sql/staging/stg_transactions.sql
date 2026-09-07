DELETE FROM stg_transactions
WHERE batch_date = %(batch_date)s;

INSERT INTO stg_transactions (
    transaction_id,
    customer_id,
    branch_id,
    transaction_date,
    amount,
    payment_method,
    transaction_status,
    channel,
    updated_at,
    merchant_category,
    device_type,
    currency,
    fraud_flag,
    promo_code,
    batch_date
)
SELECT
    transaction_id,
    customer_id,
    branch_id,
    transaction_date,
    amount,
    payment_method,
    transaction_status,
    channel,
    updated_at,
    merchant_category,
    device_type,
    currency,
    fraud_flag,
    promo_code,
    batch_date
FROM (
    SELECT
        TRIM(transaction_id) AS transaction_id,
        TRIM(customer_id) AS customer_id,
        TRIM(branch_id) AS branch_id,
        NULLIF(TRIM(transaction_date), '')::TIMESTAMP AS transaction_date,
        NULLIF(TRIM(amount), '')::NUMERIC(18, 2) AS amount,
        TRIM(payment_method) AS payment_method,
        TRIM(transaction_status) AS transaction_status,
        TRIM(channel) AS channel,
        NULLIF(TRIM(updated_at), '')::TIMESTAMP AS updated_at,
        TRIM(merchant_category) AS merchant_category,
        TRIM(device_type) AS device_type,
        TRIM(currency) AS currency,
        TRIM(fraud_flag) AS fraud_flag,
        TRIM(promo_code) AS promo_code,
        batch_date,

        ROW_NUMBER() OVER (
            PARTITION BY
                TRIM(transaction_id),
                batch_date
            ORDER BY
                NULLIF(TRIM(updated_at), '')::TIMESTAMP DESC NULLS LAST
        ) AS rn
    FROM bronze_transactions
    WHERE batch_date = %(batch_date)s
) ranked
WHERE rn = 1;
