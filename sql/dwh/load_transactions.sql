INSERT INTO transactions (
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
FROM stg_transactions
WHERE batch_date = %(batch_date)s
ON CONFLICT (transaction_id)
DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    branch_id = EXCLUDED.branch_id,
    transaction_date = EXCLUDED.transaction_date,
    amount = EXCLUDED.amount,
    payment_method = EXCLUDED.payment_method,
    transaction_status = EXCLUDED.transaction_status,
    channel = EXCLUDED.channel,
    updated_at = EXCLUDED.updated_at,
    merchant_category = EXCLUDED.merchant_category,
    device_type = EXCLUDED.device_type,
    currency = EXCLUDED.currency,
    fraud_flag = EXCLUDED.fraud_flag,
    promo_code = EXCLUDED.promo_code,
    batch_date = EXCLUDED.batch_date
WHERE
    transactions.updated_at IS NULL
    OR EXCLUDED.updated_at >= transactions.updated_at;
