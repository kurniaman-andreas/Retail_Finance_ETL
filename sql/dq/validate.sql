-- NULL transaction ID
SELECT
    'null_transaction_id' AS rule_name,
    COUNT(*) AS failed_count
FROM stg_transactions
WHERE batch_date = %(batch_date)s
  AND transaction_id IS NULL

UNION ALL

-- Duplicate transaction
SELECT
    'duplicate_transaction' AS rule_name,
    COUNT(*) AS failed_count
FROM (
    SELECT transaction_id
    FROM stg_transactions
    WHERE batch_date = %(batch_date)s
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
) dupes

UNION ALL

-- Invalid amount
SELECT
    'invalid_amount' AS rule_name,
    COUNT(*) AS failed_count
FROM stg_transactions
WHERE batch_date = %(batch_date)s
  AND (amount IS NULL OR amount <= 0)

UNION ALL

-- Invalid customer FK
SELECT
    'invalid_customer_fk' AS rule_name,
    COUNT(*) AS failed_count
FROM stg_transactions t
LEFT JOIN stg_customers c
    ON t.customer_id = c.customer_id
   AND c.batch_date = %(batch_date)s
WHERE t.batch_date = %(batch_date)s
  AND t.customer_id IS NOT NULL
  AND c.customer_id IS NULL

UNION ALL

-- Invalid branch FK
SELECT
    'invalid_branch_fk' AS rule_name,
    COUNT(*) AS failed_count
FROM stg_transactions t
LEFT JOIN stg_branches b
    ON t.branch_id = b.branch_id
   AND b.batch_date = %(batch_date)s
WHERE t.batch_date = %(batch_date)s
  AND t.branch_id IS NOT NULL
  AND b.branch_id IS NULL;
