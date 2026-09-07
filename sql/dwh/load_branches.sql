INSERT INTO branches (
    branch_id,
    branch_name,
    region,
    branch_status,
    branch_type,
    opening_date,
    manager_name,
    city,
    batch_date
)
SELECT
    branch_id,
    branch_name,
    region,
    branch_status,
    branch_type,
    opening_date,
    manager_name,
    city,
    batch_date
FROM stg_branches
WHERE batch_date = %(batch_date)s
ON CONFLICT (branch_id)
DO UPDATE SET
    branch_name = EXCLUDED.branch_name,
    region = EXCLUDED.region,
    branch_status = EXCLUDED.branch_status,
    branch_type = EXCLUDED.branch_type,
    opening_date = EXCLUDED.opening_date,
    manager_name = EXCLUDED.manager_name,
    city = EXCLUDED.city,
    batch_date = EXCLUDED.batch_date;
