DELETE FROM stg_branches
WHERE batch_date = %(batch_date)s;

INSERT INTO stg_branches (
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
FROM (
    SELECT
        TRIM(branch_id) AS branch_id,
        TRIM(branch_name) AS branch_name,
        TRIM(region) AS region,
        TRIM(branch_status) AS branch_status,
        TRIM(branch_type) AS branch_type,
        NULLIF(TRIM(opening_date), '')::DATE AS opening_date,
        TRIM(manager_name) AS manager_name,
        TRIM(city) AS city,
        batch_date,

        ROW_NUMBER() OVER (
            PARTITION BY
                TRIM(branch_id),
                batch_date
            ORDER BY
                branch_id
        ) AS rn
    FROM bronze_branches
    WHERE batch_date = %(batch_date)s
) ranked
WHERE rn = 1;
