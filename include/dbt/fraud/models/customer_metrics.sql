WITH transaction_summary AS (
    SELECT
        ct.user_id,
        COUNT(ct.transaction_id) AS total_transactions,
        SUM(CASE WHEN lt.is_fraudulent THEN 1 ELSE 0 END) AS fraudulent_transactions,
        SUM(CASE WHEN NOT lt.is_fraudulent THEN 1 ELSE 0 END) AS non_fraudulent_transactions,
    FROM {{ ref('customer_transactions') }} AS ct
    INNER JOIN {{ ref('labeled_transactions') }} AS lt
        ON ct.transaction_id = lt.transaction_id
    GROUP BY
        ct.user_id
)
SELECT
    user_id,
    total_transactions,
    fraudulent_transactions,
    non_fraudulent_transactions,
    ROUND((fraudulent_transactions::FLOAT / NULLIF(total_transactions, 0)) * 100, 2) AS risk_score
FROM transaction_summary