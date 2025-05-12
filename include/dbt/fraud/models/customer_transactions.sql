SELECT 
    transaction_id,
    user_id,
    transaction_date,
    amount
FROM {{ source('fraud', 'customer_transactions') }}