SELECT 
    transaction_id,
    is_fraudulent
FROM {{ source('fraud', 'labeled_transactions') }}