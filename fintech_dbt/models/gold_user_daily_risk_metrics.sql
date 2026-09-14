with daily_report as (
    SELECT
        user_id,
        COUNT(transaction_id) as total_transactions,
        SUM(amount) as total_spend,
        COUNT(CASE WHEN risk_score > 0.80 THEN 1 END) AS high_risk_count,
        DATE(transaction_timestamp) AS transaction_date

    FROM {{ ref('silver_transactions') }}

    GROUP BY user_id, transaction_date
)

SELECT * FROM daily_report