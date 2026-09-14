{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT
        CAST(transaction_id AS STRING) AS transaction_id,
        CAST(user_id AS STRING) AS user_id,
        CAST(amount AS NUMERIC) AS amount,
        CAST(currency AS STRING) AS currency,
        CAST(timestamp AS TIMESTAMP) AS transaction_timestamp,
        CAST(merchant_category AS STRING) AS merchant_category,
        CAST(device_ip AS STRING) AS device_ip,
        CAST(location_country AS STRING) AS location_country,
        CAST(is_card_present AS BOOLEAN) AS is_card_present,
        CAST(risk_score AS FLOAT64) AS risk_score,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM {{ source('bronze_layer', 'bronze_transactions') }}
    WHERE transaction_id IS NOT NULL
)

SELECT * FROM source_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY transaction_id 
    ORDER BY transaction_timestamp DESC, ingested_at DESC
) = 1