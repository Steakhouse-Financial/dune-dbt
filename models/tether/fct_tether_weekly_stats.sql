{{ config(
    materialized='incremental',
    unique_key=['period', 'blockchain']
) }}

WITH all_transfers AS (
    SELECT * FROM {{ ref('dbt_tether_tron') }}
    UNION ALL
    SELECT * FROM {{ ref('dbt_tether_ethereum') }}
    UNION ALL
    SELECT * FROM {{ ref('dbt_tether_polygon') }}
    UNION ALL
    SELECT * FROM {{ ref('dbt_tether_bnb') }}
    UNION ALL
    SELECT * FROM {{ ref('dbt_tether_arbitrum') }}
)

SELECT 
    date_trunc('month', dt) as period, 
    blockchain,
    SUM(amount) as amount, 
    COUNT(*) as txs,
    -- Use approx_distinct for massive performance gain on large datasets
    APPROX_DISTINCT("from") as senders,
    APPROX_DISTINCT("to") as receivers,
    
    -- Optimized Count Aggregates using FILTER (DuneSQL/Trino native)
    COUNT(*) FILTER (WHERE amount < 100) as tx_100,
    COUNT(*) FILTER (WHERE amount < 1000) as tx_1000,
    COUNT(*) FILTER (WHERE amount < 10000) as tx_10000,
    COUNT(*) FILTER (WHERE amount < 100000) as tx_100000,
    COUNT(*) FILTER (WHERE amount < 1000000) as tx_1000000,
    COUNT(*) FILTER (WHERE amount >= 1000000) as tx_10000000,
    
    -- Optimized Sum Aggregates
    SUM(amount) FILTER (WHERE amount < 100) as amt_100,
    SUM(amount) FILTER (WHERE amount < 1000) as amt_1000,
    SUM(amount) FILTER (WHERE amount < 10000) as amt_10000,
    SUM(amount) FILTER (WHERE amount < 100000) as amt_100000,
    SUM(amount) FILTER (WHERE amount < 1000000) as amt_1000000,
    SUM(amount) FILTER (WHERE amount >= 1000000) as amt_10000000
FROM all_transfers
{% if is_incremental() %}
    WHERE period >= date_trunc('week', NOW())
{% endif %}
GROUP BY 1, 2