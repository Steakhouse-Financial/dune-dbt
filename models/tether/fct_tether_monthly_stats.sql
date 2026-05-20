{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['period', 'blockchain']
) }}

WITH all_transfers AS (
    SELECT dt, amount, blockchain, CAST("from" as VARCHAR) as "from", CAST("to" as VARCHAR) as "to" FROM {{ ref('dbt_tether_tron') }}
    {% if is_incremental() %} WHERE dt >= date_trunc('month', NOW() - interval '2' day) {% endif %}
    UNION ALL
    SELECT dt, amount, blockchain, CAST("from" as VARCHAR) as "from", CAST("to" as VARCHAR) as "to" FROM {{ ref('dbt_tether_ethereum') }}
    {% if is_incremental() %} WHERE dt >= date_trunc('month', NOW() - interval '2' day) {% endif %}
    UNION ALL
    SELECT dt, amount, blockchain, CAST("from" as VARCHAR) as "from", CAST("to" as VARCHAR) as "to" FROM {{ ref('dbt_tether_polygon') }}
    {% if is_incremental() %} WHERE dt >= date_trunc('month', NOW() - interval '2' day) {% endif %}
    UNION ALL
    SELECT dt, amount, blockchain, CAST("from" as VARCHAR) as "from", CAST("to" as VARCHAR) as "to" FROM {{ ref('dbt_tether_bnb') }}
    {% if is_incremental() %} WHERE dt >= date_trunc('month', NOW() - interval '2' day) {% endif %}
    UNION ALL
    SELECT dt, amount, blockchain, CAST("from" as VARCHAR) as "from", CAST("to" as VARCHAR) as "to" FROM {{ ref('dbt_tether_arbitrum') }}
    {% if is_incremental() %} WHERE dt >= date_trunc('month', NOW() - interval '2' day) {% endif %}
    UNION ALL
    SELECT dt, amount, blockchain, "from", "to" FROM {{ ref('dbt_tether_solana') }}
    {% if is_incremental() %} WHERE dt >= date_trunc('month', NOW() - interval '2' day) {% endif %}
)

SELECT 
    date_trunc('month', dt) as period, 
    blockchain,
    SUM(amount) as amount, 
    COUNT(*) as txs,
    -- Use approx_distinct for massive performance gain on large datasets
    COUNT(DISTINCT "from") as senders,
    COUNT(DISTINCT "to") as receivers,
    
    -- Optimized Count Aggregates using FILTER (DuneSQL/Trino native)
    COUNT(*) FILTER (WHERE amount < 100) as tx_100,
    COUNT(*) FILTER (WHERE amount >= 100 AND amount < 1000) as tx_1000,
    COUNT(*) FILTER (WHERE amount >= 1000 AND amount < 10000) as tx_10000,
    COUNT(*) FILTER (WHERE amount >= 10000 AND amount < 100000) as tx_100000,
    COUNT(*) FILTER (WHERE amount >= 100000 AND amount < 1000000) as tx_1000000,
    COUNT(*) FILTER (WHERE amount >= 1000000) as tx_10000000,
    
    -- Optimized Sum Aggregates
    SUM(amount) FILTER (WHERE amount < 100) as amt_100,
    SUM(amount) FILTER (WHERE amount >= 100 AND amount < 1000) as amt_1000,
    SUM(amount) FILTER (WHERE amount >= 1000 AND amount < 10000) as amt_10000,
    SUM(amount) FILTER (WHERE amount >= 10000 AND amount < 100000) as amt_100000,
    SUM(amount) FILTER (WHERE amount >= 100000 AND amount < 1000000) as amt_1000000,
    SUM(amount) FILTER (WHERE amount >= 1000000) as amt_10000000
FROM all_transfers
GROUP BY 1, 2