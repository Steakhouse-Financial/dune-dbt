{{ config(
    alias = 'dbt_tether_solana'
    , materialized='incremental'
    , incremental_strategy='delete+insert'
    , properties ={
        "partitioned_by": "ARRAY['dt']"
    }
    , unique_key=['dt', 'tx_hash', 'evt_index']
    , enabled = true
) }}

SELECT 
    DATE(block_time) AS dt,
    block_time as period,
    'solana' AS blockchain,
    amount / power(10, 6) AS amount,
    from_owner as "from",
    to_owner as "to",
    tx_id as tx_hash,
    tx_index as evt_index
FROM {{ source('tokens_solana', 'tether_solana_transfers') }}
WHERE token_mint_address = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
{% if is_incremental() %}
AND block_time >= NOW() - interval '2' day
{% endif %}