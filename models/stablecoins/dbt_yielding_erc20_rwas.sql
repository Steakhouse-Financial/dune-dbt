{#
    key notes on yielding rwas model:
    - Uses merge strategy with custom deduplication logic

    - On incremental runs, filters out rows that already exist in the target table
    - Checks target table using unique key columns to prevent duplicates
#}


{{ config(
    alias = 'dbt_yielding_erc20_rwas'
    , materialized = 'incremental'
    , incremental_strategy = 'delete+insert'
    , properties = {
        "partitioned_by": "ARRAY['dt', 'blockchain']"
    }
)
}}

with
erc20_tokens as (
    select protocol, blockchain, from_hex(token_address) as token_address, token_name, token_decimals, start_date
    from {{ref('erc20_rwa_tokens')}}
)
, unioned_tokens as (
    select protocol, blockchain, token_address, token_name, token_decimals, start_date from erc20_tokens
)
-- generate time sequence for each chain and token starting from their creation date
, seq as (
    select
        t.blockchain,
        t.protocol,
        t.token_address,
        t.token_name,
        date(s.dt) as dt
    from (
        select blockchain, protocol, token_address, token_name, start_date
        from unioned_tokens
    ) t
    cross join unnest(sequence(t.start_date, current_date, interval '1' day)) as s(dt)
),
backed_transfers as (
    /* Backed Finance */
    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    from 
        {{ source('bibta_gnosis', 'backedtokenimplementation_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'gnosis'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3
    
),
ondo_transfers as (
    /* Ondo Finance */
    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    from 
        {{ source('ondofinance_ethereum', 'ondofinance_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'ethereum'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3
    UNION all
    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    from 
        {{ source('ondofinance_ethereum', 'usdy_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'ethereum'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3
    UNION all
    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    from 
        {{ source('ondofinance_ethereum', 'usdyc_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'ethereum'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    GROUP BY 1, 2, 3
    
)
, blackrock_transfers as (
    /* Blackrock */
    select t.evt_block_date as dt
        , t.contract_address
        , 'ethereum' as blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('blackrock_buidl_ethereum', 'dstoken_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'ethereum'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2
    UNION all
    select t.evt_block_date as dt
        , tk.token_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('buidl_token_multichain', 'buidl_token_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = t.chain
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3
)
, franklin_transfers as (

    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('franklin_templeton_multichain', 'moneymarketfund_v5_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and t.chain = tk.blockchain
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval '1' day)
    {% endif %}
    GROUP BY 1, 2, 3

)
, superstate_transfers as (
    /* Superstate */
    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('superstate_ethereum', 'ustb_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'ethereum'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3
    UNION all
    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('superstate_ethereum', 'superstatetokenv5_1_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'ethereum'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3

)
, centrifuge_transfers as (
    /* Centrifuge */
    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('jtrsy_multichain', 'tranche_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = t.chain
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3

)
, hashnote_transfers as (

    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.amount
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.amount
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('hashnote_multichain', 'shortdurationyieldcoin_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = t.chain
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3

)
, wisdom_transfers as (

    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('wtgxx_ethereum', 'erc20revocablecompliancestandard_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'ethereum'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3

)
, dinari_transfers as (

    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.amount
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.amount
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('dinari_arbitrum', 'usfrd_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'ethereum'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3

)
, openeden_transfers as (
    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('tbill_ethereum', 'openedenvaultv2_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = 'ethereum'
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
    {% if is_incremental() %}
        AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
    {% endif %}
    GROUP BY 1, 2, 3
)
, spiko_transfers as (

    select t.evt_block_date as dt
        , t.contract_address
        , tk.blockchain
        , SUM(
            CASE 
                when t."from" = 0x0000000000000000000000000000000000000000 then t.value
                when t."to" = 0x0000000000000000000000000000000000000000 then -t.value
            END / POWER(10, tk.token_decimals)
        ) as amount
    FROM {{ source('spiko_multichain', 'token_evt_transfer')}} as t
            join erc20_tokens as tk on t.contract_address = tk.token_address
            and t.evt_block_date >= tk.start_date
            and tk.blockchain = t.chain
    WHERE 0x0000000000000000000000000000000000000000 in (t."from", t."to")
        {% if is_incremental() %}
            AND t.evt_block_date >= date_trunc('day', NOW() - interval'1' day)
        {% endif %}
    GROUP BY 1, 2, 3

)
, rwa_transfers as (
    select dt, contract_address, blockchain, amount
    FROM backed_transfers
    UNION all
    select dt, contract_address, blockchain, amount
    FROM ondo_transfers
    UNION all
    select dt, contract_address, blockchain, amount
    FROM blackrock_transfers
    UNION all
    select dt, contract_address, blockchain, amount
    FROM superstate_transfers
    UNION all
    select dt, contract_address, blockchain, amount
    FROM hashnote_transfers
    UNION all
    select dt, contract_address, blockchain, amount
    FROM dinari_transfers
    UNION all
    select dt, contract_address, blockchain, amount
    FROM openeden_transfers
    UNION all
    select dt, contract_address, blockchain, amount
    FROM spiko_transfers
),
rwa_daily as (
    select
        seq.blockchain,
        seq.dt,
        seq.protocol,
        seq.token_name,
        seq.token_address,
        coalesce(tr.amount, 0) as daily_net_amount
    from seq left join (
        select dt, contract_address, blockchain, amount from rwa_transfers
    ) tr on seq.dt = tr.dt and seq.token_address = tr.contract_address
    and seq.blockchain = tr.blockchain
)
select * 
from rwa_daily

