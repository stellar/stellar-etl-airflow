with
    current_lps as (
        select
            lp.liquidity_pool_id
            , lp.asset_pair
            , lp.asset_a_code
            , lp.asset_a_issuer
            , lp.asset_a_type
            , lp.asset_b_code
            , lp.asset_b_issuer
            , lp.asset_b_type
            , lp.asset_a_amount
            , lp.asset_b_amount
        from `hubble-261722.crypto_stellar_internal_2.v_liquidity_pools_current` as lp
        where deleted is false
    )
    , asset_xlm_price as (
        select
            asset_code
            , asset_issuer
            , price_in_xlm
            , rank() over (partition by asset_code, asset_issuer order by last_updated_ts desc) as rank_nr
        from `hubble-261722.crypto_stellar_internal_2.asset_prices_xlm`
    )
    , asset_a_value as (
        select
            a.liquidity_pool_id
            , a.asset_pair
            , a.asset_a_code
            , a.asset_a_issuer
            , a.asset_a_amount
            , coalesce(p.price_in_xlm, 0) as price_in_xlm
            , case
                when a.asset_a_type = 'native'
                    then a.asset_a_amount
                else a.asset_a_amount * coalesce(p.price_in_xlm, 0)
            end as asset_a_value_xlm
        from current_lps as a
        left outer join asset_xlm_price as p
            on p.asset_code = a.asset_a_code
            and p.asset_issuer = a.asset_a_issuer
            and p.rank_nr = 1
    )
    , asset_b_value as (
        select
            b.liquidity_pool_id
            , b.asset_pair
            , b.asset_b_code
            , b.asset_b_issuer
            , b.asset_b_amount
            , coalesce(p.price_in_xlm, 0) as price_in_xlm
            , b.asset_b_amount * coalesce(p.price_in_xlm, 0) as asset_b_value_xlm
        from current_lps as b
        left outer join asset_xlm_price as p
            on p.asset_code = b.asset_b_code
            and p.asset_issuer = b.asset_b_issuer
            and p.rank_nr = 1
    )
    , xlm_price as (
        select
            price_in_usd
            , rank() over (order by last_updated_ts desc) as rank_nr
        from `hubble-261722.crypto_stellar_internal_2.asset_prices_usd`
    )
select
    a.liquidity_pool_id
    , a.asset_pair
    , a.asset_a_value_xlm
    , b.asset_b_value_xlm
    , xlm.price_in_usd as xlm_price_usd
    , a.asset_a_value_xlm * xlm.price_in_usd as asset_a_usd_value
    , b.asset_b_value_xlm * xlm.price_in_usd as asset_b_usd_value
    , ((a.asset_a_value_xlm * xlm.price_in_usd) + (b.asset_b_value_xlm * xlm.price_in_usd)) as total_value_locked
from asset_a_value as a
join xlm_price as xlm
    on 1 = 1
join asset_b_value as b
    on a.liquidity_pool_id = b.liquidity_pool_id
where xlm.rank_nr = 1
order by total_value_locked desc