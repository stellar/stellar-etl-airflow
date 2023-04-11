with
    current_offers as (
        select
            o.seller_id
            , o.offer_id
            , o.selling_asset_type
            , o.selling_asset_code
            , o.selling_asset_issuer
            , o.buying_asset_type
            , o.buying_asset_code
            , o.buying_asset_issuer
            , o.amount
            , o.pricen
            , o.priced
            , o.price
            , o.flags
            , o.last_modified_ledger
            , l.closed_at
            , o.ledger_entry_change
            , o.deleted
            , o.sponsor
            , dense_rank() over (
                partition by o.seller_id, o.offer_id
                order by o.last_modified_ledger desc o.ledger_entry_change desc
            ) as rank_number
        from `hubble-261722.crypto_stellar_internal_2.offers` as o
        join `hubble-261722.crypto_stellar_internal_2.history_ledgers` as l
            on o.last_modified_ledger = l.sequence
        group by
            seller_id
            , offer_id
            , selling_asset_type
            , selling_asset_code
            , selling_asset_issuer
            , buying_asset_type
            , buying_asset_code
            , buying_asset_issuer
            , amount
            , pricen
            , priced
            , price
            , flags
            , last_modified_ledger
            , closed_at
            , ledger_entry_change
            , deleted
            , sponsor
    )
select
    seller_id
    , offer_id
    , selling_asset_type
    , selling_asset_code
    , selling_asset_issuer
    , buying_asset_type
    , buying_asset_code
    , buying_asset_issuer
    , amount
    , pricen
    , priced
    , price
    , flags
    , last_modified_ledger
    , closed_at
    , ledger_entry_change
    , deleted
    , sponsor
from current_offers
where rank_number = 1
