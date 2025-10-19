{{ config(
    materialized='table',
    tags=['causal']
) }}

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

closed_deals as (
    select * from {{ ref('stg_closed_deals') }}
),

marketing_leads as (
    select * from {{ ref('stg_marketing_qualified_leads') }}
),

-- Aggregate deal info to the seller level
seller_deal_info as (
    select
        seller_id,
        max(mql_id) as mql_id, -- Assume one MQL per seller for simplicity here
        max(business_segment) as business_segment,
        max(lead_type) as lead_type,
        1 as is_converted -- If they are in closed_deals, they converted
    from closed_deals
    group by 1
),

-- Join MQL info using the mql_id from deals
seller_mql_info as (
    select
        sdi.seller_id,
        sdi.is_converted,
        sdi.business_segment,
        sdi.lead_type,
        ml.origin as mql_origin,
        1 as is_mql -- If they have joined MQL info, they are an MQL
    from seller_deal_info sdi
    inner join marketing_leads ml on sdi.mql_id = ml.mql_id
),

final as (
    select
        s.seller_id,
        coalesce(sm.is_mql, 0) as is_mql, -- Treatment: 1 if MQL, 0 otherwise
        coalesce(sm.is_converted, 0) as is_converted, -- Outcome: 1 if converted, 0 otherwise

        -- Potential Confounders (Features X)
        -- WARNING: These features are primarily from closed_deals/MQLs,
        -- potentially introducing bias as they might be missing or different for non-MQL sellers.
        -- This is a major limitation of the available data for causal inference.
        sm.business_segment,
        sm.lead_type,
        sm.mql_origin

        -- We are excluding seller geography for now due to unclear timing (pre/post treatment)
        -- s.city as seller_city,
        -- s.state as seller_state

    from sellers s
    -- Left join to keep all sellers, even those not in the MQL funnel
    left join seller_mql_info sm on s.seller_id = sm.seller_id
)

select * from final