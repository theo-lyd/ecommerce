{{ config(
    materialized='table',
    tags=['mart']
) }}

with
sellers as (
    select * from {{ ref('stg_sellers') }}
),

closed_deals as (
    select * from {{ ref('stg_closed_deals') }}
),

marketing_leads as (
    select * from {{ ref('stg_marketing_qualified_leads') }}
),

-- First, create a clear view of the marketing funnel
marketing_funnel as (
    select
        m.mql_id,
        m.first_contact_date,
        m.origin as lead_source,
        cd.seller_id,
        cd.lead_type,
        cd.business_segment,
        cast(cd.won_date as date) as won_date,
        -- Calculate funnel metrics here
        -- Subtracting two 'date' types directly gives an integer (number of days)
        (cast(cd.won_date as date) - m.first_contact_date) as days_from_lead_to_close
    from marketing_leads as m
    left join closed_deals as cd on m.mql_id = cd.mql_id
),

final as (
    select
        s.seller_id,
        s.city as seller_city,
        s.state as seller_state,

        -- Bring in the marketing funnel information
        mf.lead_source,
        mf.lead_type,
        mf.business_segment,
        mf.first_contact_date,
        mf.won_date,
        mf.days_from_lead_to_close

    from sellers as s
    -- Use a left join to ensure all sellers are included,
    -- even those without a marketing funnel entry.
    left join marketing_funnel as mf on s.seller_id = mf.seller_id
)

select * from final