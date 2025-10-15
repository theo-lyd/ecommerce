{{ config(
    materialized='view',
    tags=['staging']
) }}

with source as (
    select * from {{ source('olist_raw', 'order_reviews') }}
),

renamed as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        cast(review_creation_date as timestamp) as review_creation_at,
        cast(review_answer_timestamp as timestamp) as review_answered_at

    from source
)

select * from renamed