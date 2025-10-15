-- Which product categories receive the highest/lowest review scores?
-- This query shows the top 5 and bottom 5 categories by average review score.

with category_reviews as (
    select
        product_category,
        avg(review_score) as average_review_score,
        count(distinct order_id) as number_of_reviews
    from {{ ref('fact_orders') }}
    where product_category is not null
    group by 1
),

top_5 as (
    select *
    from category_reviews
    order by average_review_score desc
    limit 5
),

bottom_5 as (
    select *
    from category_reviews
    order by average_review_score asc
    limit 5
)

select * from top_5
union all
select * from bottom_5
order by average_review_score desc;