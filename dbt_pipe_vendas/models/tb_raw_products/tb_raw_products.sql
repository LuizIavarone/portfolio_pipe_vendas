{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT * FROM `portfolio-440702.raw.tb_raw_products`
)

SELECT
    id,
    title,
    price,
    description,
    category,
    image,
    rating_rate,
    rating_count,
    dt_processamento
FROM source_data
