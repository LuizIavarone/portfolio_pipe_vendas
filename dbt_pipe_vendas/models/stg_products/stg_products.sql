WITH raw_data AS (
    SELECT
        id,
        title,
        CAST(price AS FLOAT64) AS price,
        description,
        category,
        image,
        CAST(rating_rate AS STRING) AS rating_rate,
        CAST(rating_count AS STRING) AS rating_count,
        CAST(dt_processamento AS DATETIME) AS dt_processamento
    FROM
        `portfolio-440702.raw.tb_raw_products`
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
FROM raw_data
