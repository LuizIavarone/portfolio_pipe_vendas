CREATE OR REPLACE TABLE `portfolio-440702.trusted.stg_products` (
  id STRING,
  title STRING,
  price FLOAT64,
  description STRING,
  category STRING,
  image STRING,
  rating_rate STRING,
  rating_count STRING,
  dt_processamento DATETIME
)
PARTITION BY DATE(dt_processamento)
CLUSTER BY category;
