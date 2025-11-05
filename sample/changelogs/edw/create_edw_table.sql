CREATE TABLE {{ edw_dataset }}.customer_summary
(
    customer_id UInt64,
    name String,
    order_count UInt32,
    last_order_date DateTime
)
ENGINE = MergeTree()
ORDER BY customer_id