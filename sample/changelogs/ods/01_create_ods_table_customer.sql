CREATE TABLE {{ ods_dataset }}.customer
(
    customer_id UInt64,
    name String,
    created_at DateTime
)
ENGINE = MergeTree()
ORDER BY customer_id