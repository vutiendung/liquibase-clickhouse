CREATE TABLE {{ ods_dataset }}.line_user_activity (
    user_id UInt64,
    activity_type String,
    activity_time DateTime,
    details String
) ENGINE = MergeTree()
ORDER BY activity_time