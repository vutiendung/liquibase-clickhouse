CREATE TABLE {{ datamart_dataset }}.user_report
(
    dashboard_id UInt64,
    sales_amount Float64,
    report_date Date
)
ENGINE = MergeTree()
ORDER BY dashboard_id