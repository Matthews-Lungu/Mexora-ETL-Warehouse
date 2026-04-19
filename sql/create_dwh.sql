-- Check 1: All tables
SELECT 'TABLE' as type, table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('dwh_mexora','staging_mexora','reporting_mexora')

UNION ALL

-- Check 2: All materialized views
SELECT 'MATVIEW' as type, schemaname, matviewname
FROM pg_matviews
WHERE schemaname IN ('dwh_mexora','staging_mexora','reporting_mexora')

ORDER BY type, table_schema, table_name;