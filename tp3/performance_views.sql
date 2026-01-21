-- Vues pour les métriques de performance de la BDD

-- Vue: Statistiques des tables
CREATE OR REPLACE VIEW v_table_stats AS
SELECT
    schemaname,
    relname as tablename,
    pg_size_pretty(pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(relname))) as total_size,
    pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(relname)) as total_size_bytes,
    n_tup_ins as rows_inserted,
    n_tup_upd as rows_updated,
    n_tup_del as rows_deleted,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(quote_ident(schemaname)||'.'||quote_ident(relname)) DESC;

-- Vue: Taille des index
CREATE OR REPLACE VIEW v_index_stats AS
SELECT
    schemaname,
    relname as tablename,
    indexrelname as indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;

-- Vue: Performance des requêtes
CREATE OR REPLACE VIEW v_query_performance AS
SELECT
    NOW() as time,
    COUNT(*) as total_connections,
    SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END) as active_queries,
    SUM(CASE WHEN state = 'idle' THEN 1 ELSE 0 END) as idle_connections,
    SUM(CASE WHEN wait_event_type IS NOT NULL THEN 1 ELSE 0 END) as waiting_queries
FROM pg_stat_activity
WHERE datname = current_database();

-- Vue: Taille de la base de données
CREATE OR REPLACE VIEW v_database_size AS
SELECT
    pg_database.datname as database_name,
    pg_size_pretty(pg_database_size(pg_database.datname)) as size,
    pg_database_size(pg_database.datname) as size_bytes
FROM pg_database
WHERE pg_database.datname = current_database();

-- Vue: Cache hit ratio
CREATE OR REPLACE VIEW v_cache_hit_ratio AS
SELECT
    NOW() as time,
    sum(heap_blks_read) as heap_read,
    sum(heap_blks_hit) as heap_hit,
    CASE
        WHEN sum(heap_blks_hit) + sum(heap_blks_read) = 0 THEN 0
        ELSE (sum(heap_blks_hit)::float / (sum(heap_blks_hit) + sum(heap_blks_read))) * 100
    END as cache_hit_ratio
FROM pg_statio_user_tables;

-- Vue: Métriques temps réel des vols
CREATE OR REPLACE VIEW v_flight_metrics_realtime AS
SELECT
    NOW() as time,
    COUNT(DISTINCT aircraft_id) as total_aircraft,
    COUNT(*) as total_positions,
    AVG(CASE WHEN NOT on_ground THEN geo_altitude END) as avg_altitude,
    AVG(CASE WHEN NOT on_ground THEN velocity END) as avg_velocity,
    MAX(velocity) as max_velocity,
    SUM(CASE WHEN on_ground THEN 1 ELSE 0 END) as aircraft_on_ground,
    SUM(CASE WHEN NOT on_ground THEN 1 ELSE 0 END) as aircraft_airborne
FROM fact_flight_positions
WHERE ingestion_time > NOW() - INTERVAL '5 minutes';

-- Vue: Nombre d'avions par pays (dernières 5 min)
CREATE OR REPLACE VIEW v_aircraft_by_country AS
SELECT
    dc.country_name,
    COUNT(DISTINCT fp.aircraft_id) as aircraft_count
FROM fact_flight_positions fp
LEFT JOIN dim_country dc ON fp.country_id = dc.country_id
WHERE fp.ingestion_time > NOW() - INTERVAL '5 minutes'
GROUP BY dc.country_name
ORDER BY aircraft_count DESC;

-- Vue: Évolution du trafic par minute
CREATE OR REPLACE VIEW v_traffic_per_minute AS
SELECT
    date_trunc('minute', ingestion_time) as time,
    COUNT(DISTINCT aircraft_id) as aircraft_count,
    AVG(CASE WHEN NOT on_ground THEN velocity END) as avg_velocity,
    AVG(CASE WHEN NOT on_ground THEN geo_altitude END) as avg_altitude
FROM fact_flight_positions
WHERE ingestion_time > NOW() - INTERVAL '1 hour'
GROUP BY date_trunc('minute', ingestion_time)
ORDER BY time;
