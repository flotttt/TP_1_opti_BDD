-- Schema PostgreSQL optimisé pour le tracking de vols

-- Table de dimension: Avions
CREATE TABLE IF NOT EXISTS dim_aircraft (
    aircraft_id SERIAL PRIMARY KEY,
    icao24 VARCHAR(10) UNIQUE NOT NULL,
    first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Table de dimension: Pays
CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

-- Table de faits: Positions des vols
CREATE TABLE IF NOT EXISTS fact_flight_positions (
    position_id BIGSERIAL PRIMARY KEY,
    aircraft_id INTEGER NOT NULL REFERENCES dim_aircraft(aircraft_id),
    country_id INTEGER REFERENCES dim_country(country_id),
    callsign VARCHAR(20),
    longitude DOUBLE PRECISION NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    geo_altitude DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    true_track DOUBLE PRECISION,
    on_ground BOOLEAN,
    api_timestamp INTEGER NOT NULL,
    ingestion_time TIMESTAMP NOT NULL,
    processed_time TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(aircraft_id, api_timestamp)
);

-- Table aggregée: Statistiques par heure
CREATE TABLE IF NOT EXISTS agg_hourly_stats (
    stat_id SERIAL PRIMARY KEY,
    hour_timestamp TIMESTAMP NOT NULL,
    country_id INTEGER REFERENCES dim_country(country_id),
    total_flights INTEGER,
    avg_altitude DOUBLE PRECISION,
    avg_velocity DOUBLE PRECISION,
    flights_on_ground INTEGER,
    flights_airborne INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(hour_timestamp, country_id)
);

-- Index pour performance
CREATE INDEX IF NOT EXISTS idx_flight_positions_aircraft ON fact_flight_positions(aircraft_id);
CREATE INDEX IF NOT EXISTS idx_flight_positions_timestamp ON fact_flight_positions(api_timestamp);
CREATE INDEX IF NOT EXISTS idx_flight_positions_ingestion ON fact_flight_positions(ingestion_time);
CREATE INDEX IF NOT EXISTS idx_flight_positions_geo ON fact_flight_positions(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_aircraft_icao24 ON dim_aircraft(icao24);
CREATE INDEX IF NOT EXISTS idx_aircraft_last_seen ON dim_aircraft(last_seen);
CREATE INDEX IF NOT EXISTS idx_hourly_stats_hour ON agg_hourly_stats(hour_timestamp);

-- Vue: Dernière position connue de chaque avion
CREATE OR REPLACE VIEW v_latest_positions AS
SELECT
    da.icao24,
    fp.callsign,
    dc.country_name,
    fp.longitude,
    fp.latitude,
    fp.geo_altitude,
    fp.velocity,
    fp.true_track,
    fp.on_ground,
    fp.ingestion_time,
    fp.api_timestamp
FROM fact_flight_positions fp
INNER JOIN dim_aircraft da ON fp.aircraft_id = da.aircraft_id
LEFT JOIN dim_country dc ON fp.country_id = dc.country_id
WHERE fp.position_id IN (
    SELECT MAX(position_id)
    FROM fact_flight_positions
    GROUP BY aircraft_id
);

-- Vue: Statistiques en temps réel
CREATE OR REPLACE VIEW v_realtime_stats AS
SELECT
    dc.country_name,
    COUNT(*) as total_flights,
    AVG(fp.geo_altitude) as avg_altitude,
    AVG(fp.velocity) as avg_velocity,
    SUM(CASE WHEN fp.on_ground THEN 1 ELSE 0 END) as flights_on_ground,
    SUM(CASE WHEN NOT fp.on_ground THEN 1 ELSE 0 END) as flights_airborne
FROM fact_flight_positions fp
LEFT JOIN dim_country dc ON fp.country_id = dc.country_id
WHERE fp.ingestion_time > NOW() - INTERVAL '5 minutes'
GROUP BY dc.country_name
ORDER BY total_flights DESC;

-- Vue: Top 10 avions les plus actifs (dernières 24h)
CREATE OR REPLACE VIEW v_top_active_aircraft AS
SELECT
    da.icao24,
    COUNT(*) as position_updates,
    MIN(fp.ingestion_time) as first_seen,
    MAX(fp.ingestion_time) as last_seen,
    AVG(fp.velocity) as avg_velocity
FROM fact_flight_positions fp
INNER JOIN dim_aircraft da ON fp.aircraft_id = da.aircraft_id
WHERE fp.ingestion_time > NOW() - INTERVAL '24 hours'
GROUP BY da.icao24
ORDER BY position_updates DESC
LIMIT 10;

-- Vue: Nombre d'avions par pays (pour Grafana)
CREATE OR REPLACE VIEW v_aircraft_by_country AS
SELECT
    dc.country_name,
    COUNT(DISTINCT da.aircraft_id) as aircraft_count
FROM dim_aircraft da
LEFT JOIN fact_flight_positions fp ON da.aircraft_id = fp.aircraft_id
LEFT JOIN dim_country dc ON fp.country_id = dc.country_id
WHERE fp.ingestion_time > NOW() - INTERVAL '24 hours'
GROUP BY dc.country_name
ORDER BY aircraft_count DESC;

-- Fonction pour nettoyer les anciennes données
CREATE OR REPLACE FUNCTION cleanup_old_positions(retention_hours INTEGER DEFAULT 48)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM fact_flight_positions
    WHERE ingestion_time < NOW() - (retention_hours || ' hours')::INTERVAL;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;

    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Fonction pour calculer les statistiques horaires
CREATE OR REPLACE FUNCTION aggregate_hourly_stats()
RETURNS INTEGER AS $$
DECLARE
    inserted_count INTEGER;
BEGIN
    INSERT INTO agg_hourly_stats (
        hour_timestamp,
        country_id,
        total_flights,
        avg_altitude,
        avg_velocity,
        flights_on_ground,
        flights_airborne
    )
    SELECT
        date_trunc('hour', fp.ingestion_time) as hour_timestamp,
        fp.country_id,
        COUNT(*) as total_flights,
        AVG(fp.geo_altitude) as avg_altitude,
        AVG(fp.velocity) as avg_velocity,
        SUM(CASE WHEN fp.on_ground THEN 1 ELSE 0 END) as flights_on_ground,
        SUM(CASE WHEN NOT fp.on_ground THEN 1 ELSE 0 END) as flights_airborne
    FROM fact_flight_positions fp
    WHERE fp.ingestion_time >= date_trunc('hour', NOW() - INTERVAL '1 hour')
      AND fp.ingestion_time < date_trunc('hour', NOW())
    GROUP BY date_trunc('hour', fp.ingestion_time), fp.country_id
    ON CONFLICT (hour_timestamp, country_id) DO UPDATE SET
        total_flights = EXCLUDED.total_flights,
        avg_altitude = EXCLUDED.avg_altitude,
        avg_velocity = EXCLUDED.avg_velocity,
        flights_on_ground = EXCLUDED.flights_on_ground,
        flights_airborne = EXCLUDED.flights_airborne;

    GET DIAGNOSTICS inserted_count = ROW_COUNT;

    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;
