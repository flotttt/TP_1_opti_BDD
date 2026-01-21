-- ============================================================================
-- OPTIMISATIONS TP3 - PHASE 3
-- ============================================================================
-- Objectif: Améliorer les performances des requêtes critiques
-- Méthode: Vue matérialisée + Index optimisés
-- ============================================================================

-- 1. SUPPRIMER L'ANCIENNE VUE (si elle existe)
DROP VIEW IF EXISTS v_latest_positions CASCADE;

-- 2. CRÉER UNE VUE MATÉRIALISÉE pour les dernières positions
-- Avantage: Les données sont calculées UNE SEULE FOIS et mises en cache
CREATE MATERIALIZED VIEW mv_latest_positions AS
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
    fp.api_timestamp,
    fp.aircraft_id,
    fp.position_id
FROM fact_flight_positions fp
INNER JOIN dim_aircraft da ON fp.aircraft_id = da.aircraft_id
LEFT JOIN dim_country dc ON fp.country_id = dc.country_id
WHERE fp.position_id IN (
    SELECT MAX(position_id)
    FROM fact_flight_positions
    GROUP BY aircraft_id
);

-- 3. CRÉER DES INDEX SUR LA VUE MATÉRIALISÉE
-- Index pour les filtres les plus courants
CREATE INDEX idx_mv_latest_on_ground ON mv_latest_positions(on_ground);
CREATE INDEX idx_mv_latest_velocity ON mv_latest_positions(velocity DESC);
CREATE INDEX idx_mv_latest_geo ON mv_latest_positions(latitude, longitude) WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
CREATE INDEX idx_mv_latest_icao24 ON mv_latest_positions(icao24);

-- 4. CRÉER UNE VUE SIMPLE QUI POINTE VERS LA VUE MATÉRIALISÉE
-- Pour garder la compatibilité avec le code existant
CREATE VIEW v_latest_positions AS
SELECT
    icao24,
    callsign,
    country_name,
    longitude,
    latitude,
    geo_altitude,
    velocity,
    true_track,
    on_ground,
    ingestion_time,
    api_timestamp
FROM mv_latest_positions;

-- 5. FONCTION POUR RAFRAÎCHIR LA VUE MATÉRIALISÉE
CREATE OR REPLACE FUNCTION refresh_latest_positions()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_latest_positions;
END;
$$ LANGUAGE plpgsql;

-- 6. STATISTIQUES
ANALYZE mv_latest_positions;

-- ============================================================================
-- RÉSUMÉ DES OPTIMISATIONS
-- ============================================================================
-- ✅ Vue matérialisée au lieu de vue normale → calcul une seule fois
-- ✅ Index sur on_ground → filtre NOT on_ground ultra-rapide
-- ✅ Index sur velocity DESC → ORDER BY velocity instantané
-- ✅ Index sur (latitude, longitude) → filtre géographique optimisé
-- ✅ Index sur icao24 → recherche par avion rapide
--
-- Rafraîchissement: Appeler refresh_latest_positions() toutes les 1-2 minutes
-- ============================================================================
