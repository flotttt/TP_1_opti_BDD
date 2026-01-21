"use client";

import { useMemo } from "react";

interface Flight {
  icao24: string;
  callsign: string;
  latitude: number;
  longitude: number;
  geo_altitude: number;
  velocity: number;
  on_ground: boolean;
  country_name: string;
  true_track: number;
}

interface FlightDashboardProps {
  flights: Flight[];
  visibleFlights: Flight[];
  onFlightClick?: (flight: Flight) => void;
}

export default function FlightDashboard({
  flights,
  visibleFlights,
  onFlightClick,
}: FlightDashboardProps) {
  const stats = useMemo(() => {
    const airborne = flights.filter((f) => !f.on_ground);

    const fastest = airborne.reduce(
      (max, f) => (f.velocity > (max?.velocity || 0) ? f : max),
      airborne[0],
    );

    const highest = airborne.reduce(
      (max, f) => (f.geo_altitude > (max?.geo_altitude || 0) ? f : max),
      airborne[0],
    );

    const slowest = airborne.reduce(
      (min, f) =>
        f.velocity < (min?.velocity || Infinity) && f.velocity > 0 ? f : min,
      airborne[0],
    );

    const countries = flights.reduce(
      (acc, f) => {
        acc[f.country_name] = (acc[f.country_name] || 0) + 1;
        return acc;
      },
      {} as Record<string, number>,
    );

    const topCountries = Object.entries(countries)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5);

    const avgSpeed =
      airborne.reduce((sum, f) => sum + f.velocity, 0) / airborne.length;
    const avgAltitude =
      airborne.reduce((sum, f) => sum + f.geo_altitude, 0) / airborne.length;

    return {
      fastest,
      highest,
      slowest,
      topCountries,
      avgSpeed,
      avgAltitude,
      totalAirborne: airborne.length,
      totalOnGround: flights.length - airborne.length,
    };
  }, [flights]);

  const visibleStats = useMemo(() => {
    if (visibleFlights.length === 0) return null;

    const airborne = visibleFlights.filter((f) => !f.on_ground);

    const fastest = airborne.reduce(
      (max, f) => (f.velocity > (max?.velocity || 0) ? f : max),
      airborne[0],
    );

    const highest = airborne.reduce(
      (max, f) => (f.geo_altitude > (max?.geo_altitude || 0) ? f : max),
      airborne[0],
    );

    return { fastest, highest };
  }, [visibleFlights]);

  return (
    <div className="absolute top-4 right-4 z-[1000] w-80 space-y-3">
      <div className="bg-gradient-to-br from-slate-900 to-slate-800 backdrop-blur-sm p-4 rounded-xl shadow-2xl border border-slate-700">
        <h2 className="text-lg font-bold mb-3 text-white flex items-center gap-2">
          <span className="text-2xl">📊</span> Stats Globales
        </h2>

        <div className="space-y-2 text-sm">
          <div className="flex justify-between text-emerald-400">
            <span>En vol:</span>
            <span className="font-bold">{stats.totalAirborne}</span>
          </div>
          <div className="flex justify-between text-slate-400">
            <span>Au sol:</span>
            <span className="font-bold">{stats.totalOnGround}</span>
          </div>

          <div className="border-t border-slate-700 pt-2 mt-2">
            <div className="flex justify-between text-blue-400">
              <span>Vitesse moy.:</span>
              <span className="font-bold">
                {Math.round(stats.avgSpeed * 3.6)} km/h
              </span>
            </div>
            <div className="flex justify-between text-purple-400">
              <span>Altitude moy.:</span>
              <span className="font-bold">
                {Math.round(stats.avgAltitude)} m
              </span>
            </div>
          </div>
        </div>
      </div>

      <div className="bg-gradient-to-br from-yellow-900 to-orange-900 backdrop-blur-sm p-4 rounded-xl shadow-2xl border border-yellow-700">
        <h3 className="text-md font-bold mb-2 text-yellow-100 flex items-center gap-2">
          <span className="text-xl">🏆</span> Records Mondiaux
        </h3>

        <div className="space-y-2 text-xs">
          {stats.fastest && (
            <div
              onClick={() => onFlightClick?.(stats.fastest)}
              className="bg-red-500/20 p-2 rounded-lg border border-red-500/30 cursor-pointer hover:bg-red-500/30 transition-all hover:scale-105"
            >
              <div className="text-red-300 font-semibold">Plus Rapide 🚀</div>
              <div className="text-white font-bold">
                {stats.fastest.callsign || stats.fastest.icao24}
              </div>
              <div className="text-red-200">
                {Math.round(stats.fastest.velocity * 3.6)} km/h
              </div>
              <div className="text-red-400 text-[10px] mt-1">
                Cliquer pour localiser
              </div>
            </div>
          )}

          {stats.highest && (
            <div
              onClick={() => onFlightClick?.(stats.highest)}
              className="bg-blue-500/20 p-2 rounded-lg border border-blue-500/30 cursor-pointer hover:bg-blue-500/30 transition-all hover:scale-105"
            >
              <div className="text-blue-300 font-semibold">Plus Haut ⬆️</div>
              <div className="text-white font-bold">
                {stats.highest.callsign || stats.highest.icao24}
              </div>
              <div className="text-blue-200">
                {Math.round(stats.highest.geo_altitude)} m
              </div>
              <div className="text-blue-400 text-[10px] mt-1">
                Cliquer pour localiser
              </div>
            </div>
          )}

          {stats.slowest && (
            <div
              onClick={() => onFlightClick?.(stats.slowest)}
              className="bg-green-500/20 p-2 rounded-lg border border-green-500/30 cursor-pointer hover:bg-green-500/30 transition-all hover:scale-105"
            >
              <div className="text-green-300 font-semibold">Plus Lent 🐌</div>
              <div className="text-white font-bold">
                {stats.slowest.callsign || stats.slowest.icao24}
              </div>
              <div className="text-green-200">
                {Math.round(stats.slowest.velocity * 3.6)} km/h
              </div>
              <div className="text-green-400 text-[10px] mt-1">
                Cliquer pour localiser
              </div>
            </div>
          )}
        </div>
      </div>

      {visibleFlights.length > 0 && (
        <div className="bg-gradient-to-br from-indigo-900 to-purple-900 backdrop-blur-sm p-4 rounded-xl shadow-2xl border border-indigo-700">
          <h3 className="text-md font-bold mb-2 text-indigo-100 flex items-center gap-2">
            <span className="text-xl">👁️</span> Vue Actuelle (
            {visibleFlights.length})
          </h3>

          {visibleStats && (
            <div className="space-y-2 text-xs">
              {visibleStats.fastest && (
                <div
                  onClick={() => onFlightClick?.(visibleStats.fastest)}
                  className="bg-pink-500/20 p-2 rounded-lg border border-pink-500/30 cursor-pointer hover:bg-pink-500/30 transition-all hover:scale-105"
                >
                  <div className="text-pink-300 font-semibold">
                    Champion local 👑
                  </div>
                  <div className="text-white font-bold">
                    {visibleStats.fastest.callsign ||
                      visibleStats.fastest.icao24}
                  </div>
                  <div className="text-pink-200">
                    {Math.round(visibleStats.fastest.velocity * 3.6)} km/h
                  </div>
                  <div className="text-pink-400 text-[10px] mt-1">
                    Cliquer pour centrer
                  </div>
                </div>
              )}

              {visibleStats.highest && (
                <div
                  onClick={() => onFlightClick?.(visibleStats.highest)}
                  className="bg-cyan-500/20 p-2 rounded-lg border border-cyan-500/30 cursor-pointer hover:bg-cyan-500/30 transition-all hover:scale-105"
                >
                  <div className="text-cyan-300 font-semibold">
                    Plus haut visible 🔭
                  </div>
                  <div className="text-white font-bold">
                    {visibleStats.highest.callsign ||
                      visibleStats.highest.icao24}
                  </div>
                  <div className="text-cyan-200">
                    {Math.round(visibleStats.highest.geo_altitude)} m
                  </div>
                  <div className="text-cyan-400 text-[10px] mt-1">
                    Cliquer pour centrer
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      <div className="bg-gradient-to-br from-slate-800 to-slate-900 backdrop-blur-sm p-4 rounded-xl shadow-2xl border border-slate-600">
        <h3 className="text-md font-bold mb-2 text-slate-100 flex items-center gap-2">
          <span className="text-xl">🌍</span> Top Pays
        </h3>

        <div className="space-y-1 text-xs">
          {stats.topCountries.map(([country, count], idx) => (
            <div key={country} className="flex justify-between items-center">
              <span className="text-slate-300 flex items-center gap-2">
                <span className="text-yellow-400">{idx + 1}.</span>
                {country}
              </span>
              <span className="font-bold text-white bg-slate-700 px-2 py-0.5 rounded">
                {count}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
