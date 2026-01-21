"use client";

import { useEffect, useState, useRef } from "react";
import dynamic from "next/dynamic";
import "leaflet/dist/leaflet.css";

const MapContainer = dynamic(
  () => import("react-leaflet").then((mod) => mod.MapContainer),
  { ssr: false },
);
const TileLayer = dynamic(
  () => import("react-leaflet").then((mod) => mod.TileLayer),
  { ssr: false },
);
const Marker = dynamic(
  () => import("react-leaflet").then((mod) => mod.Marker),
  { ssr: false },
);
const Popup = dynamic(() => import("react-leaflet").then((mod) => mod.Popup), {
  ssr: false,
});

const MarkerClusterGroup = dynamic(
  () => import("react-leaflet-cluster").then((mod) => mod.default),
  { ssr: false },
);

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

// Fonction pour créer une icône tournée
const createRotatedIcon = (rotation: number) => {
  if (typeof window === "undefined") return null;
  const L = require("leaflet");

  return L.divIcon({
    html: `
      <div style="transform: rotate(${rotation}deg); transform-origin: center; width: 16px; height: 16px;">
        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="#3b82f6">
          <path d="M21 16v-2l-8-5V3.5c0-.83-.67-1.5-1.5-1.5S10 2.67 10 3.5V9l-8 5v2l8-2.5V19l-2 1.5V22l3.5-1 3.5 1v-1.5L13 19v-5.5l8 2.5z"/>
        </svg>
      </div>
    `,
    className: "custom-airplane-icon",
    iconSize: [16, 16],
    iconAnchor: [8, 8],
    popupAnchor: [0, -8],
  });
};

export default function FlightMap() {
  const [flights, setFlights] = useState<Flight[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchFlights = async () => {
      try {
        const res = await fetch("/api/flights");
        const data = await res.json();
        if (data.success) {
          setFlights(data.flights);
        }
      } catch (error) {
        console.error("Error fetching flights:", error);
      } finally {
        setLoading(false);
      }
    };

    fetchFlights();
    const interval = setInterval(fetchFlights, 10000); // Refresh every 10s pour réduire la charge

    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen bg-gray-900">
        <div className="text-xl text-white">✈️ Chargement des vols...</div>
      </div>
    );
  }

  return (
    <div className="relative w-full h-screen">
      <div className="absolute top-4 left-4 z-[1000] bg-white/90 backdrop-blur-sm p-4 rounded-lg shadow-lg">
        <h2 className="text-xl font-bold mb-2 flex items-center gap-2">
          <span className="text-2xl">🛫</span> Suivi des Vols en Direct
        </h2>
        <div className="space-y-1 text-sm">
          <p className="text-gray-700">
            <span className="font-semibold">Avions en vol:</span>{" "}
            {flights.length}
          </p>
          <p className="text-gray-500 text-xs">
            Mise à jour toutes les 10 secondes
          </p>
        </div>
      </div>

      <MapContainer
        key="flight-map"
        center={[20, 0]}
        zoom={2}
        style={{ height: "100vh", width: "100%" }}
        className="z-0"
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        <MarkerClusterGroup
          chunkedLoading
          maxClusterRadius={50}
          spiderfyOnMaxZoom={true}
          showCoverageOnHover={false}
          zoomToBoundsOnClick={true}
        >
          {flights.map((flight) => {
            const rotation = flight.true_track || 0;
            const icon = createRotatedIcon(rotation);

            return icon ? (
              <Marker
                key={flight.icao24}
                position={[flight.latitude, flight.longitude]}
                icon={icon}
              >
                <Popup>
                  <div className="text-sm space-y-1">
                    <p className="font-bold text-blue-600 text-base">
                      {flight.callsign || flight.icao24}
                    </p>
                    <p className="text-gray-700">
                      <span className="font-semibold">🌍 Pays:</span>{" "}
                      {flight.country_name}
                    </p>
                    <p className="text-gray-700">
                      <span className="font-semibold">📏 Altitude:</span>{" "}
                      {Math.round(flight.geo_altitude)}m
                    </p>
                    <p className="text-gray-700">
                      <span className="font-semibold">⚡ Vitesse:</span>{" "}
                      {Math.round(flight.velocity * 3.6)}km/h
                    </p>
                    <p className="text-gray-700">
                      <span className="font-semibold">🧭 Direction:</span>{" "}
                      {Math.round(rotation)}°
                    </p>
                  </div>
                </Popup>
              </Marker>
            ) : null;
          })}
        </MarkerClusterGroup>
      </MapContainer>
    </div>
  );
}
