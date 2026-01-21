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

const MarkerClusterGroup = dynamic(
  () => import("react-leaflet-cluster").then((mod) => mod.default),
  { ssr: false },
);

const AnimatedFlightMarker = dynamic(() => import("./AnimatedFlightMarker"), {
  ssr: false,
});

const FlightDashboard = dynamic(() => import("./FlightDashboard"), {
  ssr: false,
});

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

const iconCache = new Map<string, any>();

const getCountryFlag = (countryName: string): string => {
  const countryFlags: Record<string, string> = {
    France: "🇫🇷",
    "United States": "🇺🇸",
    Germany: "🇩🇪",
    "United Kingdom": "🇬🇧",
    Spain: "🇪🇸",
    Italy: "🇮🇹",
    Netherlands: "🇳🇱",
    Belgium: "🇧🇪",
    Switzerland: "🇨🇭",
    Austria: "🇦🇹",
    Portugal: "🇵🇹",
    Poland: "🇵🇱",
    Canada: "🇨🇦",
    Japan: "🇯🇵",
    China: "🇨🇳",
    Australia: "🇦🇺",
    Brazil: "🇧🇷",
    India: "🇮🇳",
    Russia: "🇷🇺",
    Mexico: "🇲🇽",
    Turkey: "🇹🇷",
    "South Korea": "🇰🇷",
    Sweden: "🇸🇪",
    Norway: "🇳🇴",
    Denmark: "🇩🇰",
    Finland: "🇫🇮",
    Ireland: "🇮🇪",
    Greece: "🇬🇷",
    "Czech Republic": "🇨🇿",
    Romania: "🇷🇴",
    Hungary: "🇭🇺",
    "United Arab Emirates": "🇦🇪",
    "Saudi Arabia": "🇸🇦",
    Qatar: "🇶🇦",
    Singapore: "🇸🇬",
    Thailand: "🇹🇭",
    Indonesia: "🇮🇩",
    Malaysia: "🇲🇾",
    "South Africa": "🇿🇦",
    Egypt: "🇪🇬",
    Israel: "🇮🇱",
    "New Zealand": "🇳🇿",
    Argentina: "🇦🇷",
    Chile: "🇨🇱",
    Colombia: "🇨🇴",
  };
  return countryFlags[countryName] || "🌐";
};

const createRotatedIcon = (
  rotation: number,
  countryName: string,
  isFastest: boolean = false,
  isHighest: boolean = false,
) => {
  if (typeof window === "undefined") return null;

  const roundedRotation = Math.round(rotation / 5) * 5;
  const cacheKey = `${roundedRotation}-${countryName}-${isFastest}-${isHighest}`;

  if (!isFastest && !isHighest && iconCache.has(cacheKey)) {
    return iconCache.get(cacheKey);
  }

  const L = require("leaflet");
  const flag = getCountryFlag(countryName);

  let planeColor = "#2563eb";
  let strokeColor = "#1e40af";
  let glow = "";

  if (isFastest) {
    planeColor = "#ef4444";
    strokeColor = "#dc2626";
    glow = "filter: drop-shadow(0 0 3px #ef4444);";
  } else if (isHighest) {
    planeColor = "#3b82f6";
    strokeColor = "#1e3a8a";
    glow = "filter: drop-shadow(0 0 3px #3b82f6);";
  }

  const icon = L.divIcon({
    html: `
      <div style="transform: rotate(${roundedRotation}deg); transform-origin: center; width: 24px; height: 24px; position: relative;">
        <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 32 32" style="position: absolute; top: 2px; left: 2px; ${glow}">
          <g transform="translate(16,16)">
            <path d="M 0,-12 L -2,-8 L -8,-6 L -8,-4 L -2,-5 L -2,6 L -4,8 L -4,10 L 0,9 L 4,10 L 4,8 L 2,6 L 2,-5 L 8,-4 L 8,-6 L 2,-8 Z"
                  fill="${planeColor}"
                  stroke="${strokeColor}"
                  stroke-width="0.5"/>
          </g>
        </svg>
        <div style="position: absolute; top: -6px; right: -6px; font-size: 12px; text-shadow: 0 0 2px black;">${flag}</div>
      </div>
    `,
    className: "airplane-marker",
    iconSize: [24, 24],
    iconAnchor: [12, 12],
    popupAnchor: [0, -12],
  });

  if (!isFastest && !isHighest) {
    iconCache.set(cacheKey, icon);
  }

  return icon;
};

export default function FlightMap() {
  const [flights, setFlights] = useState<Flight[]>([]);
  const [visibleFlights, setVisibleFlights] = useState<Flight[]>([]);
  const [loading, setLoading] = useState(true);
  const mapRef = useRef<any>(null);

  const handleFlightClick = (flight: Flight) => {
    if (mapRef.current) {
      mapRef.current.setView([flight.latitude, flight.longitude], 8, {
        animate: true,
        duration: 1.5,
      });
    }
  };

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
    const interval = setInterval(fetchFlights, 10000);

    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    const updateVisibleFlights = () => {
      if (!mapRef.current) return;

      const map = mapRef.current;
      const bounds = map.getBounds();

      const visible = flights.filter((flight) => {
        const lat = flight.latitude;
        const lng = flight.longitude;
        return bounds.contains([lat, lng]);
      });

      setVisibleFlights(visible);
    };

    if (mapRef.current && flights.length > 0) {
      updateVisibleFlights();

      const map = mapRef.current;
      map.on("moveend", updateVisibleFlights);
      map.on("zoomend", updateVisibleFlights);

      return () => {
        map.off("moveend", updateVisibleFlights);
        map.off("zoomend", updateVisibleFlights);
      };
    }
  }, [flights]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen bg-gray-900">
        <div className="text-xl text-white">✈️ Chargement des vols...</div>
      </div>
    );
  }

  const airborne = flights.filter((f) => !f.on_ground);
  const fastest = airborne.reduce(
    (max, f) => (f.velocity > (max?.velocity || 0) ? f : max),
    airborne[0],
  );
  const highest = airborne.reduce(
    (max, f) => (f.geo_altitude > (max?.geo_altitude || 0) ? f : max),
    airborne[0],
  );

  return (
    <div className="relative w-full h-screen">
      <FlightDashboard
        flights={flights}
        visibleFlights={visibleFlights}
        onFlightClick={handleFlightClick}
      />

      <MapContainer
        key="flight-map"
        center={[20, 0]}
        zoom={2}
        minZoom={2}
        maxZoom={18}
        worldCopyJump={false}
        maxBounds={[
          [-90, -180],
          [90, 180],
        ]}
        maxBoundsViscosity={1.0}
        style={{ height: "100vh", width: "100%" }}
        className="z-0"
        ref={mapRef}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
          url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
          subdomains="abcd"
          maxZoom={20}
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
            const isFastest = fastest && flight.icao24 === fastest.icao24;
            const isHighest = highest && flight.icao24 === highest.icao24;
            const icon = createRotatedIcon(
              rotation,
              flight.country_name,
              isFastest,
              isHighest,
            );

            return icon ? (
              <AnimatedFlightMarker
                key={flight.icao24}
                icao24={flight.icao24}
                callsign={flight.callsign}
                latitude={flight.latitude}
                longitude={flight.longitude}
                geo_altitude={flight.geo_altitude}
                velocity={flight.velocity}
                country_name={flight.country_name}
                true_track={flight.true_track}
                icon={icon}
              />
            ) : null;
          })}
        </MarkerClusterGroup>
      </MapContainer>
    </div>
  );
}
