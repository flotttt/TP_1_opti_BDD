"use client";

import { memo } from "react";
import { Marker, Popup } from "react-leaflet";

interface AnimatedFlightMarkerProps {
  icao24: string;
  callsign: string;
  latitude: number;
  longitude: number;
  geo_altitude: number;
  velocity: number;
  country_name: string;
  true_track: number;
  icon: any;
}

function AnimatedFlightMarker(props: AnimatedFlightMarkerProps) {
  return (
    <Marker position={[props.latitude, props.longitude]} icon={props.icon}>
      <Popup>
        <div className="text-sm space-y-1">
          <p className="font-bold text-blue-600 text-base">
            {props.callsign || props.icao24}
          </p>
          <p className="text-gray-700">
            <span className="font-semibold">Pays:</span> {props.country_name}
          </p>
          <p className="text-gray-700">
            <span className="font-semibold">Altitude:</span>{" "}
            {Math.round(props.geo_altitude)}m
          </p>
          <p className="text-gray-700">
            <span className="font-semibold">Vitesse:</span>{" "}
            {Math.round(props.velocity * 3.6)}km/h
          </p>
          <p className="text-gray-700">
            <span className="font-semibold">Direction:</span>{" "}
            {Math.round(props.true_track)}°
          </p>
        </div>
      </Popup>
    </Marker>
  );
}

export default memo(AnimatedFlightMarker, (prev, next) => {
  return (
    prev.latitude === next.latitude &&
    prev.longitude === next.longitude &&
    prev.true_track === next.true_track
  );
});
