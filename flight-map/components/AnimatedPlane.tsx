'use client';

import { useEffect, useRef } from 'react';
import { Marker, Popup } from 'react-leaflet';
import { Marker as LeafletMarker } from 'leaflet';

interface AnimatedPlaneProps {
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

export default function AnimatedPlane(props: AnimatedPlaneProps) {
  const markerRef = useRef<LeafletMarker>(null);
  const previousPosition = useRef({ lat: props.latitude, lng: props.longitude });
  const animationFrame = useRef<number>();

  useEffect(() => {
    const marker = markerRef.current;
    if (!marker) return;

    const startPos = previousPosition.current;
    const endPos = { lat: props.latitude, lng: props.longitude };

    // Si la position a changé, animer
    if (startPos.lat !== endPos.lat || startPos.lng !== endPos.lng) {
      const startTime = Date.now();
      const duration = 5000; // 5 secondes pour correspondre au refresh

      const animate = () => {
        const elapsed = Date.now() - startTime;
        const progress = Math.min(elapsed / duration, 1);

        // Easing pour un mouvement plus naturel
        const eased = progress < 0.5
          ? 2 * progress * progress
          : 1 - Math.pow(-2 * progress + 2, 2) / 2;

        const currentLat = startPos.lat + (endPos.lat - startPos.lat) * eased;
        const currentLng = startPos.lng + (endPos.lng - startPos.lng) * eased;

        marker.setLatLng([currentLat, currentLng]);

        if (progress < 1) {
          animationFrame.current = requestAnimationFrame(animate);
        } else {
          previousPosition.current = endPos;
        }
      };

      if (animationFrame.current) {
        cancelAnimationFrame(animationFrame.current);
      }

      animate();
    }

    return () => {
      if (animationFrame.current) {
        cancelAnimationFrame(animationFrame.current);
      }
    };
  }, [props.latitude, props.longitude]);

  return (
    <Marker
      ref={markerRef}
      position={[props.latitude, props.longitude]}
      icon={props.icon}
    >
      <Popup>
        <div className="text-sm space-y-1">
          <p className="font-bold text-blue-600 text-base">
            {props.callsign || props.icao24}
          </p>
          <p className="text-gray-700">
            <span className="font-semibold">🌍 Pays:</span> {props.country_name}
          </p>
          <p className="text-gray-700">
            <span className="font-semibold">📏 Altitude:</span> {Math.round(props.geo_altitude)}m
          </p>
          <p className="text-gray-700">
            <span className="font-semibold">⚡ Vitesse:</span> {Math.round(props.velocity * 3.6)}km/h
          </p>
          <p className="text-gray-700">
            <span className="font-semibold">🧭 Direction:</span> {Math.round(props.true_track)}°
          </p>
        </div>
      </Popup>
    </Marker>
  );
}
