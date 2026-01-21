import { NextResponse } from "next/server";
import pool from "@/lib/db";

export async function GET() {
  try {
    const result = await pool.query(`
      SELECT
        icao24,
        callsign,
        latitude,
        longitude,
        geo_altitude,
        velocity,
        on_ground,
        country_name,
        true_track
      FROM v_latest_positions
      WHERE latitude IS NOT NULL
        AND longitude IS NOT NULL
        AND NOT on_ground
    `);

    return NextResponse.json({
      success: true,
      count: result.rows.length,
      flights: result.rows,
    });
  } catch (error) {
    console.error("Database error:", error);
    return NextResponse.json(
      { success: false, error: "Failed to fetch flights" },
      { status: 500 },
    );
  }
}
