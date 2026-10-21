# ============================================================
# AQUASENSE PRO v4.0 - ULTRA-EASY FARMER EDITION + REAL SMTP EMAIL
# FastAPI + SQLite + Plain-Language KPIs + Tech Details Toggle
# + Foolproof Pump Controls + Spoken Action Voice Confirmations
# + Real Gmail SMTP Email Delivery + English / Kannada / Hindi
# Save as main.py and run: python main.py
# ============================================================

from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Response, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

import os
import sqlite3
import threading
import time
import random
import math
import io
import csv
import json
import hashlib
import smtplib
import urllib.request
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone


# ============================================================
# APP CONFIG & GLOBAL STATE
# ============================================================

DB_PATH = "aquasense_pro.db"

ZONES = [
    "Zone_A_Laterite",
    "Zone_B_Alluvial"
]

DB_LOCK = threading.Lock()
WORKER_STARTED = False
LAST_ACTION = {"value": ""}

STATE = {
    "emergency_stop": False,
    "pump_states": {
        "Zone_A_Laterite": "AUTO",
        "Zone_B_Alluvial": "AUTO"
    },
    "active_action": "System Initializing",
    "watchdog": "NORMAL",
    "rain_probability": 18.0,
    "flow_expected_lpm": 0.0,
    "flow_actual_lpm": 0.0,
    "selected_region": "Coastal Laterite Soil (Udyavar / Udupi Corridor)",
    "selected_crop": "Paddy",
    "external_weather": {
        "source": "Standby / Local Udupi Baseline",
        "temp": 30.5,
        "humidity": 74.0,
        "wind_speed": 2.4,
        "rain_prob": 20.0,
        "last_synced": "Never"
    },
    "drone": {
        "status": "DOCKED - STANDBY",
        "battery_pct": 98,
        "altitude_m": 0.0,
        "last_scan": "Boot Baseline",
        "anomalies_found": 0,
        "thermal_grid": []
    },
    "carbon": {
        "total_water_saved_l": 1420.5,
        "kwh_consumed": 18.4,
        "solar_offset_pct": 84.0,
        "co2_saved_kg": 12.85,
        "efficiency_grade": "A+ (Ultra-Green)"
    },
    "mqtt_bus": {
        "broker": "mqtts://esp32-cluster.aquasense.io:8883",
        "connected_nodes": 12,
        "last_topic": "aquasense/esp32/boot",
        "last_payload": "CLUSTER_ONLINE_ACK",
        "latency_ms": 14,
        "last_published": "Boot Baseline"
    },
    "sms_gateway": {
        "provider": "SMTP Email & WhatsApp/SMS Gateway",
        "last_recipient": "+91-9876543210",
        "last_channel": "WHATSAPP + SMS",
        "last_message": "System armed and monitoring Udupi corridor.",
        "dispatched_count": 1,
        "last_sent": "Boot Baseline",
        "real_email_configured": False
    },
    "active_user": None
}

LIVE = {
    "Zone_A_Laterite": {
        "moisture": 47.5,
        "temp": 31.2,
        "humidity": 68.0,
        "ec": 1.25
    },
    "Zone_B_Alluvial": {
        "moisture": 56.0,
        "temp": 29.8,
        "humidity": 72.5,
        "ec": 0.95
    }
}


# ============================================================
# DATABASE, BLOCKCHAIN & REAL SMTP EMAIL HELPERS
# ============================================================

def utcnow() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def compute_sha256(block_str: str) -> str:
    return hashlib.sha256(block_str.encode("utf-8")).hexdigest()


def add_audit_and_block(conn, pump_action: str, water_volume: float, duration_mins: int, triggered_by: str):
    ts = utcnow()
    conn.execute(
        """
        INSERT INTO pump_logs
        (timestamp, pump_action, water_volume_liters, duration_minutes, triggered_by)
        VALUES (?, ?, ?, ?, ?)
        """,
        (ts, pump_action, water_volume, duration_mins, triggered_by)
    )

    last_block = conn.execute(
        "SELECT block_hash FROM blockchain_ledger ORDER BY block_id DESC LIMIT 1"
    ).fetchone()
    prev_hash = last_block["block_hash"] if last_block else "0" * 64

    payload = f"{ts}|{pump_action}|{water_volume}|{duration_mins}|{triggered_by}|{prev_hash}"
    block_hash = compute_sha256(payload)

    conn.execute(
        """
        INSERT INTO blockchain_ledger
        (timestamp, event_summary, water_liters, triggered_by, prev_hash, block_hash)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (ts, pump_action, water_volume, triggered_by, prev_hash, block_hash)
    )
    return block_hash


def init_db():
    conn = get_db()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sensors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            zone_id TEXT,
            soil_moisture_vwc REAL,
            ambient_temp_c REAL,
            humidity_pct REAL,
            electrical_conductivity_ec REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS pump_logs (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            pump_action TEXT,
            water_volume_liters REAL,
            duration_minutes INTEGER,
            triggered_by TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fertigation_records (
            record_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            growth_stage TEXT,
            nitrogen_ppm REAL,
            phosphorus_ppm REAL,
            potassium_ppm REAL,
            leaching_risk_flag BOOLEAN
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_config (
            config_key TEXT PRIMARY KEY,
            config_value TEXT,
            updated_at DATETIME
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS blockchain_ledger (
            block_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            event_summary TEXT,
            water_liters REAL,
            triggered_by TEXT,
            prev_hash TEXT,
            block_hash TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS yield_forecasts (
            forecast_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            crop_variety TEXT,
            et0_mm_day REAL,
            avg_vwc_pct REAL,
            projected_yield_tons REAL,
            estimated_market_revenue_inr REAL,
            confidence_score REAL
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS rbac_audit (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            user_role TEXT,
            endpoint_accessed TEXT,
            access_status TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            contact_id TEXT UNIQUE,
            farmer_name TEXT,
            preferred_lang TEXT DEFAULT 'en',
            saved_region TEXT DEFAULT 'Coastal Laterite Soil (Udyavar / Udupi Corridor)',
            saved_crop TEXT DEFAULT 'Paddy',
            alerts_enabled BOOLEAN DEFAULT 1,
            last_login DATETIME
        )
    """)

    conn.commit()
    conn.close()


def ensure_config(conn):
    defaults = [
        ("crit_moisture_threshold", "30.0"),
        ("rain_override_threshold", "75.0"),
        ("maintenance_pump_minutes", "10"),
        ("pump_flow_lpm", "10.5"),
        ("smtp_sender_email", os.getenv("SMTP_EMAIL", "")),
        ("smtp_app_password", os.getenv("SMTP_PASSWORD", ""))
    ]

    for key, value in defaults:
        conn.execute(
            """
            INSERT OR IGNORE INTO system_config
            (config_key, config_value, updated_at)
            VALUES (?, ?, ?)
            """,
            (key, value, utcnow())
        )
    conn.commit()


def get_config(conn, key: str, default: str = "") -> str:
    row = conn.execute(
        "SELECT config_value FROM system_config WHERE config_key = ?",
        (key,)
    ).fetchone()
    return row["config_value"] if row else default


def set_config(conn, key: str, value: str):
    conn.execute(
        """
        INSERT OR REPLACE INTO system_config
        (config_key, config_value, updated_at)
        VALUES (?, ?, ?)
        """,
        (key, value, utcnow())
    )
    conn.commit()


def send_real_smtp_email(conn, to_email: str, subject: str, body_text: str) -> dict:
    """
    Sends a real email via Gmail SMTP SSL (port 465) if SMTP credentials are configured
    in environment variables or saved via the Sign-In modal.
    """
    sender_email = (get_config(conn, "smtp_sender_email", "") or os.getenv("SMTP_EMAIL", "")).strip()
    app_password = (get_config(conn, "smtp_app_password", "") or os.getenv("SMTP_PASSWORD", "")).strip().replace(" ", "")

    if not sender_email or not app_password:
        return {
            "real_sent": False,
            "mode": "LOCAL_DB_SYNC_ONLY",
            "note": "Saved to SQLite database! (Add Gmail + 16-digit App Password in Sign-In settings to send real emails to your inbox)."
        }

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"AquaSense Smart Farm <{sender_email}>"
        msg["To"] = to_email

        avg_m = round((LIVE["Zone_A_Laterite"]["moisture"] + LIVE["Zone_B_Alluvial"]["moisture"]) / 2, 1)
        avg_t = round((LIVE["Zone_A_Laterite"]["temp"] + LIVE["Zone_B_Alluvial"]["temp"]) / 2, 1)

        html_content = f"""
        <div style="font-family: Arial, sans-serif; max-width: 560px; margin: auto; background: #0b132b; color: #f8fafc; padding: 24px; border-radius: 16px; border: 2px solid #00F0FF;">
            <h2 style="color: #00F0FF; margin-top: 0;">💧 AquaSense Pro - Live Farm Report</h2>
            <p style="font-size: 15px; line-height: 1.6; color: #e2e8f0;">{body_text}</p>
            <div style="background: #1c2541; padding: 16px; border-radius: 12px; margin: 16px 0;">
                <p style="margin: 6px 0;">🌱 <b>Selected Crop:</b> {STATE['selected_crop']}</p>
                <p style="margin: 6px 0;">📍 <b>Farm Region:</b> {STATE['selected_region']}</p>
                <p style="margin: 6px 0;">💧 <b>Average Soil Water:</b> {avg_m}%</p>
                <p style="margin: 6px 0;">🌡️ <b>Air Temperature:</b> {avg_t}°C</p>
                <p style="margin: 6px 0;">🤖 <b>Smart System Action:</b> {STATE['active_action']}</p>
            </div>
            <p style="font-size: 12px; color: #94a3b8;">Sent automatically by your AquaSense Pro Smart Farm Server at {utcnow()} UTC.</p>
        </div>
        """

        msg.attach(MIMEText(body_text, "plain"))
        msg.attach(MIMEText(html_content, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=8) as server:
            server.login(sender_email, app_password)
            server.sendmail(sender_email, to_email, msg.as_string())

        STATE["sms_gateway"]["real_email_configured"] = True
        return {
            "real_sent": True,
            "mode": "LIVE_GMAIL_SMTP_SSL",
            "note": f"Real email delivered to {to_email} inbox via Gmail SMTP!"
        }
    except Exception as smtp_err:
        return {
            "real_sent": False,
            "mode": "SMTP_ERROR_FALLBACK",
            "note": f"Saved locally, but Gmail SMTP returned: {str(smtp_err)}"
        }


def generate_thermal_grid():
    cells = []
    for idx in range(12):
        zone_prefix = "A" if idx < 6 else "B"
        base_z = LIVE["Zone_A_Laterite"] if idx < 6 else LIVE["Zone_B_Alluvial"]
        cell_vwc = round(max(12.0, min(92.0, base_z["moisture"] + random.uniform(-8.0, 8.0))), 1)
        cell_temp = round(base_z["temp"] + random.uniform(-1.8, 2.4), 1)
        status = "OPTIMAL"
        if cell_vwc < 32.0 or cell_temp > 34.5:
            status = "THERMAL_STRESS"
        elif cell_vwc > 74.0:
            status = "SATURATED"

        cells.append({
            "node_id": f"N-{zone_prefix}{(idx % 6) + 1}",
            "zone": f"Zone_{zone_prefix}",
            "vwc": cell_vwc,
            "leaf_temp": cell_temp,
            "status": status
        })
    STATE["drone"]["thermal_grid"] = cells
    STATE["drone"]["anomalies_found"] = sum(1 for c in cells if c["status"] != "OPTIMAL")


# ============================================================
# AGRONOMIC, PEST, YIELD & CARBON INTELLIGENCE ENGINES
# ============================================================

KC_TABLE = {
    "initial": 0.40,
    "development": 0.70,
    "mid_season": 1.15,
    "late_season": 0.70
}

CROP_ECONOMICS = {
    "Paddy": {"base_tons_ha": 6.8, "price_per_ton_inr": 28500.0, "optimal_vwc": 58.0},
    "Arecanut": {"base_tons_ha": 3.4, "price_per_ton_inr": 465000.0, "optimal_vwc": 52.0},
    "Pepper": {"base_tons_ha": 1.9, "price_per_ton_inr": 610000.0, "optimal_vwc": 48.0},
    "Sugarcane": {"base_tons_ha": 82.0, "price_per_ton_inr": 3650.0, "optimal_vwc": 62.0},
    "Vegetables (General)": {"base_tons_ha": 24.5, "price_per_ton_inr": 22000.0, "optimal_vwc": 50.0}
}


def compute_et0(temp_c: float, humidity_pct: float, wind_ms: float, radiation_mj: float) -> float:
    es = 0.6108 * math.exp((17.27 * temp_c) / (temp_c + 237.3))
    ea = es * (humidity_pct / 100.0)
    vpd = max(0.05, es - ea)
    et0 = 0.0023 * (radiation_mj + 2.0) * (temp_c + 17.8) * math.sqrt(vpd) * (1 + 0.1 * wind_ms)
    return round(max(0.1, et0), 3)


def compute_kc(base_moisture: float, crop_stage: str) -> float:
    base_kc = KC_TABLE.get(crop_stage, KC_TABLE["mid_season"])
    stress_factor = min(1.0, max(0.3, base_moisture / 70.0))
    return round(base_kc * stress_factor, 3)


def compute_evapo_stress(temperature: float, humidity: float) -> float:
    return round(max(0.1, (temperature / 25.0) * ((100.0 - humidity) / 50.0)), 3)


def compute_and_store_yield_forecast(conn, crop: str, avg_vwc: float, temp_c: float, humidity_pct: float, wind_ms: float = 2.4, solar_mj: float = 18.5) -> dict:
    econ = CROP_ECONOMICS.get(crop, CROP_ECONOMICS["Paddy"])
    et0 = compute_et0(temp_c, humidity_pct, wind_ms, solar_mj)
    kc = compute_kc(avg_vwc, "mid_season")
    etc = round(et0 * kc, 3)

    vwc_deviation = abs(avg_vwc - econ["optimal_vwc"])
    moisture_efficiency = max(0.55, min(1.18, 1.12 - (vwc_deviation / 95.0)))
    et_synergy = max(0.82, min(1.12, 1.0 + ((4.5 - abs(etc - 4.2)) * 0.03)))

    farm_hectares = 5.0
    projected_tons = round(econ["base_tons_ha"] * farm_hectares * moisture_efficiency * et_synergy, 2)
    estimated_revenue_inr = round(projected_tons * econ["price_per_ton_inr"], 2)
    confidence_score = round(max(78.0, min(98.8, 96.5 - (vwc_deviation * 0.28))), 1)

    ts = utcnow()
    conn.execute(
        """
        INSERT INTO yield_forecasts
        (timestamp, crop_variety, et0_mm_day, avg_vwc_pct, projected_yield_tons, estimated_market_revenue_inr, confidence_score)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (ts, crop, et0, round(avg_vwc, 2), projected_tons, estimated_revenue_inr, confidence_score)
    )

    return {
        "timestamp": ts,
        "crop_variety": crop,
        "et0_mm_day": et0,
        "etc_mm_day": etc,
        "avg_vwc_pct": round(avg_vwc, 2),
        "projected_yield_tons": projected_tons,
        "estimated_market_revenue_inr": estimated_revenue_inr,
        "price_per_ton_inr": econ["price_per_ton_inr"],
        "confidence_score": confidence_score
    }


def evaluate_pest_and_disease(temp: float, humidity: float, moisture: float, crop: str) -> dict:
    leaf_wetness = round(min(100.0, max(10.0, (humidity * 0.65) + (moisture * 0.35))), 1)

    crop_threats = {
        "Paddy": ("Magnaporthe Oryzae (Paddy Leaf Blast)", "Apply Tricyclazole bio-fungicide & pause overhead misting"),
        "Arecanut": ("Phytophthora Meadii (Koleroga / Mahali Rot)", "Spray 1% Bordeaux mixture & clear crown drainage"),
        "Pepper": ("Phytophthora Capsici (Quick Wilt / Foot Rot)", "Drench root zone with Trichoderma viride bio-agent"),
        "Sugarcane": ("Colletotrichum Falcatum (Red Rot)", "Regulate furrow moisture & apply Pseudomonas fluorescens"),
        "Vegetables (General)": ("Powdery Mildew & Whitefly Vector", "Apply Neem Oil 1500 ppm botanical extract")
    }
    threat_name, bio_rx = crop_threats.get(crop, crop_threats["Paddy"])

    if humidity > 78.0 and temp >= 26.0:
        risk_level = "HIGH OUTBREAK RISK"
        confidence = round(min(96.0, 65.0 + (humidity - 75.0) * 1.4), 1)
        color = "rose"
    elif humidity > 64.0 or moisture > 68.0:
        risk_level = "MODERATE WATCH"
        confidence = round(42.0 + (humidity - 50.0) * 0.8, 1)
        color = "amber"
    else:
        risk_level = "LOW / BIO-SAFE"
        confidence = round(max(8.0, 25.0 - (70.0 - humidity) * 0.3), 1)
        bio_rx = "Weather is safe for crops. Keep normal drip watering."
        color = "emerald"

    return {
        "risk_level": risk_level,
        "threat_name": threat_name,
        "confidence_pct": confidence,
        "leaf_wetness_pct": leaf_wetness,
        "bio_prescription": bio_rx,
        "badge_color": color
    }


def seed_demo_data(conn):
    count = conn.execute("SELECT COUNT(*) AS c FROM sensors").fetchone()["c"]
    if count == 0:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for i in range(360, 0, -1):
            ts = now - timedelta(hours=i * 2)
            ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

            for zone_index, zone in enumerate(ZONES):
                moisture = max(8.0, min(95.0, 48 + 14 * math.sin(i / 12 + zone_index) + random.uniform(-6, 6)))
                temp = max(12.0, min(48.0, 28 + 6 * math.sin(i / 18 + zone_index) + random.uniform(-1.2, 1.2)))
                humidity = max(15.0, min(95.0, 66 + 12 * math.sin(i / 22 + zone_index) + random.uniform(-4, 4)))
                ec = max(0.2, min(4.0, 1.1 + 0.35 * math.sin(i / 26 + zone_index) + random.uniform(-0.08, 0.08)))

                conn.execute(
                    """
                    INSERT INTO sensors
                    (timestamp, zone_id, soil_moisture_vwc, ambient_temp_c, humidity_pct, electrical_conductivity_ec)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (ts_str, zone, round(moisture, 2), round(temp, 2), round(humidity, 2), round(ec, 2))
                )

    b_count = conn.execute("SELECT COUNT(*) AS c FROM blockchain_ledger").fetchone()["c"]
    if b_count == 0:
        add_audit_and_block(conn, "Genesis Block: AquaSense Pro Kernel Initialized", 0.0, 0, "System Genesis")

    y_count = conn.execute("SELECT COUNT(*) AS c FROM yield_forecasts").fetchone()["c"]
    if y_count == 0:
        compute_and_store_yield_forecast(conn, STATE["selected_crop"], 51.75, 30.5, 70.2)

    conn.commit()
    generate_thermal_grid()


# ============================================================
# MULTI-TENANT ROLE-BASED ACCESS CONTROL (verify_rbac)
# ============================================================

def verify_rbac(x_user_role: Optional[str] = Header(default="Operator", alias="X-User-Role")) -> str:
    valid_roles = ("Operator", "Auditor", "Administrator")
    role = x_user_role if x_user_role in valid_roles else "Operator"

    try:
        with DB_LOCK:
            conn = get_db()
            conn.execute(
                """
                INSERT INTO rbac_audit (timestamp, user_role, endpoint_accessed, access_status)
                VALUES (?, ?, ?, ?)
                """,
                (utcnow(), role, "RBAC_GATEWAY", "READ_ONLY" if role == "Auditor" else "AUTHORIZED")
            )
            conn.commit()
            conn.close()
    except Exception:
        pass

    return role


def require_write_permission(role: str, action_label: str):
    if role == "Auditor":
        raise HTTPException(
            status_code=403,
            detail=f"Viewer Mode Lock: Switch role to 'Farmer' at the top right to run '{action_label}'."
        )


# ============================================================
# BACKGROUND TELEMETRY SIMULATOR
# ============================================================

def telemetry_worker():
    global LAST_ACTION

    while True:
        time.sleep(5)

        try:
            with DB_LOCK:
                conn = get_db()

                crit_threshold = float(get_config(conn, "crit_moisture_threshold", "30.0"))
                rain_threshold = float(get_config(conn, "rain_override_threshold", "75.0"))
                flow_rate = float(get_config(conn, "pump_flow_lpm", "10.5"))

                timestamp = utcnow()
                total_moisture = 0.0
                total_temp = 0.0
                total_hum = 0.0
                active_pump = False

                for zone in ZONES:
                    values = LIVE[zone]
                    pump_state = STATE["pump_states"].get(zone, "AUTO")

                    drift = random.uniform(-0.55, 0.30)

                    if STATE["emergency_stop"]:
                        drift -= 0.10
                    if pump_state == "FORCE_OFF":
                        drift -= 0.15
                    if pump_state in ("FORCE_ON", "ACTIVE"):
                        drift += 1.25
                        active_pump = True

                    values["moisture"] = max(5.0, min(96.0, values["moisture"] + drift))
                    values["temp"] = max(10.0, min(50.0, values["temp"] + random.uniform(-0.25, 0.25)))
                    values["humidity"] = max(10.0, min(96.0, values["humidity"] + random.uniform(-0.75, 0.75)))
                    values["ec"] = max(0.2, min(4.5, values["ec"] + random.uniform(-0.03, 0.03)))

                    total_moisture += values["moisture"]
                    total_temp += values["temp"]
                    total_hum += values["humidity"]

                    conn.execute(
                        """
                        INSERT INTO sensors
                        (timestamp, zone_id, soil_moisture_vwc, ambient_temp_c, humidity_pct, electrical_conductivity_ec)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            timestamp,
                            zone,
                            round(values["moisture"], 2),
                            round(values["temp"], 2),
                            round(values["humidity"], 2),
                            round(values["ec"], 2)
                        )
                    )

                avg_moisture = total_moisture / max(1, len(ZONES))
                avg_temp = total_temp / max(1, len(ZONES))
                avg_hum = total_hum / max(1, len(ZONES))

                STATE["rain_probability"] = max(
                    0.0,
                    min(100.0, STATE["rain_probability"] + random.uniform(-2.5, 2.5))
                )

                expected_flow = flow_rate if active_pump else 0.0
                actual_flow = expected_flow * random.uniform(0.92, 1.08)
                if expected_flow == 0:
                    actual_flow = random.uniform(0.0, 0.15)

                STATE["flow_expected_lpm"] = round(expected_flow, 2)
                STATE["flow_actual_lpm"] = round(actual_flow, 2)

                if expected_flow > 0 and actual_flow < expected_flow * 0.70:
                    STATE["watchdog"] = "ALERT: Pipe Blocked / High Pressure"
                elif expected_flow > 0 and actual_flow > expected_flow * 1.30:
                    STATE["watchdog"] = "ALERT: Pipe Leak Detected"
                else:
                    STATE["watchdog"] = "NORMAL"

                if STATE["emergency_stop"]:
                    STATE["active_action"] = "ALL PUMPS STOPPED (EMERGENCY)"
                elif avg_moisture < crit_threshold:
                    STATE["active_action"] = "Soil is Dry - Watering Needed Now"
                elif avg_moisture > rain_threshold or STATE["rain_probability"] > 80:
                    STATE["active_action"] = "Rain Expected - Pumps Paused to Save Water"
                else:
                    STATE["active_action"] = "Farm is Healthy - Smart Auto Monitoring"

                for zone in ZONES:
                    if STATE["pump_states"].get(zone, "AUTO") in ("AUTO", "ACTIVE", "DRAINAGE"):
                        if STATE["emergency_stop"]:
                            STATE["pump_states"][zone] = "FORCE_OFF"
                        elif avg_moisture < crit_threshold:
                            STATE["pump_states"][zone] = "ACTIVE"
                        elif avg_moisture > rain_threshold:
                            STATE["pump_states"][zone] = "DRAINAGE"
                        else:
                            STATE["pump_states"][zone] = "AUTO"

                STATE["carbon"]["total_water_saved_l"] = round(STATE["carbon"]["total_water_saved_l"] + random.uniform(0.4, 1.8), 1)
                STATE["carbon"]["kwh_consumed"] = round(STATE["carbon"]["kwh_consumed"] + (0.04 if active_pump else 0.005), 2)
                STATE["carbon"]["co2_saved_kg"] = round(STATE["carbon"]["total_water_saved_l"] * 0.0091, 2)

                compute_and_store_yield_forecast(conn, STATE["selected_crop"], avg_moisture, avg_temp, avg_hum)

                if LAST_ACTION["value"] != STATE["active_action"]:
                    add_audit_and_block(
                        conn,
                        f"Smart Auto Status: {STATE['active_action']}",
                        0.0,
                        0,
                        "Smart Auto System"
                    )
                    LAST_ACTION["value"] = STATE["active_action"]

                conn.commit()
                conn.close()

        except Exception as error:
            print("Telemetry worker error:", error)


# ============================================================
# LIFESPAN & FASTAPI APP INITIALIZATION
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global WORKER_STARTED
    with DB_LOCK:
        init_db()
        conn = get_db()
        ensure_config(conn)
        seed_demo_data(conn)
        conn.close()

    if not WORKER_STARTED:
        worker = threading.Thread(target=telemetry_worker, daemon=True)
        worker.start()
        WORKER_STARTED = True
    yield


app = FastAPI(title="AquaSense Pro v4.0", version="4.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# PYDANTIC MODELS
# ============================================================

class UserLoginRequest(BaseModel):
    contact_id: str = Field(..., description="Email address or Phone Number")
    farmer_name: str = Field(default="Farmer")
    preferred_lang: str = Field(default="en")
    smtp_sender_email: Optional[str] = Field(default="")
    smtp_app_password: Optional[str] = Field(default="")


class UserSyncRequest(BaseModel):
    contact_id: str
    preferred_lang: str
    saved_region: str
    saved_crop: str


class OverrideRequest(BaseModel):
    zone_id: str
    action: str
    duration_mins: int = Field(default=10, ge=0, le=240)


class SimulationRequest(BaseModel):
    temperature: float = Field(..., ge=-20, le=60)
    humidity: float = Field(..., ge=0, le=100)
    base_moisture: float = Field(..., ge=0, le=100)
    wind_speed: float = Field(default=2.0, ge=0, le=20)
    solar_radiation: float = Field(default=18.0, ge=0, le=40)
    crop_stage: str = Field(default="mid_season")
    region: str = Field(default="Coastal Laterite Soil (Udyavar / Udupi)")
    crop_type: str = Field(default="Paddy")


class FertigationRequest(BaseModel):
    growth_stage: str
    nitrogen_ppm: float = Field(..., ge=0, le=1000)
    phosphorus_ppm: float = Field(..., ge=0, le=1000)
    potassium_ppm: float = Field(..., ge=0, le=1000)
    leaching_risk_flag: bool = False


class RegionCropRequest(BaseModel):
    region: str
    crop_type: str


class EmergencyRequest(BaseModel):
    engage: bool = True


class VoiceCommandRequest(BaseModel):
    transcript: str


class MQTTPublishRequest(BaseModel):
    node_id: str = Field(default="ESP32-NODE-A1")
    topic: str = Field(default="aquasense/esp32/zone_a/solenoid")
    payload: str = Field(default="PULSE_OPEN_10M")
    qos: int = Field(default=1, ge=0, le=2)


class SMSAlertRequest(BaseModel):
    recipient_phone: str = Field(default="+91-9876543210")
    channel: str = Field(default="WHATSAPP + SMS")
    alert_message: str = Field(default="AquaSense Farm Update: Soil moisture and pumps are working fine.")


# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)


@app.post("/api/auth/login")
def api_auth_login(data: UserLoginRequest):
    contact = data.contact_id.strip()
    if not contact:
        raise HTTPException(status_code=400, detail="Please enter a valid Email or Phone Number.")

    name = data.farmer_name.strip() or "Farmer"
    ts = utcnow()

    with DB_LOCK:
        conn = get_db()

        # Save optional real Gmail SMTP credentials if provided in the modal
        if data.smtp_sender_email and data.smtp_app_password:
            set_config(conn, "smtp_sender_email", data.smtp_sender_email.strip())
            set_config(conn, "smtp_app_password", data.smtp_app_password.strip())
            STATE["sms_gateway"]["real_email_configured"] = True

        existing = conn.execute(
            "SELECT * FROM users WHERE LOWER(contact_id) = LOWER(?)",
            (contact,)
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE users SET farmer_name = ?, last_login = ? WHERE user_id = ?",
                (name if name != "Farmer" else existing["farmer_name"], ts, existing["user_id"])
            )
            user_row = conn.execute("SELECT * FROM users WHERE user_id = ?", (existing["user_id"],)).fetchone()
            is_new = False
        else:
            conn.execute(
                """
                INSERT INTO users (contact_id, farmer_name, preferred_lang, saved_region, saved_crop, alerts_enabled, last_login)
                VALUES (?, ?, ?, ?, ?, 1, ?)
                """,
                (contact, name, data.preferred_lang, STATE["selected_region"], STATE["selected_crop"], ts)
            )
            user_row = conn.execute("SELECT * FROM users WHERE LOWER(contact_id) = LOWER(?)", (contact,)).fetchone()
            is_new = True

        user_dict = dict(user_row)
        STATE["active_user"] = user_dict
        STATE["selected_region"] = user_dict["saved_region"]
        STATE["selected_crop"] = user_dict["saved_crop"]
        STATE["sms_gateway"]["last_recipient"] = user_dict["contact_id"]

        channel_type = "EMAIL" if "@" in contact else "WHATSAPP + SMS"
        STATE["sms_gateway"]["last_channel"] = channel_type

        email_result = {"real_sent": False, "note": ""}
        if "@" in contact:
            email_result = send_real_smtp_email(
                conn,
                contact,
                "Welcome to AquaSense Pro - Your Farm Settings Are Saved!",
                f"Hello {user_dict['farmer_name']}, your farm profile (Crop: {user_dict['saved_crop']}, Region: {user_dict['saved_region']}) is now synced with {contact}."
            )

        add_audit_and_block(
            conn,
            f"Farmer Sign-In ({'New' if is_new else 'Restored'}): {user_dict['farmer_name']} [{contact}]",
            0.0,
            0,
            f"Auth ({contact})"
        )
        conn.commit()
        conn.close()

    return {
        "status": "success",
        "is_new": is_new,
        "user": user_dict,
        "email_delivery": email_result,
        "message": f"Welcome {user_dict['farmer_name']}! Settings saved for {contact}. {email_result.get('note', '')}"
    }


@app.post("/api/auth/sync")
def api_auth_sync(data: UserSyncRequest):
    contact = data.contact_id.strip()
    if not contact:
        raise HTTPException(status_code=400, detail="Not signed in.")

    with DB_LOCK:
        conn = get_db()
        conn.execute(
            """
            UPDATE users
            SET preferred_lang = ?, saved_region = ?, saved_crop = ?, last_login = ?
            WHERE LOWER(contact_id) = LOWER(?)
            """,
            (data.preferred_lang, data.saved_region, data.saved_crop, utcnow(), contact)
        )
        user_row = conn.execute("SELECT * FROM users WHERE LOWER(contact_id) = LOWER(?)", (contact,)).fetchone()
        if user_row:
            STATE["active_user"] = dict(user_row)
            STATE["sms_gateway"]["last_recipient"] = contact
        add_audit_and_block(
            conn,
            f"Saved User Preferences to {contact}: Crop={data.saved_crop}, Lang={data.preferred_lang}",
            0.0,
            0,
            f"Profile Sync ({contact})"
        )
        conn.commit()
        conn.close()

    return {
        "status": "success",
        "message": f"Saved settings to {contact}!"
    }


@app.get("/api/telemetry")
def api_telemetry(role: str = Depends(verify_rbac)):
    try:
        with DB_LOCK:
            conn = get_db()

            latest_zones = conn.execute(
                """
                SELECT s.*
                FROM sensors s
                JOIN (
                    SELECT zone_id, MAX(id) AS max_id
                    FROM sensors
                    GROUP BY zone_id
                ) latest
                ON s.id = latest.max_id
                ORDER BY s.zone_id
                """
            ).fetchall()

            recent_series = conn.execute(
                """
                SELECT
                    timestamp,
                    ROUND(AVG(soil_moisture_vwc), 2) AS moisture,
                    ROUND(AVG(ambient_temp_c), 2) AS temp,
                    ROUND(AVG(humidity_pct), 2) AS humidity
                FROM sensors
                GROUP BY timestamp
                ORDER BY MAX(id) DESC
                LIMIT 60
                """
            ).fetchall()

            pump_logs = conn.execute(
                """
                SELECT event_id, timestamp, pump_action, water_volume_liters, duration_minutes, triggered_by
                FROM pump_logs
                ORDER BY event_id DESC
                LIMIT 8
                """
            ).fetchall()

            blocks = conn.execute(
                """
                SELECT block_id, timestamp, event_summary, water_liters, triggered_by, prev_hash, block_hash
                FROM blockchain_ledger
                ORDER BY block_id DESC
                LIMIT 6
                """
            ).fetchall()

            latest_yield = conn.execute(
                """
                SELECT *
                FROM yield_forecasts
                ORDER BY forecast_id DESC
                LIMIT 1
                """
            ).fetchone()

            rbac_logs = conn.execute(
                """
                SELECT audit_id, timestamp, user_role, endpoint_accessed, access_status
                FROM rbac_audit
                ORDER BY audit_id DESC
                LIMIT 5
                """
            ).fetchall()

            smtp_configured = bool(get_config(conn, "smtp_sender_email", "") and get_config(conn, "smtp_app_password", ""))
            STATE["sms_gateway"]["real_email_configured"] = smtp_configured

            config = {
                "crit_moisture_threshold": get_config(conn, "crit_moisture_threshold", "30.0"),
                "rain_override_threshold": get_config(conn, "rain_override_threshold", "75.0"),
                "maintenance_pump_minutes": get_config(conn, "maintenance_pump_minutes", "10"),
                "pump_flow_lpm": get_config(conn, "pump_flow_lpm", "10.5"),
                "smtp_configured": smtp_configured
            }

            conn.close()

        zones_list = [dict(row) for row in latest_zones]
        avg_temp = sum(z["ambient_temp_c"] for z in zones_list) / max(1, len(zones_list))
        avg_hum = sum(z["humidity_pct"] for z in zones_list) / max(1, len(zones_list))
        avg_vwc = sum(z["soil_moisture_vwc"] for z in zones_list) / max(1, len(zones_list))

        pest_intel = evaluate_pest_and_disease(avg_temp, avg_hum, avg_vwc, STATE["selected_crop"])
        econ = CROP_ECONOMICS.get(STATE["selected_crop"], CROP_ECONOMICS["Paddy"])

        yield_dict = dict(latest_yield) if latest_yield else {
            "crop_variety": STATE["selected_crop"],
            "et0_mm_day": 4.12,
            "avg_vwc_pct": round(avg_vwc, 2),
            "projected_yield_tons": 34.0,
            "estimated_market_revenue_inr": 969000.0,
            "confidence_score": 94.5
        }
        yield_dict["price_per_ton_inr"] = econ["price_per_ton_inr"]

        return {
            "status": "success",
            "active_role": role,
            "active_user": STATE["active_user"],
            "system": {
                "emergency_stop": STATE["emergency_stop"],
                "active_action": STATE["active_action"],
                "pump_states": STATE["pump_states"],
                "watchdog": STATE["watchdog"],
                "rain_probability": round(STATE["rain_probability"], 1),
                "flow_expected_lpm": STATE["flow_expected_lpm"],
                "flow_actual_lpm": STATE["flow_actual_lpm"],
                "selected_region": STATE["selected_region"],
                "selected_crop": STATE["selected_crop"],
                "external_weather": STATE["external_weather"],
                "drone": STATE["drone"],
                "carbon": STATE["carbon"],
                "pest_intelligence": pest_intel,
                "yield_forecast": yield_dict,
                "mqtt_bus": STATE["mqtt_bus"],
                "sms_gateway": STATE["sms_gateway"]
            },
            "zones": zones_list,
            "recent_series": [dict(row) for row in reversed(recent_series)],
            "pump_logs": [dict(row) for row in pump_logs],
            "blockchain_ledger": [dict(row) for row in blocks],
            "rbac_logs": [dict(row) for row in rbac_logs],
            "config": config
        }

    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/api/mqtt/publish")
def api_mqtt_publish(data: MQTTPublishRequest, role: str = Depends(verify_rbac)):
    require_write_permission(role, "ESP32 Sensor Box Signal")
    try:
        latency = random.randint(9, 24)
        ts = utcnow()
        STATE["mqtt_bus"] = {
            "broker": "mqtts://esp32-cluster.aquasense.io:8883",
            "connected_nodes": 12,
            "last_topic": data.topic,
            "last_payload": f"[{data.node_id}] {data.payload}",
            "latency_ms": latency,
            "last_published": ts
        }

        summary = f"Wireless Signal [{data.node_id}] -> {data.payload}"
        with DB_LOCK:
            conn = get_db()
            block_hash = add_audit_and_block(conn, summary, 0.0, 0, f"ESP32 Bridge ({role})")
            conn.commit()
            conn.close()

        return {
            "status": "published_to_mqtt_broker",
            "broker": STATE["mqtt_bus"]["broker"],
            "node_id": data.node_id,
            "topic": data.topic,
            "payload": data.payload,
            "qos": data.qos,
            "esp32_ack_latency_ms": latency,
            "block_hash": block_hash,
            "timestamp": ts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/sms/alert")
def api_sms_alert(data: SMSAlertRequest, role: str = Depends(verify_rbac)):
    require_write_permission(role, "Phone / Email Report")
    try:
        ts = utcnow()
        recipient = data.recipient_phone.strip()
        STATE["sms_gateway"]["last_recipient"] = recipient
        STATE["sms_gateway"]["last_channel"] = data.channel
        STATE["sms_gateway"]["last_message"] = data.alert_message
        STATE["sms_gateway"]["dispatched_count"] += 1
        STATE["sms_gateway"]["last_sent"] = ts

        email_status = {"real_sent": False, "note": "Logged to farm database."}
        with DB_LOCK:
            conn = get_db()
            if "@" in recipient:
                email_status = send_real_smtp_email(
                    conn,
                    recipient,
                    f"AquaSense Farm Report ({STATE['selected_crop']})",
                    data.alert_message
                )
            summary = f"{data.channel} Report -> {recipient}: {data.alert_message[:48]}..."
            block_hash = add_audit_and_block(conn, summary, 0.0, 0, f"Alert Gateway ({role})")
            conn.commit()
            conn.close()

        return {
            "status": "sms_whatsapp_dispatched",
            "gateway": STATE["sms_gateway"]["provider"],
            "channel": data.channel,
            "recipient": recipient,
            "message": data.alert_message,
            "dispatched_count": STATE["sms_gateway"]["dispatched_count"],
            "email_delivery": email_status,
            "block_hash": block_hash,
            "timestamp": ts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/drone/scan")
def api_drone_scan(role: str = Depends(verify_rbac)):
    try:
        generate_thermal_grid()
        STATE["drone"]["status"] = "SCAN COMPLETE"
        STATE["drone"]["altitude_m"] = 24.5
        STATE["drone"]["battery_pct"] = max(15, STATE["drone"]["battery_pct"] - random.randint(2, 5))
        STATE["drone"]["last_scan"] = utcnow()

        anomalies = STATE["drone"]["anomalies_found"]
        summary = f"Drone Farm Scan: {anomalies} dry/hot spots found across 12 sections"

        with DB_LOCK:
            conn = get_db()
            add_audit_and_block(conn, summary, 0.0, 0, f"Drone Scout ({role})")
            conn.commit()
            conn.close()

        return {
            "status": "success",
            "drone": STATE["drone"],
            "message": summary
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/voice/command")
def api_voice_command(data: VoiceCommandRequest, role: str = Depends(verify_rbac)):
    cmd = data.transcript.lower().strip()
    reply = ""

    if role == "Auditor" and any(k in cmd for k in ["emergency", "kill", "stop", "reset", "resume", "on", "start", "irrigate"]):
        raise HTTPException(
            status_code=403,
            detail="Viewer Mode Lock: Switch role to 'Farmer' to use voice pump commands."
        )

    with DB_LOCK:
        conn = get_db()

        if "emergency" in cmd or "kill" in cmd or "stop all" in cmd:
            STATE["emergency_stop"] = True
            STATE["active_action"] = "ALL PUMPS STOPPED (EMERGENCY)"
            for z in ZONES:
                STATE["pump_states"][z] = "FORCE_OFF"
            add_audit_and_block(conn, "Voice Command: Stopped All Water Pumps", 0.0, 0, f"AquaVoice ({role})")
            reply = "Stopping all water pumps immediately."

        elif "reset" in cmd or "resume" in cmd or "auto" in cmd:
            STATE["emergency_stop"] = False
            STATE["active_action"] = "Smart Auto Watering Active"
            for z in ZONES:
                STATE["pump_states"][z] = "AUTO"
            add_audit_and_block(conn, "Voice Command: Returned to Smart Auto Mode", 0.0, 0, f"AquaVoice ({role})")
            reply = "All fields are now set to smart automatic watering."

        elif "zone a" in cmd or "field a" in cmd:
            STATE["pump_states"]["Zone_A_Laterite"] = "FORCE_ON"
            add_audit_and_block(conn, "Voice Command: Turned Water ON for Field A", 105.0, 10, f"AquaVoice ({role})")
            reply = "Starting water pump for Field A now for 10 minutes."

        elif "zone b" in cmd or "field b" in cmd:
            STATE["pump_states"]["Zone_B_Alluvial"] = "FORCE_ON"
            add_audit_and_block(conn, "Voice Command: Turned Water ON for Field B", 105.0, 10, f"AquaVoice ({role})")
            reply = "Starting water pump for Field B now for 10 minutes."

        elif "drone" in cmd or "scan" in cmd or "thermal" in cmd:
            generate_thermal_grid()
            STATE["drone"]["status"] = "SCAN COMPLETE"
            STATE["drone"]["last_scan"] = utcnow()
            add_audit_and_block(conn, "Voice Command: Scanned Farm with Drone", 0.0, 0, f"AquaVoice ({role})")
            reply = f"Drone scan finished. Found {STATE['drone']['anomalies_found']} sections needing water."

        else:
            avg_m = round((LIVE["Zone_A_Laterite"]["moisture"] + LIVE["Zone_B_Alluvial"]["moisture"]) / 2, 1)
            avg_t = round((LIVE["Zone_A_Laterite"]["temp"] + LIVE["Zone_B_Alluvial"]["temp"]) / 2, 1)
            reply = (
                f"Your farm is healthy. Soil water level is {avg_m} percent, "
                f"air temperature is {avg_t} degrees Celsius, and {STATE['active_action']}."
            )

        conn.commit()
        conn.close()

    return {
        "status": "success",
        "transcript": data.transcript,
        "reply": reply,
        "action_taken": STATE["active_action"]
    }


@app.post("/api/region/configure")
def api_region_configure(data: RegionCropRequest, role: str = Depends(verify_rbac)):
    require_write_permission(role, "Save Crop & Location")
    try:
        STATE["selected_region"] = data.region
        STATE["selected_crop"] = data.crop_type

        avg_m = (LIVE["Zone_A_Laterite"]["moisture"] + LIVE["Zone_B_Alluvial"]["moisture"]) / 2.0
        avg_t = (LIVE["Zone_A_Laterite"]["temp"] + LIVE["Zone_B_Alluvial"]["temp"]) / 2.0
        avg_h = (LIVE["Zone_A_Laterite"]["humidity"] + LIVE["Zone_B_Alluvial"]["humidity"]) / 2.0

        with DB_LOCK:
            conn = get_db()
            set_config(conn, "active_region", data.region)
            set_config(conn, "active_crop", data.crop_type)
            if STATE["active_user"]:
                conn.execute(
                    "UPDATE users SET saved_region = ?, saved_crop = ? WHERE LOWER(contact_id) = LOWER(?)",
                    (data.region, data.crop_type, STATE["active_user"]["contact_id"])
                )
            yield_fc = compute_and_store_yield_forecast(conn, data.crop_type, avg_m, avg_t, avg_h)
            add_audit_and_block(
                conn,
                f"Saved Crop Profile: {data.crop_type} in {data.region} (Expected Harvest: {yield_fc['projected_yield_tons']}t)",
                0.0,
                0,
                f"Farmer ({role})"
            )
            conn.commit()
            conn.close()

        return {
            "status": "success",
            "selected_region": data.region,
            "selected_crop": data.crop_type,
            "yield_forecast": yield_fc,
            "message": "Saved your crop and farm location!"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/weather/sync")
def api_weather_sync(role: str = Depends(verify_rbac)):
    try:
        url = "https://api.open-meteo.com/v1/forecast?latitude=13.3409&longitude=74.7421&current=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&timezone=auto"
        req = urllib.request.Request(url, headers={"User-Agent": "AquaSensePro-IoT/4.0"})

        with urllib.request.urlopen(req, timeout=4) as response:
            payload = json.loads(response.read().decode())
            current = payload.get("current", {})

            ext_temp = float(current.get("temperature_2m", 30.5))
            ext_hum = float(current.get("relative_humidity_2m", 72.0))
            ext_wind = float(current.get("wind_speed_10m", 2.4))
            ext_precip = float(current.get("precipitation", 0.0))

            rain_prob_calc = min(100.0, max(5.0, ext_precip * 45.0 + (100.0 - ext_hum) * 0.2))

            STATE["external_weather"] = {
                "source": "Open-Meteo Satellite Weather (Udyavar / Udupi)",
                "temp": ext_temp,
                "humidity": ext_hum,
                "wind_speed": ext_wind,
                "rain_prob": round(rain_prob_calc, 1),
                "last_synced": utcnow()
            }
            STATE["rain_probability"] = round(rain_prob_calc, 1)

            with DB_LOCK:
                conn = get_db()
                add_audit_and_block(conn, f"Weather Updated: {ext_temp}°C, {ext_hum}% Humidity (Udupi)", 0.0, 0, f"Weather Satellite ({role})")
                conn.commit()
                conn.close()

            return {
                "status": "success",
                "message": "Updated live Udupi weather",
                "weather": STATE["external_weather"]
            }

    except Exception as e:
        fallback_temp = round(random.uniform(28.5, 33.5), 1)
        fallback_hum = round(random.uniform(68.0, 84.0), 1)
        fallback_wind = round(random.uniform(1.8, 4.2), 1)

        STATE["external_weather"] = {
            "source": "Local Udyavar Weather Station",
            "temp": fallback_temp,
            "humidity": fallback_hum,
            "wind_speed": fallback_wind,
            "rain_prob": 28.0,
            "last_synced": utcnow()
        }

        return {
            "status": "fallback",
            "message": f"Used local weather station ({str(e)})",
            "weather": STATE["external_weather"]
        }


@app.post("/api/irrigation/override")
def api_irrigation_override(data: OverrideRequest, role: str = Depends(verify_rbac)):
    require_write_permission(role, "Water Pump Control")
    try:
        action = data.action.upper().strip()
        if action not in ("FORCE_ON", "FORCE_OFF", "AUTO"):
            raise HTTPException(status_code=400, detail="Action must be FORCE_ON, FORCE_OFF, or AUTO")

        if STATE["emergency_stop"] and action == "FORCE_ON":
            raise HTTPException(status_code=400, detail="Emergency stop is active. Click 'Resume Normal Watering' at the top right first.")

        with DB_LOCK:
            conn = get_db()
            flow_rate = float(get_config(conn, "pump_flow_lpm", "10.5"))

            water_volume = 0.0
            duration = data.duration_mins
            friendly_zone = "Field A" if "Zone_A" in data.zone_id else "Field B"

            if action == "FORCE_ON":
                water_volume = round(duration * flow_rate, 2)
                pump_action = f"{friendly_zone}: Water Pump Turned ON Now ({duration} mins)"
            elif action == "FORCE_OFF":
                duration = 0
                pump_action = f"{friendly_zone}: Water Pump Stopped"
            else:
                duration = 0
                pump_action = f"{friendly_zone}: Set to Smart Automatic Watering"

            add_audit_and_block(conn, pump_action, water_volume, duration, f"Farmer ({role})")
            conn.commit()
            conn.close()

        STATE["pump_states"][data.zone_id] = action

        return {
            "status": "success",
            "zone_id": data.zone_id,
            "action": action,
            "duration_mins": duration,
            "water_volume_liters": water_volume,
            "emergency_stop": STATE["emergency_stop"],
            "role": role
        }

    except HTTPException as http_error:
        raise http_error
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/api/analytics/historical")
def api_analytics_historical(range: str = Query(default="24h")):
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        if range == "24h":
            since = now - timedelta(hours=24)
            bucket = "%Y-%m-%d %H:00"
        elif range == "7d":
            since = now - timedelta(days=7)
            bucket = "%Y-%m-%d"
        elif range == "30d":
            since = now - timedelta(days=30)
            bucket = "%Y-%m-%d"
        else:
            raise HTTPException(status_code=400, detail="range must be 24h, 7d, or 30d")

        since_str = since.strftime("%Y-%m-%d %H:%M:%S")

        with DB_LOCK:
            conn = get_db()
            rows = conn.execute(
                f"""
                SELECT
                    strftime('{bucket}', timestamp) AS bucket,
                    ROUND(AVG(soil_moisture_vwc), 2) AS moisture,
                    ROUND(AVG(ambient_temp_c), 2) AS temp,
                    ROUND(AVG(humidity_pct), 2) AS humidity
                FROM sensors
                WHERE timestamp >= ?
                GROUP BY bucket
                ORDER BY bucket
                """,
                (since_str,)
            ).fetchall()
            conn.close()

        return {
            "range": range,
            "labels": [row["bucket"] for row in rows],
            "moisture": [row["moisture"] for row in rows],
            "temp": [row["temp"] for row in rows],
            "humidity": [row["humidity"] for row in rows]
        }

    except HTTPException as http_error:
        raise http_error
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/api/simulate")
def api_simulate(data: SimulationRequest, role: str = Depends(verify_rbac)):
    try:
        with DB_LOCK:
            conn = get_db()
            crit_threshold = float(get_config(conn, "crit_moisture_threshold", "30.0"))
            rain_threshold = float(get_config(conn, "rain_override_threshold", "75.0"))
            maintenance_minutes = int(float(get_config(conn, "maintenance_pump_minutes", "10")))
            flow_rate = float(get_config(conn, "pump_flow_lpm", "10.5"))
            conn.close()

        evapo_stress = compute_evapo_stress(data.temperature, data.humidity)
        et0 = compute_et0(data.temperature, data.humidity, data.wind_speed, data.solar_radiation)
        kc = compute_kc(data.base_moisture, data.crop_stage)

        crop_multipliers = {
            "Paddy": 1.3,
            "Arecanut": 1.1,
            "Pepper": 0.8,
            "Sugarcane": 1.4,
            "Vegetables (General)": 1.0
        }
        multiplier = crop_multipliers.get(data.crop_type, 1.0)
        etc = round(et0 * kc * multiplier, 3)

        projected_moisture = max(5.0, data.base_moisture - (evapo_stress * 4.2 * multiplier))
        moisture_loss = round(data.base_moisture - projected_moisture, 2)

        if projected_moisture < crit_threshold:
            pump_minutes = int((crit_threshold - projected_moisture) * 3.8 * multiplier)
            action = f"Soil Will Get Too Dry ({data.crop_type}) - Water Immediately"
            severity = "critical"
            npk_advice = "Add Nitrogen + Potassium Fertilizer Booster"
        elif projected_moisture > rain_threshold:
            pump_minutes = 0
            action = f"Soil Has Plenty of Water ({data.region}) - Keep Pumps OFF"
            severity = "safe"
            npk_advice = "Do Not Add Fertilizer Today (Soil is Very Wet)"
        else:
            pump_minutes = int(maintenance_minutes * multiplier)
            action = f"Normal Light Watering ({data.crop_type})"
            severity = "normal"
            npk_advice = "Use Balanced NPK Fertilizer Mix"

        water_volume = round(pump_minutes * flow_rate, 2)

        with DB_LOCK:
            conn = get_db()
            yield_fc = compute_and_store_yield_forecast(
                conn,
                data.crop_type,
                projected_moisture,
                data.temperature,
                data.humidity,
                data.wind_speed,
                data.solar_radiation
            )
            add_audit_and_block(
                conn,
                f"Weather Calculator [{data.crop_type}]: {action} | Expected Harvest: {yield_fc['projected_yield_tons']}t",
                water_volume,
                pump_minutes,
                f"Calculator ({role})"
            )
            conn.commit()
            conn.close()

        return {
            "status": "success",
            "inputs": data.model_dump(),
            "blueprint_model": {
                "evapo_stress_index": evapo_stress,
                "projected_soil_moisture": round(projected_moisture, 2),
                "moisture_loss_pct": moisture_loss
            },
            "faO56": {
                "et0": et0,
                "kc": kc,
                "etc": etc
            },
            "prescription": {
                "pump_minutes": pump_minutes,
                "water_volume_liters": water_volume,
                "action": action,
                "severity": severity,
                "npk_advice": npk_advice
            },
            "yield_forecast": yield_fc
        }

    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/api/export/csv")
def api_export_csv():
    try:
        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow([
            "record_type", "timestamp", "zone_id", "soil_moisture_vwc",
            "ambient_temp_c", "humidity_pct", "electrical_conductivity_ec",
            "pump_action", "water_volume_liters", "duration_minutes", "triggered_by", "sha256_hash"
        ])

        with DB_LOCK:
            conn = get_db()
            sensor_rows = conn.execute("SELECT * FROM sensors ORDER BY id DESC LIMIT 500").fetchall()
            block_rows = conn.execute("SELECT * FROM blockchain_ledger ORDER BY block_id DESC").fetchall()
            conn.close()

        for row in block_rows:
            writer.writerow([
                "blockchain_audit", row["timestamp"], "", "", "", "", "",
                row["event_summary"], row["water_liters"], "", row["triggered_by"], row["block_hash"]
            ])

        for row in sensor_rows:
            writer.writerow([
                "sensor", row["timestamp"], row["zone_id"], row["soil_moisture_vwc"],
                row["ambient_temp_c"], row["humidity_pct"], row["electrical_conductivity_ec"], "", "", "", "", ""
            ])

        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=aquasense_pro_farm_report.csv"}
        )

    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/api/fertigation/configure")
def api_fertigation_configure(data: FertigationRequest, role: str = Depends(verify_rbac)):
    require_write_permission(role, "Add Liquid Fertilizer")
    try:
        leaching_risk = data.leaching_risk_flag or (data.nitrogen_ppm + data.potassium_ppm > 450)
        payload = {
            "nitrogen_ppm": data.nitrogen_ppm,
            "phosphorus_ppm": data.phosphorus_ppm,
            "potassium_ppm": data.potassium_ppm,
            "leaching_risk_flag": leaching_risk
        }

        with DB_LOCK:
            conn = get_db()
            conn.execute(
                """
                INSERT INTO fertigation_records
                (timestamp, growth_stage, nitrogen_ppm, phosphorus_ppm, potassium_ppm, leaching_risk_flag)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (utcnow(), data.growth_stage, data.nitrogen_ppm, data.phosphorus_ppm, data.potassium_ppm, 1 if leaching_risk else 0)
            )
            set_config(conn, f"fertigation:{data.growth_stage.lower()}", json.dumps(payload))
            add_audit_and_block(
                conn,
                f"Added Fertilizer ({data.growth_stage}): N={data.nitrogen_ppm} P={data.phosphorus_ppm} K={data.potassium_ppm}",
                0.0,
                0,
                f"Fertilizer Mixer ({role})"
            )
            conn.commit()
            conn.close()

        return {
            "status": "success",
            "growth_stage": data.growth_stage,
            "npk": payload,
            "leaching_risk_flag": leaching_risk
        }

    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/api/emergency")
def api_emergency(data: EmergencyRequest, role: str = Depends(verify_rbac)):
    require_write_permission(role, "Emergency Stop All Pumps")
    try:
        with DB_LOCK:
            conn = get_db()

            if data.engage:
                STATE["emergency_stop"] = True
                STATE["active_action"] = "ALL PUMPS STOPPED (EMERGENCY)"
                for zone in ZONES:
                    STATE["pump_states"][zone] = "FORCE_OFF"
                add_audit_and_block(conn, "EMERGENCY STOP: All Water Pumps Turned OFF", 0.0, 0, f"Safety Operator ({role})")
            else:
                STATE["emergency_stop"] = False
                STATE["active_action"] = "Normal Smart Watering Resumed"
                for zone in ZONES:
                    STATE["pump_states"][zone] = "AUTO"
                add_audit_and_block(conn, "Resumed Normal Smart Automatic Watering", 0.0, 0, f"Safety Operator ({role})")

            conn.commit()
            conn.close()

        return {
            "status": "success",
            "emergency_stop": STATE["emergency_stop"],
            "active_action": STATE["active_action"]
        }

    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


# ============================================================
# FRONTEND: ULTRA-EASY FARMER EDITION + TECH TOGGLE + VOICE + SMTP
# ============================================================

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AquaSense Pro v4.0 // Easy Smart Farm & Water Helper</title>

<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&family=Inter:wght@400;600;700;900&family=JetBrains+Mono:wght@500;700;800&display=swap" rel="stylesheet">

<style>
    :root {
        --neon-cyan: #00F0FF;
        --neon-pink: #FF007F;
        --neon-green: #39FF14;
        --neon-yellow: #FFE600;
        --neon-purple: #BC13FE;
    }

    * { box-sizing: border-box; }

    body {
        margin: 0;
        background-color: #030408;
        color: #E2F1FF;
        font-family: 'Space Grotesk', 'Inter', system-ui, sans-serif;
        overflow-x: hidden;
        transition: background-color 0.4s ease, color 0.4s ease;
    }

    .scanline-overlay {
        position: fixed;
        inset: 0;
        pointer-events: none;
        z-index: 9990;
        background: repeating-linear-gradient(
            to bottom,
            transparent 0px,
            transparent 2px,
            rgba(0, 240, 255, 0.018) 3px,
            rgba(0, 0, 0, 0.04) 4px
        );
    }
    body.field-mode .scanline-overlay { display: none; }

    /* OUTDOOR HIGH-CONTRAST DAYLIGHT FIELD MODE */
    body.field-mode {
        background-color: #F1F5F9 !important;
        color: #0F172A !important;
    }
    body.field-mode .ambient-orb { opacity: 0.10 !important; }
    body.field-mode .holo-card {
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.98) 0%, rgba(241, 245, 249, 0.94) 100%) !important;
        border: 2px solid #94A3B8 !important;
        box-shadow: 0 12px 30px rgba(15, 23, 42, 0.1) !important;
        color: #0F172A !important;
    }
    body.field-mode .neon-text-cyan { color: #0369A1 !important; text-shadow: none !important; }
    body.field-mode .neon-text-pink { color: #BE185D !important; text-shadow: none !important; }
    body.field-mode .neon-text-green { color: #15803D !important; text-shadow: none !important; }
    body.field-mode .neon-text-yellow { color: #B45309 !important; text-shadow: none !important; }
    body.field-mode .text-slate-400,
    body.field-mode .text-slate-300,
    body.field-mode .text-slate-200 { color: #334155 !important; }
    body.field-mode .bg-black\/75,
    body.field-mode .bg-black\/70,
    body.field-mode .bg-black\/60 {
        background-color: #E2E8F0 !important;
        color: #0F172A !important;
        border-color: #94A3B8 !important;
    }
    body.field-mode .cyber-input,
    body.field-mode .cyber-select {
        background: #FFFFFF !important;
        color: #0F172A !important;
        border: 1.5px solid #64748B !important;
        font-weight: 600;
    }
    body.field-mode .mac-tabs-dock {
        background: #FFFFFF !important;
        border: 1.5px solid #94A3B8 !important;
    }
    body.field-mode .mac-tab-btn { color: #334155 !important; }

    /* Ambient Orbs */
    .ambient-orb {
        position: fixed;
        border-radius: 50%;
        filter: blur(110px);
        opacity: 0.35;
        pointer-events: none;
        z-index: 0;
    }
    .orb-1 { width: 550px; height: 550px; top: -150px; left: -100px; background: radial-gradient(circle, #00F0FF 0%, #0044ff 70%, transparent 100%); }
    .orb-2 { width: 650px; height: 650px; top: 30%; right: -200px; background: radial-gradient(circle, #FF007F 0%, #7900ff 70%, transparent 100%); }
    .orb-3 { width: 500px; height: 500px; bottom: -150px; left: 30%; background: radial-gradient(circle, #39FF14 0%, #00F0FF 70%, transparent 100%); }

    /* SPACIOUS 3D LIQUID GLASS CARDS */
    .holo-card {
        position: relative;
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.08) 0%, rgba(8, 14, 28, 0.75) 55%, rgba(5, 8, 18, 0.90) 100%);
        border: 1.5px solid rgba(0, 240, 255, 0.28);
        border-top-color: rgba(255, 255, 255, 0.35);
        border-radius: 24px;
        padding: 24px;
        backdrop-filter: blur(30px) saturate(200%);
        box-shadow: 0 20px 50px -15px rgba(0, 0, 0, 0.85), 0 0 20px rgba(0, 240, 255, 0.1);
        z-index: 3;
    }

    /* Separate Page Navigation Dock */
    .mac-tabs-dock {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        padding: 8px;
        border-radius: 20px;
        background: rgba(10, 16, 32, 0.88);
        border: 1.5px solid rgba(0, 240, 255, 0.32);
        backdrop-filter: blur(25px);
        width: 100%;
    }
    .mac-tab-btn {
        flex: 1 1 auto;
        min-width: 155px;
        padding: 14px 18px;
        border-radius: 14px;
        font-size: 14px;
        font-weight: 800;
        color: #cbd5e1;
        transition: all 0.2s ease;
        cursor: pointer;
        border: 1px solid transparent;
        text-align: center;
    }
    .mac-tab-btn:hover {
        color: #fff;
        background: rgba(0, 240, 255, 0.15);
        border-color: rgba(0, 240, 255, 0.4);
    }
    .mac-tab-btn.active {
        color: #030408;
        background: linear-gradient(135deg, #00F0FF, #39FF14);
        box-shadow: 0 0 25px rgba(0, 240, 255, 0.55);
        font-weight: 900;
    }

    .page-view { display: none; }
    .page-view.active {
        display: block;
        animation: pageFadeIn 0.25s ease-out;
    }
    @keyframes pageFadeIn {
        from { opacity: 0; transform: translateY(8px); }
        to { opacity: 1; transform: translateY(0); }
    }

    .mono { font-family: 'JetBrains Mono', monospace; }
    .neon-text-cyan { color: #00F0FF; text-shadow: 0 0 12px rgba(0, 240, 255, 0.6); }
    .neon-text-pink { color: #FF007F; text-shadow: 0 0 12px rgba(255, 0, 127, 0.6); }
    .neon-text-green { color: #39FF14; text-shadow: 0 0 12px rgba(57, 255, 20, 0.6); }
    .neon-text-yellow { color: #FFE600; text-shadow: 0 0 12px rgba(255, 230, 0, 0.6); }

    /* Big User-Friendly Buttons */
    .btn-cyber {
        border: 1px solid rgba(255, 255, 255, 0.3);
        border-radius: 14px;
        padding: 13px 20px;
        font-weight: 800;
        cursor: pointer;
        transition: all 0.2s ease;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
    }
    .btn-cyber:hover {
        transform: translateY(-2px);
        box-shadow: 0 10px 25px -5px rgba(0, 240, 255, 0.55);
    }
    .btn-emerald { background: linear-gradient(135deg, #00F0FF, #39FF14); color: #030408; }
    .btn-cyan { background: linear-gradient(135deg, #00B4D8, #00F0FF); color: #030408; }
    .btn-purple { background: linear-gradient(135deg, #FF007F, #BC13FE); color: #fff; }
    .btn-danger { background: linear-gradient(135deg, #FF0055, #FF007F); color: #fff; }

    .rbac-locked {
        opacity: 0.42 !important;
        filter: grayscale(0.85) !important;
        cursor: not-allowed !important;
    }

    /* Tech Details Hidden by Default to Reduce Cognitive Overload */
    .tech-detail-box {
        display: none;
    }
    body.show-tech-details .tech-detail-box {
        display: block;
        animation: pageFadeIn 0.2s ease-out;
    }

    /* 12-Section Farm Map */
    .iso-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 14px;
    }
    .iso-node {
        padding: 16px;
        border-radius: 16px;
        background: rgba(8, 14, 30, 0.9);
        border: 1.5px solid rgba(0, 240, 255, 0.35);
        cursor: pointer;
        transition: all 0.2s ease;
    }
    .iso-node:hover {
        transform: translateY(-4px);
        border-color: #00F0FF;
        box-shadow: 0 12px 25px rgba(0, 240, 255, 0.35);
    }
    .iso-node.stress {
        border-color: #FF007F;
        background: linear-gradient(135deg, rgba(255, 0, 127, 0.3), rgba(8, 14, 30, 0.92));
    }

    .cyber-input, .cyber-select {
        width: 100%;
        background: rgba(4, 7, 18, 0.9);
        color: #E2F1FF;
        border: 1.5px solid rgba(0, 240, 255, 0.4);
        border-radius: 12px;
        padding: 12px 14px;
        font-size: 14px;
        outline: none;
    }

    #toastContainer {
        position: fixed;
        bottom: 24px;
        right: 24px;
        z-index: 10050;
        display: flex;
        flex-direction: column;
        gap: 10px;
        max-width: 390px;
        width: calc(100vw - 48px);
        pointer-events: none;
    }
    .cyber-toast {
        pointer-events: auto;
        padding: 14px 16px;
        border-radius: 16px;
        background: rgba(8, 13, 26, 0.95);
        backdrop-filter: blur(20px);
        border: 1.5px solid #00F0FF;
        color: #E2F1FF;
        box-shadow: 0 15px 35px rgba(0, 0, 0, 0.85);
    }

    .cyber-modal-backdrop {
        position: fixed;
        inset: 0;
        z-index: 10020;
        background: rgba(3, 4, 8, 0.84);
        backdrop-filter: blur(12px);
        display: none;
        align-items: center;
        justify-content: center;
        padding: 16px;
    }
    .cyber-modal-backdrop.open { display: flex; }
    .cyber-modal-box {
        width: 100%;
        max-width: 540px;
        border-radius: 24px;
        background: linear-gradient(135deg, rgba(16, 24, 48, 0.98), rgba(6, 10, 22, 0.99));
        border: 2px solid #00F0FF;
        box-shadow: 0 25px 60px rgba(0, 0, 0, 0.9);
        padding: 28px;
        max-height: 92vh;
        overflow-y: auto;
    }
</style>
</head>

<body class="pb-16">

<div class="scanline-overlay"></div>
<div class="ambient-orb orb-1"></div>
<div class="ambient-orb orb-2"></div>
<div class="ambient-orb orb-3"></div>

<!-- 🔔 Toast Notification Container -->
<div id="toastContainer"></div>

<!-- ========================================================== -->
<!-- 👤 FARMER SIGN-IN & REAL GMAIL SMTP SETUP MODAL            -->
<!-- ========================================================== -->
<div id="signInModal" class="cyber-modal-backdrop" onclick="if(event.target===this) closeModals()">
    <div class="cyber-modal-box space-y-5">
        <div class="flex items-center justify-between border-b border-slate-700 pb-3">
            <div class="flex items-center gap-2.5">
                <span class="text-2xl">👤</span>
                <div>
                    <h3 class="text-lg font-black neon-text-cyan" data-i18n="signin_title">Sign In to Save Your Farm Info</h3>
                    <p class="text-xs text-slate-400" data-i18n="signin_subtitle">Saves your crop, language & sends farm alerts to your Email or Phone</p>
                </div>
            </div>
            <button onclick="closeModals()" class="text-xs mono px-3 py-1.5 rounded-lg bg-slate-800 text-slate-300 hover:bg-slate-700">✕</button>
        </div>

        <div class="space-y-4">
            <div>
                <label class="text-xs font-bold uppercase text-slate-300 block mb-1" data-i18n="lbl_farmer_name">Your Name</label>
                <input id="loginNameInput" type="text" class="cyber-input" placeholder="e.g. Mazeem / Ramesh">
            </div>
            <div>
                <label class="text-xs font-bold uppercase text-[#00F0FF] block mb-1" data-i18n="lbl_contact_id">Your Email Address OR Phone Number *</label>
                <input id="loginContactInput" type="text" class="cyber-input" placeholder="e.g. farmer@gmail.com or +91-9876543210">
            </div>

            <!-- REAL GMAIL SMTP CREDENTIALS BOX (OPTIONAL FOR REAL INBOX DELIVERY) -->
            <details class="p-3.5 rounded-2xl bg-black/60 border border-[#00F0FF]/30 text-xs space-y-3">
                <summary class="cursor-pointer font-bold text-[#00F0FF]">
                    📧 Optional: Setup Real Gmail Inbox Delivery (SMTP)
                </summary>
                <p class="text-slate-400 mt-2 leading-relaxed">
                    To receive <b>real emails in your Gmail inbox</b> when you click "Send Report", enter the sender Gmail address and a 16-letter <a href="https://myaccount.google.com/apppasswords" target="_blank" class="text-[#39FF14] underline">Google App Password</a>:
                </p>
                <div class="space-y-2 mt-2">
                    <input id="smtpSenderInput" type="email" class="cyber-input text-xs" placeholder="Sender Gmail (e.g. yourname@gmail.com)">
                    <input id="smtpPassInput" type="password" class="cyber-input text-xs" placeholder="16-letter Google App Password (abcd efgh ijkl mnop)">
                </div>
            </details>
        </div>

        <div class="flex gap-3 pt-1">
            <button onclick="submitFarmerLogin()" class="btn-cyber btn-emerald w-full text-base py-3" data-i18n="btn_save_login">
                ✅ Sign In & Save My Settings
            </button>
        </div>

        <div id="signedInProfileBox" class="hidden p-4 rounded-2xl bg-black/60 border border-[#39FF14]/40 text-xs space-y-2">
            <div class="font-bold text-[#39FF14] text-sm" id="modalUserSummary">Signed in!</div>
            <div class="flex gap-2 pt-1">
                <button onclick="syncUserPreferencesToCloud()" class="btn-cyber btn-cyan text-xs flex-1 py-2">💾 Save Current Settings</button>
                <button onclick="logoutFarmer()" class="btn-cyber btn-danger text-xs py-2 px-4">Sign Out</button>
            </div>
        </div>
    </div>
</div>

<!-- MAIN CONTAINER -->
<div class="relative z-10 max-w-7xl mx-auto px-4 py-4 space-y-6">

    <!-- ========================================================== -->
    <!-- TOP HEADER BAR                                             -->
    <!-- ========================================================== -->
    <header class="holo-card flex flex-wrap items-center justify-between gap-4">
        <div class="flex items-center gap-3.5">
            <div class="w-12 h-12 rounded-2xl bg-gradient-to-br from-[#00F0FF] via-[#BC13FE] to-[#FF007F] flex items-center justify-center text-2xl shadow-[0_0_25px_rgba(0,240,255,0.6)]">
                💧
            </div>
            <div>
                <div class="flex items-center gap-2.5">
                    <h1 class="text-xl md:text-2xl font-black tracking-wide neon-text-cyan" data-i18n="app_title">
                        AQUASENSE PRO
                    </h1>
                    <span class="text-[11px] mono font-bold uppercase px-2.5 py-0.5 rounded-full bg-[#39FF14]/15 text-[#39FF14] border border-[#39FF14]/40" id="connectionStatus">
                        ● LIVE
                    </span>
                </div>
                <p class="text-xs text-slate-300" data-i18n="app_subtitle">Easy Smart Farm & Water Helper · Udupi</p>
            </div>
        </div>

        <!-- TOP CONTROLS -->
        <div class="flex flex-wrap items-center gap-2">
            <!-- 🌐 LANGUAGE SWITCHER -->
            <div class="flex items-center bg-black/75 p-1 rounded-xl border border-[#00F0FF]/40">
                <button onclick="setLanguage('en')" id="lang-en" class="px-3 py-1.5 rounded-lg text-xs font-extrabold bg-[#00F0FF] text-slate-950">English</button>
                <button onclick="setLanguage('kn')" id="lang-kn" class="px-3 py-1.5 rounded-lg text-xs font-bold text-slate-300 hover:text-white">ಕನ್ನಡ</button>
                <button onclick="setLanguage('hi')" id="lang-hi" class="px-3 py-1.5 rounded-lg text-xs font-bold text-slate-300 hover:text-white">हिंदी</button>
            </div>

            <!-- 🗣️ SPOKEN VOICE PROMPTS ON/OFF -->
            <button onclick="toggleSpokenPrompts()" id="voicePromptsBtn" class="px-3 py-2 rounded-xl bg-black/70 border border-[#39FF14]/50 text-xs font-bold text-[#39FF14] hover:bg-slate-900 transition">
                🗣️ <span id="voicePromptsLabel">Voice Alerts: ON</span>
            </button>

            <!-- 👤 SIGN IN BUTTON -->
            <button onclick="openSignInModal()" id="signInHeaderBtn" class="btn-cyber btn-cyan text-xs py-2.5 px-4">
                👤 <span id="signInBtnText" data-i18n="btn_signin">Sign In (Email / Phone)</span>
            </button>

            <!-- ☀️ DAYLIGHT MODE -->
            <button onclick="toggleFieldMode()" id="fieldModeBtn" class="px-3 py-2 rounded-xl bg-black/70 border border-[#FFE600]/50 text-xs font-bold text-[#FFE600] hover:bg-slate-900 transition">
                ☀️ <span data-i18n="btn_field_mode">Daylight Mode</span>
            </button>

            <!-- 🔐 ROLE SELECTOR -->
            <select id="rbacRoleSelect" onchange="onRoleChange()" class="bg-black/75 text-[#00F0FF] border border-[#00F0FF]/40 rounded-xl px-2.5 py-2 text-xs font-bold outline-none cursor-pointer">
                <option value="Operator" selected>👨‍🌾 Farmer (Full Control)</option>
                <option value="Auditor">🔍 Viewer (Read-Only)</option>
            </select>

            <!-- ⚠️ EMERGENCY STOP -->
            <button onclick="emergencyToggle()" id="emergencyBtn" class="btn-cyber btn-danger rbac-write-btn text-xs py-2.5 px-4" data-original-label="⚠️ STOP ALL PUMPS">
                ⚠️ <span data-i18n="btn_emergency">STOP ALL PUMPS</span>
            </button>
        </div>
    </header>

    <!-- ========================================================== -->
    <!-- 5 SEPARATE, EASY-TO-USE PAGE TABS                          -->
    <!-- ========================================================== -->
    <nav class="mac-tabs-dock">
        <button onclick="switchPage('home', this)" id="tab-home" class="mac-tab-btn active">
            🏠 <span data-i18n="nav_home">1. Home Summary</span>
        </button>
        <button onclick="switchPage('pumps', this)" id="tab-pumps" class="mac-tab-btn">
            🚰 <span data-i18n="nav_pumps">2. Water Pumps & Fertilizer</span>
        </button>
        <button onclick="switchPage('drone', this)" id="tab-drone" class="mac-tab-btn">
            🚁 <span data-i18n="nav_drone">3. Field Map & Charts</span>
        </button>
        <button onclick="switchPage('weather', this)" id="tab-weather" class="mac-tab-btn">
            🌦️ <span data-i18n="nav_weather">4. Weather & Crop Profit</span>
        </button>
        <button onclick="switchPage('ledger', this)" id="tab-ledger" class="mac-tab-btn">
            💰 <span data-i18n="nav_ledger">5. Savings & History</span>
        </button>
    </nav>

    <!-- ========================================================== -->
    <!-- PAGE 1: 🏠 HOME SUMMARY (PLAIN WORDS + TECH TOGGLE)        -->
    <!-- ========================================================== -->
    <div id="page-home" class="page-view active space-y-6">

        <!-- Welcome & AI Action Banner -->
        <div class="holo-card border-l-8 border-l-[#39FF14] flex flex-col md:flex-row items-start md:items-center justify-between gap-6">
            <div class="space-y-2">
                <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-[#39FF14]/15 text-[#39FF14] text-xs font-bold mono">
                    <span>🤖 TODAY'S FARM STATUS</span>
                    <span>·</span>
                    <span id="clock">--:--:--</span>
                </div>
                <h2 class="text-2xl md:text-3xl font-black neon-text-green" id="activeAction">
                    Checking Your Farm...
                </h2>
                <p class="text-sm text-slate-300 max-w-2xl" id="aiPrescription" data-i18n="home_banner_sub">
                    Your soil wetness and weather are checked every 5 seconds automatically.
                </p>
            </div>
            <div class="flex flex-wrap gap-3 shrink-0">
                <button onclick="switchPage('pumps', document.getElementById('tab-pumps'))" class="btn-cyber btn-emerald text-sm py-3.5 px-6">
                    🚰 <span data-i18n="btn_goto_pumps">Control Water Pumps ➔</span>
                </button>
                <button onclick="dispatchSMSAlert()" class="btn-cyber btn-purple rbac-write-btn text-sm py-3.5 px-5">
                    💬 <span data-i18n="btn_send_report">Send Report to My Email/Phone</span>
                </button>
            </div>
        </div>

        <!-- Bar to Toggle Technical Numbers vs Simple Farmer Words -->
        <div class="flex flex-wrap items-center justify-between gap-3 px-1">
            <div>
                <h3 class="text-lg font-extrabold text-white" data-i18n="sec_farm_health">🌱 Simple Farm Health Check</h3>
                <p class="text-xs text-slate-400" data-i18n="sec_farm_health_sub">Easy-to-understand status cards for your farm.</p>
            </div>
            <button onclick="toggleTechDetails()" id="techToggleBtn" class="px-4 py-2 rounded-xl bg-black/75 border border-[#00F0FF]/50 text-xs font-bold text-[#00F0FF] hover:bg-[#00F0FF]/15 transition mono">
                🔬 <span id="techToggleText" data-i18n="btn_show_tech">Show Technical Numbers</span>
            </button>
        </div>

        <!-- UPGRADE 1: 6 Plain-Language Cards (With Optional Tech Details Toggle) -->
        <section class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5">
            <!-- 1. Soil Wetness -->
            <div class="holo-card border-l-4 border-l-[#39FF14]">
                <div class="flex justify-between items-center">
                    <span class="text-sm font-bold text-slate-300" data-i18n="kpi_moisture">💧 Soil Wetness</span>
                    <span class="text-2xl">🌱</span>
                </div>
                <div class="text-2xl md:text-3xl font-black neon-text-green my-2" id="plain_moisture">Just Right (Moist)</div>
                <div class="text-xs text-slate-300" id="plain_moisture_sub" data-i18n="kpi_moisture_sub">Roots have enough water</div>
                <div class="tech-detail-box mt-3 pt-2 border-t border-slate-700/80 text-xs mono text-[#00F0FF]">
                    Raw Sensor VWC: <b id="metric_moisture">--</b>% (Ideal: 50%–70%)
                </div>
            </div>

            <!-- 2. Air Warmth -->
            <div class="holo-card border-l-4 border-l-[#FFE600]">
                <div class="flex justify-between items-center">
                    <span class="text-sm font-bold text-slate-300" data-i18n="kpi_temp">🌡️ Today's Warmth</span>
                    <span class="text-2xl">☀️</span>
                </div>
                <div class="text-2xl md:text-3xl font-black neon-text-yellow my-2" id="plain_temp">Warm & Normal</div>
                <div class="text-xs text-slate-300" data-i18n="kpi_temp_sub">Good weather for crop growth</div>
                <div class="tech-detail-box mt-3 pt-2 border-t border-slate-700/80 text-xs mono text-[#FFE600]">
                    Ambient Temp: <b id="metric_temp">--</b> °C
                </div>
            </div>

            <!-- 3. Rain Forecast -->
            <div class="holo-card border-l-4 border-l-[#00F0FF]">
                <div class="flex justify-between items-center">
                    <span class="text-sm font-bold text-slate-300" data-i18n="kpi_rain">🌧️ Rain Today</span>
                    <span class="text-2xl">☔</span>
                </div>
                <div class="text-2xl md:text-3xl font-black neon-text-cyan my-2" id="plain_rain">Low Rain Chance</div>
                <div class="text-xs text-slate-300" data-i18n="kpi_rain_sub">Pumps will stop automatically if heavy rain comes</div>
                <div class="tech-detail-box mt-3 pt-2 border-t border-slate-700/80 text-xs mono text-[#00F0FF]">
                    24h Rain Probability: <b id="metric_rain">--</b>%
                </div>
            </div>

            <!-- 4. Air Moisture -->
            <div class="holo-card border-l-4 border-l-[#00F0FF]">
                <div class="flex justify-between items-center">
                    <span class="text-sm font-bold text-slate-300" data-i18n="kpi_humidity">💨 Air Moisture</span>
                    <span class="text-2xl">🌬️</span>
                </div>
                <div class="text-2xl font-black neon-text-cyan my-2" id="plain_humidity">Comfortable Air</div>
                <div class="text-xs text-slate-300" data-i18n="kpi_humidity_sub">Leaves are not drying out</div>
                <div class="tech-detail-box mt-3 pt-2 border-t border-slate-700/80 text-xs mono text-[#00F0FF]">
                    Relative Humidity: <b id="metric_humidity">--</b>%
                </div>
            </div>

            <!-- 5. Soil Health & Nutrients -->
            <div class="holo-card border-l-4 border-l-[#FF007F]">
                <div class="flex justify-between items-center">
                    <span class="text-sm font-bold text-slate-300" data-i18n="kpi_ec">🧪 Soil Health & Food</span>
                    <span class="text-2xl">⚡</span>
                </div>
                <div class="text-2xl font-black neon-text-pink my-2" id="plain_ec">Good for Roots</div>
                <div class="text-xs text-slate-300" data-i18n="kpi_ec_sub">Safe salt & fertilizer balance</div>
                <div class="tech-detail-box mt-3 pt-2 border-t border-slate-700/80 text-xs mono text-[#FF007F]">
                    Conductivity (EC): <b id="metric_ec">--</b> dS/m
                </div>
            </div>

            <!-- 6. Water Pipes Check -->
            <div class="holo-card border-l-4 border-l-[#39FF14]">
                <div class="flex justify-between items-center">
                    <span class="text-sm font-bold text-slate-300" data-i18n="kpi_flow">🚰 Water Pipes Check</span>
                    <span class="text-2xl">🔧</span>
                </div>
                <div class="text-2xl font-black neon-text-green my-2" id="plain_flow">No Leaks or Blockages</div>
                <div class="text-xs text-emerald-300 font-bold" id="metric_watchdog">Pipes Working Fine</div>
                <div class="tech-detail-box mt-3 pt-2 border-t border-slate-700/80 text-xs mono text-[#39FF14]">
                    Flow Speed: <b id="metric_flow">-- LPM</b>
                </div>
            </div>
        </section>

        <!-- Easy Voice Helper on Home Page -->
        <div class="holo-card">
            <div class="flex flex-wrap items-center justify-between gap-4 mb-3">
                <div>
                    <h3 class="text-lg font-extrabold neon-text-pink" data-i18n="voice_title">🎙️ Speak to Your Farm Helper (AquaVoice)</h3>
                    <p class="text-xs text-slate-300" data-i18n="voice_desc">Tap any button below or speak to control pumps with your voice.</p>
                </div>
                <button onclick="triggerVoiceAssistant()" class="btn-cyber btn-purple text-xs">
                    🎙️ <span data-i18n="btn_mic">Tap to Speak</span> (<span id="voiceStateBadge">READY</span>)
                </button>
            </div>

            <div class="flex flex-wrap gap-2 mb-3">
                <button onclick="quickVoice('System status report')" class="px-3.5 py-2.5 rounded-xl bg-black/70 border border-[#00F0FF]/40 text-xs font-bold text-[#00F0FF] hover:bg-[#00F0FF]/20">📊 Read Farm Status Out Loud</button>
                <button onclick="quickVoice('Force on Zone A')" class="px-3.5 py-2.5 rounded-xl bg-black/70 border border-[#39FF14]/40 text-xs font-bold text-[#39FF14] hover:bg-[#39FF14]/20">💧 Turn Water ON for Field A</button>
                <button onclick="quickVoice('Force on Zone B')" class="px-3.5 py-2.5 rounded-xl bg-black/70 border border-[#39FF14]/40 text-xs font-bold text-[#39FF14] hover:bg-[#39FF14]/20">💧 Turn Water ON for Field B</button>
                <button onclick="quickVoice('Launch drone thermal scan')" class="px-3.5 py-2.5 rounded-xl bg-black/70 border border-[#FFE600]/40 text-xs font-bold text-[#FFE600] hover:bg-[#FFE600]/20">🚁 Check Farm with Drone</button>
                <button onclick="quickVoice('Reset to auto mode')" class="px-3.5 py-2.5 rounded-xl bg-black/70 border border-[#FF007F]/40 text-xs font-bold text-[#FF007F] hover:bg-[#FF007F]/20">🤖 Let Smart Auto Handle It</button>
            </div>

            <div class="flex gap-2">
                <input id="voiceInputBox" class="cyber-input" placeholder="Or type here (e.g., 'Turn on Field A' or 'Status')...">
                <button onclick="sendVoiceText()" class="btn-cyber btn-cyan text-xs px-6">Ask</button>
            </div>
            <div id="voiceReplyBox" class="mt-3 p-3.5 rounded-2xl bg-black/75 border border-[#FF007F]/40 text-sm text-pink-200 mono">
                "Hello Farmer! Tap any button above and I will speak the answer out loud."
            </div>
        </div>
    </div>

    <!-- ========================================================== -->
    <!-- PAGE 2: 🚰 FOOLPROOF WATER PUMPS & FERTILIZER              -->
    <!-- ========================================================== -->
    <div id="page-pumps" class="page-view space-y-6">

        <!-- UPGRADE 2: Foolproof 1-Click Field Pump Cards -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <!-- Field A Card -->
            <div class="holo-card border-t-4 border-t-[#00F0FF] space-y-4">
                <div class="flex justify-between items-start gap-2">
                    <div>
                        <h3 class="text-xl font-black text-white" data-i18n="zone_a_title">🌾 Field A – Laterite Soil Plot</h3>
                        <p class="text-xs text-slate-400">Main Coastal Plot · Drip Pipe Line 1</p>
                    </div>
                    <span id="zoneA_badge" class="px-3.5 py-1.5 rounded-full text-xs font-black bg-[#00F0FF]/20 text-[#00F0FF] border border-[#00F0FF]/40">
                        🤖 SMART AUTO
                    </span>
                </div>

                <div class="p-4 rounded-2xl bg-black/70 border border-slate-800 space-y-2">
                    <div class="flex justify-between items-center">
                        <span class="text-sm font-bold text-slate-300" data-i18n="lbl_soil_water">Soil Wetness:</span>
                        <span class="text-[#39FF14] font-black text-base" id="zoneA_plain_moisture">Just Right (47.5%)</span>
                    </div>
                    <div class="w-full h-4 bg-slate-800 rounded-full overflow-hidden">
                        <div id="zoneA_bar" class="bg-gradient-to-r from-[#00F0FF] to-[#39FF14] h-full transition-all duration-500" style="width: 48%"></div>
                    </div>
                    <div class="flex justify-between text-xs text-slate-400 pt-1">
                        <span>Warmth: <b id="zoneA_temp">31.2°C</b></span>
                        <span>Air Moisture: <b id="zoneA_hum">68%</b></span>
                        <span>Soil Health: <b id="zoneA_ec">Good</b></span>
                    </div>
                </div>

                <!-- Foolproof Everyday Action Buttons -->
                <div class="grid grid-cols-1 sm:grid-cols-3 gap-2.5 pt-1">
                    <button onclick="quickPumpOverride('Zone_A_Laterite', 'FORCE_ON')" class="btn-cyber btn-emerald rbac-write-btn text-xs py-3.5">
                        ▶️ <span data-i18n="btn_pump_on">Turn Water ON Now</span>
                    </button>
                    <button onclick="quickPumpOverride('Zone_A_Laterite', 'FORCE_OFF')" class="btn-cyber btn-danger rbac-write-btn text-xs py-3.5">
                        ⏹️ <span data-i18n="btn_pump_off">Stop Water</span>
                    </button>
                    <button onclick="quickPumpOverride('Zone_A_Laterite', 'AUTO')" class="btn-cyber btn-cyan rbac-write-btn text-xs py-3.5">
                        🤖 <span data-i18n="btn_pump_auto">Let Smart Auto Handle It</span>
                    </button>
                </div>
            </div>

            <!-- Field B Card -->
            <div class="holo-card border-t-4 border-t-[#39FF14] space-y-4">
                <div class="flex justify-between items-start gap-2">
                    <div>
                        <h3 class="text-xl font-black text-white" data-i18n="zone_b_title">🌴 Field B – Alluvial River Plot</h3>
                        <p class="text-xs text-slate-400">Swarna River Belt · Drip Pipe Line 2</p>
                    </div>
                    <span id="zoneB_badge" class="px-3.5 py-1.5 rounded-full text-xs font-black bg-[#39FF14]/20 text-[#39FF14] border border-[#39FF14]/40">
                        🤖 SMART AUTO
                    </span>
                </div>

                <div class="p-4 rounded-2xl bg-black/70 border border-slate-800 space-y-2">
                    <div class="flex justify-between items-center">
                        <span class="text-sm font-bold text-slate-300" data-i18n="lbl_soil_water">Soil Wetness:</span>
                        <span class="text-[#39FF14] font-black text-base" id="zoneB_plain_moisture">Just Right (56.0%)</span>
                    </div>
                    <div class="w-full h-4 bg-slate-800 rounded-full overflow-hidden">
                        <div id="zoneB_bar" class="bg-gradient-to-r from-[#00F0FF] to-[#39FF14] h-full transition-all duration-500" style="width: 56%"></div>
                    </div>
                    <div class="flex justify-between text-xs text-slate-400 pt-1">
                        <span>Warmth: <b id="zoneB_temp">29.8°C</b></span>
                        <span>Air Moisture: <b id="zoneB_hum">72.5%</b></span>
                        <span>Soil Health: <b id="zoneB_ec">Good</b></span>
                    </div>
                </div>

                <!-- Foolproof Everyday Action Buttons -->
                <div class="grid grid-cols-1 sm:grid-cols-3 gap-2.5 pt-1">
                    <button onclick="quickPumpOverride('Zone_B_Alluvial', 'FORCE_ON')" class="btn-cyber btn-emerald rbac-write-btn text-xs py-3.5">
                        ▶️ <span data-i18n="btn_pump_on">Turn Water ON Now</span>
                    </button>
                    <button onclick="quickPumpOverride('Zone_B_Alluvial', 'FORCE_OFF')" class="btn-cyber btn-danger rbac-write-btn text-xs py-3.5">
                        ⏹️ <span data-i18n="btn_pump_off">Stop Water</span>
                    </button>
                    <button onclick="quickPumpOverride('Zone_B_Alluvial', 'AUTO')" class="btn-cyber btn-cyan rbac-write-btn text-xs py-3.5">
                        🤖 <span data-i18n="btn_pump_auto">Let Smart Auto Handle It</span>
                    </button>
                </div>
            </div>
        </div>

        <!-- Custom Timer + Fertilizer (NPK) + ESP32 Hardware Box -->
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <!-- Custom Timer & Fertilizer -->
            <div class="holo-card space-y-4">
                <h3 class="text-lg font-extrabold neon-text-cyan" data-i18n="fert_heading">🧪 Add Liquid Fertilizer (NPK) & Water Timer</h3>
                <p class="text-xs text-slate-300">Run water for a specific number of minutes or mix plant food into the drip pipes.</p>

                <div class="grid grid-cols-3 gap-2">
                    <div>
                        <label class="text-xs font-bold text-slate-300">Choose Field</label>
                        <select id="override_zone" class="cyber-select rbac-input mt-1">
                            <option value="Zone_A_Laterite">Field A</option>
                            <option value="Zone_B_Alluvial">Field B</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-xs font-bold text-slate-300">What to Do</label>
                        <select id="override_action" class="cyber-select rbac-input mt-1">
                            <option value="FORCE_ON">Turn Water ON Now</option>
                            <option value="FORCE_OFF">Stop Water</option>
                            <option value="AUTO">Let Smart Auto Handle It</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-xs font-bold text-slate-300">How Many Mins</label>
                        <input id="override_duration" type="number" value="10" min="1" max="240" class="cyber-input rbac-input mt-1">
                    </div>
                </div>
                <button onclick="sendOverride()" class="btn-cyber btn-cyan rbac-write-btn w-full text-xs">⏱️ Start Water Timer</button>
                <div id="overrideMsg" class="text-xs text-[#00F0FF] mono"></div>

                <hr class="border-slate-800">

                <div class="grid grid-cols-4 gap-2">
                    <div>
                        <label class="text-xs font-bold text-slate-300">Crop Stage</label>
                        <select id="fert_stage" class="cyber-select rbac-input mt-1">
                            <option value="Vegetative">Young Plant</option>
                            <option value="Flowering">Flowering</option>
                            <option value="Ripening">Ready to Harvest</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-xs font-bold text-slate-300">Nitrogen (N)</label>
                        <input id="fert_n" type="number" value="180" class="cyber-input rbac-input mt-1">
                    </div>
                    <div>
                        <label class="text-xs font-bold text-slate-300">Phosphorus (P)</label>
                        <input id="fert_p" type="number" value="60" class="cyber-input rbac-input mt-1">
                    </div>
                    <div>
                        <label class="text-xs font-bold text-slate-300">Potassium (K)</label>
                        <input id="fert_k" type="number" value="210" class="cyber-input rbac-input mt-1">
                    </div>
                </div>
                <button onclick="sendFertigation()" class="btn-cyber btn-emerald rbac-write-btn w-full text-xs">🧪 Mix & Send Fertilizer to Crop Roots</button>
                <div id="fertMsg" class="text-xs text-[#39FF14] mono"></div>
            </div>

            <!-- ESP32 Wireless Box & Phone/Email Alert Sender -->
            <div class="holo-card space-y-4">
                <h3 class="text-lg font-extrabold neon-text-pink">📡 Field Sensor Box & Real Email / Phone Alerts</h3>
                <p class="text-xs text-slate-300">Test your wireless field valves or send a live farm report to your saved Email or Phone.</p>

                <div class="grid grid-cols-2 gap-3">
                    <div>
                        <label class="text-xs font-bold text-slate-300">Field Sensor Box</label>
                        <select id="mqtt_node" class="cyber-select rbac-input mt-1">
                            <option value="ESP32-NODE-A1">Field A Valve Box</option>
                            <option value="ESP32-NODE-B1">Field B Valve Box</option>
                            <option value="LORA-GATEWAY-01">Main Farm Antenna</option>
                        </select>
                    </div>
                    <div>
                        <label class="text-xs font-bold text-slate-300">Action</label>
                        <select id="mqtt_payload" class="cyber-select rbac-input mt-1">
                            <option value="SOLENOID_PULSE_ON">Open Water Valve</option>
                            <option value="SOLENOID_LOCK_OFF">Close Water Valve</option>
                            <option value="CALIBRATE_CAPACITIVE_ADC">Check Sensor Health</option>
                        </select>
                    </div>
                </div>

                <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    <button onclick="publishMQTTCommand()" class="btn-cyber btn-cyan rbac-write-btn text-xs py-3">
                        📡 Test Field Valve Box
                    </button>
                    <button onclick="dispatchSMSAlert()" class="btn-cyber btn-purple rbac-write-btn text-xs py-3">
                        📧 Send Report to My Email / Phone
                    </button>
                </div>

                <div class="p-4 rounded-2xl bg-black/75 border border-[#00F0FF]/40 text-xs mono space-y-1.5">
                    <div>Wireless Box Status: <span class="text-[#39FF14]" id="mqttAckText">CONNECTED & WORKING</span></div>
                    <div>Last Signal Topic: <span class="text-[#00F0FF]" id="mqttTopicText">aquasense/esp32/boot</span></div>
                    <div>Saved Alert Contact: <span class="text-[#FF007F] font-bold" id="smsStatusText">Click 'Sign In' to set your Email/Phone</span></div>
                </div>
            </div>
        </div>
    </div>

    <!-- ========================================================== -->
    <!-- PAGE 3: 🚁 DRONE FIELD MAP & LIVE CHARTS                   -->
    <!-- ========================================================== -->
    <div id="page-drone" class="page-view space-y-6">

        <div class="holo-card space-y-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
                <div>
                    <h2 class="text-xl font-extrabold neon-text-cyan" data-i18n="drone_heading">🚁 12-Section Farm Map (Drone View)</h2>
                    <p class="text-xs text-slate-300" data-i18n="drone_desc">Green sections have good water. Pink sections are dry and need water. Tap any box to hear its status.</p>
                </div>
                <div class="flex items-center gap-3">
                    <span id="droneStatusBadge" class="text-xs mono px-3 py-1.5 rounded-full bg-[#00F0FF]/15 text-[#00F0FF] border border-[#00F0FF]/40">DRONE: READY</span>
                    <button onclick="launchDroneScan()" class="btn-cyber btn-emerald text-xs">
                        🛰️ Check Farm with Drone Now
                    </button>
                </div>
            </div>

            <div class="iso-grid pt-2" id="thermalGridContainer"></div>
        </div>

        <div class="holo-card space-y-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
                <div>
                    <h2 class="text-xl font-extrabold neon-text-pink" data-i18n="chart_heading">📊 Soil Wetness & Weather History Graph</h2>
                    <p class="text-xs text-slate-300">See how your soil water and temperature changed over the day.</p>
                </div>
                <div class="flex items-center gap-2">
                    <select id="historicalRange" class="cyber-select !w-40 text-xs mono">
                        <option value="live" selected>⚡ Live Updates</option>
                        <option value="24h">Last 24 Hours</option>
                        <option value="7d">Last 7 Days</option>
                        <option value="30d">Last 30 Days</option>
                    </select>
                    <button onclick="refreshHistorical()" class="btn-cyber btn-cyan text-xs py-2.5 px-4">Refresh</button>
                </div>
            </div>

            <div class="h-[320px] w-full p-3 rounded-2xl bg-black/75 border border-[#00F0FF]/30">
                <canvas id="telemetryChart"></canvas>
            </div>
        </div>
    </div>

    <!-- ========================================================== -->
    <!-- PAGE 4: 🌦️ WEATHER, CROP PROFIT & PEST HELPER              -->
    <!-- ========================================================== -->
    <div id="page-weather" class="page-view space-y-6">

        <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div class="holo-card space-y-4">
                <h3 class="text-xl font-extrabold neon-text-cyan" data-i18n="weather_box_title">🌍 Choose Your Crop & Check Udupi Weather</h3>
                <p class="text-xs text-slate-300">Select your soil type and crop so the system gives accurate watering & harvest profit estimates.</p>

                <div class="space-y-3">
                    <div>
                        <label class="text-xs font-bold text-slate-300">Your Farm Soil Type</label>
                        <select id="sel_region" class="cyber-select rbac-input mt-1">
                            <option value="Coastal Laterite Soil (Udyavar / Udupi Corridor)" selected>Coastal Laterite Soil (Udyavar / Udupi)</option>
                            <option value="Alluvial River Basin (Swarna River Belt)">Alluvial River Basin (Swarna Belt)</option>
                            <option value="Highland Western Ghats Loam">Highland Western Ghats Loam</option>
                            <option value="Arid Red Lateritic Plateau">Arid Red Lateritic Plateau</option>
                        </select>
                    </div>

                    <div>
                        <label class="text-xs font-bold text-slate-300">Your Main Crop</label>
                        <select id="sel_crop" class="cyber-select rbac-input mt-1">
                            <option value="Paddy" selected>🌾 Paddy (Rice)</option>
                            <option value="Arecanut">🌴 Arecanut (Supari)</option>
                            <option value="Pepper">🌿 Black Pepper</option>
                            <option value="Sugarcane">🎋 Sugarcane</option>
                            <option value="Vegetables (General)">🍅 Vegetables</option>
                        </select>
                    </div>

                    <div class="grid grid-cols-2 gap-3 pt-1">
                        <button onclick="applyRegionAndCrop()" class="btn-cyber btn-emerald rbac-write-btn text-xs">
                            ✅ Save My Crop
                        </button>
                        <button onclick="syncWeatherAPI()" class="btn-cyber btn-cyan text-xs">
                            ☁️ Update Local Weather
                        </button>
                    </div>
                </div>

                <div id="weatherSyncOutput" class="p-4 rounded-2xl bg-black/75 border border-[#00F0FF]/40 text-xs text-slate-200 leading-relaxed">
                    Location: Udyavar / Udupi · Click "Update Local Weather" for live satellite forecast.
                </div>
            </div>

            <div class="space-y-6">
                <div class="holo-card space-y-3">
                    <div class="flex justify-between items-center">
                        <h3 class="text-lg font-extrabold neon-text-green" data-i18n="yield_title">💹 Expected Harvest & Earnings (5 Hectares)</h3>
                        <span id="yieldConfBadge" class="text-xs mono px-2.5 py-0.5 rounded-full bg-[#39FF14]/20 text-[#39FF14]">95% Accuracy</span>
                    </div>
                    <div class="grid grid-cols-2 gap-3">
                        <div class="p-3.5 rounded-2xl bg-black/75 border border-[#39FF14]/40">
                            <div class="text-xs text-slate-400">Expected Harvest Weight</div>
                            <div class="text-2xl font-black mono neon-text-green mt-1" id="yieldTons">-- Tons</div>
                            <div class="text-xs text-slate-400 mt-0.5" id="yieldCropLabel">Crop: Paddy</div>
                        </div>
                        <div class="p-3.5 rounded-2xl bg-black/75 border border-[#FF007F]/40">
                            <div class="text-xs text-slate-400">Estimated Market Value</div>
                            <div class="text-2xl font-black mono neon-text-pink mt-1" id="yieldRevenue">₹--</div>
                            <div class="text-xs text-slate-400 mt-0.5" id="yieldUnitPrice">Market Rate: ₹--/t</div>
                        </div>
                    </div>
                    <div class="text-xs text-slate-400">Daily Crop Water Need: <b class="text-[#FFE600]" id="yieldEt0">-- mm/day</b></div>
                </div>

                <div class="holo-card space-y-2">
                    <div class="flex justify-between items-center">
                        <h3 class="text-lg font-extrabold neon-text-pink" data-i18n="pest_title">🦠 Crop Disease & Pest Protection</h3>
                        <span id="pestBadge" class="text-xs mono font-bold px-3 py-1 rounded-full bg-[#39FF14]/20 text-[#39FF14]">SAFE</span>
                    </div>
                    <div class="p-3.5 rounded-2xl bg-black/75 border border-slate-800 text-xs space-y-1.5">
                        <div>Watched Disease/Pest: <b id="pestThreat" class="text-[#FFE600]">--</b></div>
                        <div>Leaf Moisture: <b id="pestWetness" class="text-[#00F0FF]">--%</b> · Risk Chance: <b id="pestConfidence" class="text-[#FF007F]">--%</b></div>
                        <div class="pt-1 text-[#39FF14] font-semibold" id="pestPrescription">Checking crop health...</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Easy "What-If" Weather Calculator -->
        <div class="holo-card space-y-4">
            <h3 class="text-lg font-extrabold neon-text-green">🧪 "What-If" Weather & Watering Calculator</h3>
            <p class="text-xs text-slate-300">Move the sliders to see how much water your crop needs on a hotter or drier day.</p>

            <div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
                <div>
                    <div class="flex justify-between"><span>If Temperature is:</span><b class="text-[#00F0FF]" id="sim_temp_label">34.0 °C</b></div>
                    <input type="range" id="sim_temp" min="15" max="50" step="0.5" value="34" class="w-full mt-2">
                </div>
                <div>
                    <div class="flex justify-between"><span>If Air Moisture is:</span><b class="text-[#00F0FF]" id="sim_humidity_label">68.0 %</b></div>
                    <input type="range" id="sim_humidity" min="10" max="95" step="0.5" value="68" class="w-full mt-2">
                </div>
                <div>
                    <div class="flex justify-between"><span>If Soil Wetness is:</span><b class="text-[#00F0FF]" id="sim_moisture_label">36.0 %</b></div>
                    <input type="range" id="sim_moisture" min="10" max="90" step="0.5" value="36" class="w-full mt-2">
                </div>
            </div>

            <button onclick="runSimulation()" class="btn-cyber btn-emerald text-xs">⚡ Calculate Water Needed</button>
            <div id="simOutput" class="p-4 rounded-2xl bg-black/75 border border-[#39FF14]/40 text-xs text-slate-200">
                Click "Calculate Water Needed" to see and hear the recommendation.
            </div>
        </div>
    </div>

    <!-- ========================================================== -->
    <!-- PAGE 5: 💰 SAVINGS & SECURITY RECORDS                      -->
    <!-- ========================================================== -->
    <div id="page-ledger" class="page-view space-y-6">

        <div class="holo-card space-y-4">
            <div class="flex justify-between items-center">
                <div>
                    <h2 class="text-xl font-extrabold neon-text-yellow" data-i18n="sav_heading">⚡ Water, Electricity & Solar Savings</h2>
                    <p class="text-xs text-slate-300">Total water and power saved by smart automatic watering.</p>
                </div>
                <span id="carbonGrade" class="text-xs mono font-bold px-3 py-1 rounded-full bg-[#39FF14]/20 text-[#39FF14]">A+ GREEN</span>
            </div>

            <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
                <div class="p-4 rounded-2xl bg-black/75 border border-slate-800">
                    <div class="text-xs text-slate-400">🪣 Total Water Saved</div>
                    <div class="text-2xl font-black mono neon-text-cyan mt-1" id="carbWater">-- L</div>
                </div>
                <div class="p-4 rounded-2xl bg-black/75 border border-slate-800">
                    <div class="text-xs text-slate-400">🌿 Pollution Prevented (CO₂)</div>
                    <div class="text-2xl font-black mono neon-text-green mt-1" id="carbCo2">-- kg</div>
                </div>
                <div class="p-4 rounded-2xl bg-black/75 border border-slate-800">
                    <div class="text-xs text-slate-400">⚡ Pump Electricity Used</div>
                    <div class="text-2xl font-black mono neon-text-yellow mt-1" id="carbKwh">-- kWh</div>
                </div>
                <div class="p-4 rounded-2xl bg-black/75 border border-slate-800">
                    <div class="text-xs text-slate-400">☀️ Solar Power Share</div>
                    <div class="text-2xl font-black mono neon-text-pink mt-1" id="carbSolar">84.0%</div>
                </div>
            </div>
        </div>

        <div class="holo-card space-y-4">
            <div class="flex flex-wrap items-center justify-between gap-3">
                <div>
                    <h2 class="text-xl font-extrabold neon-text-cyan" data-i18n="ledger_heading">📋 Tamper-Proof Farm Activity Log</h2>
                    <p class="text-xs text-slate-300">Every pump action, sign-in, and alert is safely recorded.</p>
                </div>
                <button onclick="exportComplianceCSV()" class="btn-cyber btn-emerald text-xs">
                    📥 Download Full Farm Report (CSV)
                </button>
            </div>

            <div id="blockchainList" class="space-y-2.5 max-h-[380px] overflow-y-auto pr-1"></div>
        </div>
    </div>

</div>

<script>
    const $ = (id) => document.getElementById(id);
    let telemetryChart = null;
    let fieldModeActive = false;
    let techDetailsVisible = false;
    let spokenPromptsEnabled = true;
    let currentLang = 'en';
    let currentUser = null;

    /* ==========================================================
       🌐 MULTI-LANGUAGE DICTIONARY (PLAIN, FOOLPROOF WORDS)
       ========================================================== */
    const I18N = {
        en: {
            app_title: "AQUASENSE PRO",
            app_subtitle: "Easy Smart Farm & Water Helper · Udupi",
            btn_signin: "Sign In (Email / Phone)",
            btn_field_mode: "Daylight Mode",
            btn_emergency: "STOP ALL PUMPS",
            nav_home: "1. Home Summary",
            nav_pumps: "2. Water Pumps & Fertilizer",
            nav_drone: "3. Field Map & Charts",
            nav_weather: "4. Weather & Crop Profit",
            nav_ledger: "5. Savings & History",
            home_banner_sub: "Your soil wetness and weather are checked every 5 seconds automatically.",
            btn_goto_pumps: "Control Water Pumps ➔",
            btn_send_report: "Send Report to My Email/Phone",
            sec_farm_health: "🌱 Simple Farm Health Check",
            sec_farm_health_sub: "Easy-to-understand status cards for your farm.",
            btn_show_tech: "Show Technical Numbers",
            btn_hide_tech: "Hide Technical Numbers",
            kpi_moisture: "💧 Soil Wetness",
            kpi_moisture_sub: "Roots have enough water",
            kpi_temp: "🌡️ Today's Warmth",
            kpi_temp_sub: "Good weather for crop growth",
            kpi_rain: "🌧️ Rain Today",
            kpi_rain_sub: "Pumps stop automatically if heavy rain comes",
            kpi_humidity: "💨 Air Moisture",
            kpi_humidity_sub: "Leaves are not drying out",
            kpi_ec: "🧪 Soil Health & Food",
            kpi_ec_sub: "Safe salt & fertilizer balance",
            kpi_flow: "🚰 Water Pipes Check",
            voice_title: "🎙️ Speak to Your Farm Helper (AquaVoice)",
            voice_desc: "Tap any button below or speak to control pumps with your voice.",
            btn_mic: "Tap to Speak",
            zone_a_title: "🌾 Field A – Laterite Soil Plot",
            zone_b_title: "🌴 Field B – Alluvial River Plot",
            lbl_soil_water: "Soil Wetness:",
            btn_pump_on: "Turn Water ON Now",
            btn_pump_off: "Stop Water",
            btn_pump_auto: "Let Smart Auto Handle It",
            fert_heading: "🧪 Add Liquid Fertilizer (NPK) & Water Timer",
            drone_heading: "🚁 12-Section Farm Map (Drone View)",
            drone_desc: "Green sections have good water. Pink sections are dry and need water.",
            chart_heading: "📊 Soil Wetness & Weather History Graph",
            weather_box_title: "🌍 Choose Your Crop & Check Udupi Weather",
            yield_title: "💹 Expected Harvest & Earnings (5 Hectares)",
            pest_title: "🦠 Crop Disease & Pest Protection",
            sav_heading: "⚡ Water, Electricity & Solar Savings",
            ledger_heading: "📋 Tamper-Proof Farm Activity Log",
            signin_title: "Sign In to Save Your Farm Info",
            signin_subtitle: "Saves your crop, language & sends farm alerts to your Email or Phone",
            lbl_farmer_name: "Your Name",
            lbl_contact_id: "Your Email Address OR Phone Number *",
            btn_save_login: "✅ Sign In & Save My Settings",
            status_dry: "Needs Water Soon (Dry)",
            status_moist: "Just Right (Moist)",
            status_wet: "Very Wet (Enough Water)",
            soil_good: "Good for Roots",
            soil_salty: "High Salt - Add Water",
            pipes_ok: "No Leaks or Blockages",
            pipes_active: "Water Flowing to Crops",
            pump_watering: "💧 WATERING NOW",
            pump_stopped: "⏹️ STOPPED",
            pump_auto: "🤖 SMART AUTO"
        },
        kn: {
            app_title: "ಆಕ್ವಾಸೆನ್ಸ್ ಪ್ರೊ (AQUASENSE)",
            app_subtitle: "ಸುಲಭ ಕೃಷಿ ನೀರು ಮತ್ತು ಬೆಳೆ ಸಹಾಯಕ · ಉಡುಪಿ",
            btn_signin: "ಸೈನ್ ಇನ್ (ಇಮೇಲ್ / ಫೋನ್)",
            btn_field_mode: "ಬಿಸಿಲು ಮೋಡ್",
            btn_emergency: "ಎಲ್ಲಾ ಪಂಪ್ ನಿಲ್ಲಿಸಿ",
            nav_home: "1. ಜಮೀನಿನ ಸಾರಾಂಶ",
            nav_pumps: "2. ನೀರಿನ ಪಂಪ್ ಮತ್ತು ಗೊಬ್ಬರ",
            nav_drone: "3. ಜಮೀನಿನ ನಕ್ಷೆ & ಗ್ರಾಫ್",
            nav_weather: "4. ಹವಾಮಾನ ಮತ್ತು ಬೆಳೆ ಲಾಭ",
            nav_ledger: "5. ಉಳಿತಾಯ ಮತ್ತು ದಾಖಲೆ",
            home_banner_sub: "ಮಣ್ಣಿನ ಹಸಿ ಮತ್ತು ಹವಾಮಾನವನ್ನು ಪ್ರತಿ 5 ಸೆಕೆಂಡಿಗೆ ಸ್ವಯಂಚಾಲಿತವಾಗಿ ಪರಿಶೀಲಿಸಲಾಗುತ್ತದೆ.",
            btn_goto_pumps: "ನೀರಿನ ಪಂಪ್ ನಿಯಂತ್ರಿಸಿ ➔",
            btn_send_report: "ನನ್ನ ಫೋನ್/ಇಮೇಲ್‌ಗೆ ವರದಿ ಕಳುಹಿಸಿ",
            sec_farm_health: "🌱 ಜಮೀನಿನ ಸರಳ ಆರೋಗ್ಯ ಮಾಹಿತಿ",
            sec_farm_health_sub: "ನಿಮ್ಮ ಜಮೀನಿನ ಸ್ಥಿತಿಯನ್ನು ಸುಲಭ ಪದಗಳಲ್ಲಿ ನೋಡಿ.",
            btn_show_tech: "ತಾಂತ್ರಿಕ ಸಂಖ್ಯೆಗಳನ್ನು ತೋರಿಸಿ",
            btn_hide_tech: "ತಾಂತ್ರಿಕ ಸಂಖ್ಯೆಗಳನ್ನು ಮರೆಮಾಡಿ",
            kpi_moisture: "💧 ಮಣ್ಣಿನ ಹಸಿ (ತೇವಾಂಶ)",
            kpi_moisture_sub: "ಬೇರುಗಳಿಗೆ ಸಾಕಷ್ಟು ನೀರಿದೆ",
            kpi_temp: "🌡️ ಇಂದಿನ ಬಿಸಿಲು",
            kpi_temp_sub: "ಬೆಳೆ ಬೆಳವಣಿಗೆಗೆ ಉತ್ತಮ ಹವಾಮಾನ",
            kpi_rain: "🌧️ ಇಂದು ಮಳೆಯ ಸಾಧ್ಯತೆ",
            kpi_rain_sub: "ಮಳೆ ಬಂದರೆ ಪಂಪ್ ತಾನಾಗಿಯೇ ನಿಲ್ಲುತ್ತದೆ",
            kpi_humidity: "💨 ಗಾಳಿಯಲ್ಲಿ ತೇವಾಂಶ",
            kpi_humidity_sub: "ಎಲೆಗಳು ಒಣಗುತ್ತಿಲ್ಲ",
            kpi_ec: "🧪 ಮಣ್ಣಿನ ಆರೋಗ್ಯ",
            kpi_ec_sub: "ಬೇರುಗಳಿಗೆ ಉತ್ತಮ ಪೋಷಕಾಂಶವಿದೆ",
            kpi_flow: "🚰 ನೀರಿನ ಪೈಪ್ ಸ್ಥಿತಿ",
            voice_title: "🎙️ ಧ್ವನಿ ಸಹಾಯಕ (AquaVoice)",
            voice_desc: "ಕೆಳಗಿನ ಬಟನ್ ಒತ್ತಿ ಅಥವಾ ಮಾತನಾಡುವ ಮೂಲಕ ಪಂಪ್ ನಿಯಂತ್ರಿಸಿ.",
            btn_mic: "ಮಾತನಾಡಲು ಒತ್ತಿರಿ",
            zone_a_title: "🌾 ಜಮೀನು A – ಕೆಂಪು ಮಣ್ಣಿನ ತೋಟ",
            zone_b_title: "🌴 ಜಮೀನು B – ನದಿ ತೀರದ ತೋಟ",
            lbl_soil_water: "ಮಣ್ಣಿನ ಹಸಿ:",
            btn_pump_on: "ಈಗ ನೀರು ಆನ್ ಮಾಡಿ",
            btn_pump_off: "ನೀರು ನಿಲ್ಲಿಸಿ",
            btn_pump_auto: "ಸ್ಮಾರ್ಟ್ ಆಟೋ ನೋಡಿಕೊಳ್ಳಲಿ",
            fert_heading: "🧪 ದ್ರವ ಗೊಬ್ಬರ (NPK) ಮತ್ತು ನೀರಿನ ಟೈಮರ್",
            drone_heading: "🚁 12-ವಿಭಾಗದ ಜಮೀನಿನ ನಕ್ಷೆ (ಡ್ರೋನ್ ನೋಟ)",
            drone_desc: "ಹಸಿರು ಬಾಕ್ಸ್‌ನಲ್ಲಿ ಸಾಕಷ್ಟು ನೀರಿದೆ. ಗುಲಾಬಿ ಬಾಕ್ಸ್ ಒಣಗಿದೆ.",
            chart_heading: "📊 ಮಣ್ಣಿನ ನೀರು ಮತ್ತು ಹವಾಮಾನ ಗ್ರಾಫ್",
            weather_box_title: "🌍 ನಿಮ್ಮ ಬೆಳೆ ಆಯ್ಕೆಮಾಡಿ & ಉಡುಪಿ ಹವಾಮಾನ ನೋಡಿ",
            yield_title: "💹 ನಿರೀಕ್ಷಿತ ಇಳುವರಿ ಮತ್ತು ಆದಾಯ (5 ಹೆಕ್ಟೇರ್)",
            pest_title: "🦠 ಬೆಳೆ ರೋಗ ಮತ್ತು ಕೀಟ ರಕ್ಷಣೆ",
            sav_heading: "⚡ ನೀರು, ವಿದ್ಯುತ್ ಮತ್ತು ಸೋಲಾರ್ ಉಳಿತಾಯ",
            ledger_heading: "📋 ಸುರಕ್ಷಿತ ಜಮೀನಿನ ಚಟುವಟಿಕೆ ದಾಖಲೆ",
            signin_title: "ನಿಮ್ಮ ಮಾಹಿತಿ ಉಳಿಸಲು ಸೈನ್ ಇನ್ ಮಾಡಿ",
            signin_subtitle: "ನಿಮ್ಮ ಫೋನ್ ಸಂಖ್ಯೆ ಅಥವಾ ಇಮೇಲ್ ಬಳಸಿ ಬೆಳೆ ಮತ್ತು ಭಾಷೆಯನ್ನು ಉಳಿಸಿ",
            lbl_farmer_name: "ನಿಮ್ಮ ಹೆಸರು",
            lbl_contact_id: "ಇಮೇಲ್ ಅಥವಾ ಫೋನ್ ಸಂಖ್ಯೆ *",
            btn_save_login: "✅ ಸೈನ್ ಇನ್ ಮಾಡಿ & ಉಳಿಸಿ",
            status_dry: "ನೀರು ಬೇಕಾಗಿದೆ (ಒಣಗಿದೆ)",
            status_moist: "ಸರಿಯಾದ ಹಸಿ ಇದೆ (ಉತ್ತಮ)",
            status_wet: "ಸಾಕಷ್ಟು ನೀರಿದೆ (ಹಸಿಯಾಗಿದೆ)",
            soil_good: "ಬೇರುಗಳಿಗೆ ತುಂಬಾ ಒಳ್ಳೆಯದು",
            soil_salty: "ಉಪ್ಪಿನಂಶ ಹೆಚ್ಚಿದೆ - ನೀರು ಬಿಡಿ",
            pipes_ok: "ಪೈಪ್‌ಗಳು ಸರಿಯಾಗಿವೆ (ಸೋರಿಕೆ ಇಲ್ಲ)",
            pipes_active: "ಬೆಳೆಗಳಿಗೆ ನೀರು ಹರಿಯುತ್ತಿದೆ",
            pump_watering: "💧 ನೀರು ಹರಿಯುತ್ತಿದೆ",
            pump_stopped: "⏹️ ನಿಂತಿದೆ",
            pump_auto: "🤖 ಸ್ಮಾರ್ಟ್ ಆಟೋ"
        },
        hi: {
            app_title: "एक्वासेंस प्रो (AQUASENSE)",
            app_subtitle: "आसान स्मार्ट किसान पानी और फसल सहायक · उडुपी",
            btn_signin: "साइन इन (ईमेल / फ़ोन)",
            btn_field_mode: "धूप मोड (Daylight)",
            btn_emergency: "सभी पंप तुरंत बंद करें",
            nav_home: "1. खेत का सारांश",
            nav_pumps: "2. पानी पंप और खाद",
            nav_drone: "3. खेत का नक्शा और ग्राफ",
            nav_weather: "4. मौसम और फसल मुनाफा",
            nav_ledger: "5. कुल बचत और रिकॉर्ड",
            home_banner_sub: "आपके खेत की मिट्टी की नमी और मौसम की जांच हर 5 सेकंड में अपने आप होती है।",
            btn_goto_pumps: "पानी पंप चलाएं ➔",
            btn_send_report: "मेरे ईमेल/फ़ोन पर रिपोर्ट भेजें",
            sec_farm_health: "🌱 खेत की आसान स्वास्थ्य जांच",
            sec_farm_health_sub: "बिना कठिन शब्दों के अपने खेत की स्थिति समझें।",
            btn_show_tech: "तकनीकी नंबर दिखाएं",
            btn_hide_tech: "तकनीकी नंबर छुपाएं",
            kpi_moisture: "💧 मिट्टी की नमी",
            kpi_moisture_sub: "जड़ों को पर्याप्त पानी मिल रहा है",
            kpi_temp: "🌡️ आज की गर्माहट",
            kpi_temp_sub: "फसल के लिए अच्छा मौसम",
            kpi_rain: "🌧️ आज बारिश का अनुमान",
            kpi_rain_sub: "तेज बारिश आने पर पंप अपने आप रुक जाएंगे",
            kpi_humidity: "💨 हवा में नमी",
            kpi_humidity_sub: "पत्ते सूख नहीं रहे हैं",
            kpi_ec: "🧪 मिट्टी की सेहत",
            kpi_ec_sub: "जड़ों के लिए बिल्कुल सही पोषण",
            kpi_flow: "🚰 पानी के पाइप की जांच",
            voice_title: "🎙️ बोलकर खेत कंट्रोल करें (AquaVoice)",
            voice_desc: "नीचे दिए गए किसी भी बटन को दबाएं या माइक में बोलें।",
            btn_mic: "बोलने के लिए दबाएं",
            zone_a_title: "🌾 खेत A – लैटेराइट मिट्टी प्लॉट",
            zone_b_title: "🌴 खेत B – नदी किनारे का प्लॉट",
            lbl_soil_water: "मिट्टी की नमी:",
            btn_pump_on: "अभी पानी चालू करें",
            btn_pump_off: "पानी बंद करें",
            btn_pump_auto: "स्मार्ट ऑटो को संभालने दें",
            fert_heading: "🧪 लिक्विड खाद (NPK) और पानी टाइमर",
            drone_heading: "🚁 12-सेक्शन खेत का नक्शा (ड्रोन व्यू)",
            drone_desc: "हरे बॉक्स में अच्छा पानी है। गुलाबी बॉक्स सूखे हैं।",
            chart_heading: "📊 मिट्टी की नमी और मौसम ग्राफ",
            weather_box_title: "🌍 अपनी फसल चुनें और मौसम देखें",
            yield_title: "💹 अनुमानित पैदावार और कमाई (5 हेक्टेयर)",
            pest_title: "🦠 फसल रोग और कीट सुरक्षा",
            sav_heading: "⚡ पानी, बिजली और सोलर बचत",
            ledger_heading: "📋 सुरक्षित खेत गतिविधि रिकॉर्ड",
            signin_title: "अपनी जानकारी सेव करने के लिए साइन इन करें",
            signin_subtitle: "अपनी फसल, भाषा और ईमेल/फ़ोन अलर्ट सेव करें",
            lbl_farmer_name: "आपका नाम",
            lbl_contact_id: "आपका ईमेल या फ़ोन नंबर *",
            btn_save_login: "✅ साइन इन करें और सेव करें",
            status_dry: "पानी की जरूरत है (सूखी मिट्टी)",
            status_moist: "बिल्कुल सही नमी (गीली)",
            status_wet: "पर्याप्त पानी है (अच्छी नमी)",
            soil_good: "जड़ों के लिए बहुत अच्छा",
            soil_salty: "नमक ज्यादा है - पानी चलाएं",
            pipes_ok: "कोई लीकेज या रुकावट नहीं",
            pipes_active: "फसलों में पानी जा रहा है",
            pump_watering: "💧 पानी चालू है",
            pump_stopped: "⏹️ बंद है",
            pump_auto: "🤖 स्मार्ट ऑटो"
        }
    };

    /* ==========================================================
       🗣️ UPGRADE 3: INSTANT VOICE CONFIRMATION HELPER
       ========================================================== */
    function speakActionConfirmation(text) {
        if (!spokenPromptsEnabled || !('speechSynthesis' in window)) return;
        try {
            window.speechSynthesis.cancel();
            const utter = new SpeechSynthesisUtterance(text);
            utter.rate = 1.0;
            utter.pitch = 1.02;
            window.speechSynthesis.speak(utter);
        } catch (e) {}
    }

    function toggleSpokenPrompts() {
        spokenPromptsEnabled = !spokenPromptsEnabled;
        $('voicePromptsLabel').textContent = spokenPromptsEnabled ? 'Voice Alerts: ON' : 'Voice Alerts: OFF';
        if (spokenPromptsEnabled) {
            speakActionConfirmation('Voice confirmations turned on.');
        }
        showToast(
            spokenPromptsEnabled ? 'Spoken Voice Alerts ON' : 'Spoken Voice Alerts Muted',
            spokenPromptsEnabled ? 'Every button action will now speak a confirmation out loud.' : 'Voice confirmations muted.',
            'info'
        );
    }

    /* ==========================================================
       🔬 UPGRADE 1: PLAIN LANGUAGE vs TECH DETAILS TOGGLE
       ========================================================== */
    function toggleTechDetails() {
        techDetailsVisible = !techDetailsVisible;
        document.body.classList.toggle('show-tech-details', techDetailsVisible);
        const dict = I18N[currentLang] || I18N.en;
        $('techToggleText').textContent = techDetailsVisible ? dict.btn_hide_tech : dict.btn_show_tech;
        showToast(
            techDetailsVisible ? 'Showing Technical Numbers' : 'Simple Farmer View Active',
            techDetailsVisible ? 'Displaying raw VWC %, dS/m EC, and LPM sensor numbers.' : 'Hiding raw technical numbers for a cleaner view.',
            'info'
        );
    }

    function getPlainMoistureText(vwc) {
        const dict = I18N[currentLang] || I18N.en;
        if (vwc < 38) return dict.status_dry;
        if (vwc <= 72) return dict.status_moist;
        return dict.status_wet;
    }

    function getFriendlyPumpBadge(stateStr) {
        const dict = I18N[currentLang] || I18N.en;
        if (stateStr === 'FORCE_ON' || stateStr === 'ACTIVE') return dict.pump_watering;
        if (stateStr === 'FORCE_OFF') return dict.pump_stopped;
        return dict.pump_auto;
    }

    function setLanguage(lang) {
        currentLang = lang;
        ['en', 'kn', 'hi'].forEach(l => {
            const btn = $('lang-' + l);
            if (!btn) return;
            if (l === lang) {
                btn.className = "px-3 py-1.5 rounded-lg text-xs font-extrabold bg-[#00F0FF] text-slate-950";
            } else {
                btn.className = "px-3 py-1.5 rounded-lg text-xs font-bold text-slate-300 hover:text-white";
            }
        });

        const dict = I18N[lang] || I18N.en;
        document.querySelectorAll('[data-i18n]').forEach(el => {
            const key = el.getAttribute('data-i18n');
            if (dict[key]) el.textContent = dict[key];
        });

        $('techToggleText').textContent = techDetailsVisible ? dict.btn_hide_tech : dict.btn_show_tech;

        if (currentUser) {
            $('signInBtnText').textContent = `${currentUser.farmer_name} (${currentUser.contact_id})`;
            syncUserPreferencesToCloud(true);
        }
        refreshTelemetry();
    }

    /* ==========================================================
       📄 SEPARATE PAGE NAVIGATION
       ========================================================== */
    function switchPage(pageId, btnEl) {
        document.querySelectorAll('.page-view').forEach(p => p.classList.remove('active'));
        document.querySelectorAll('.mac-tab-btn').forEach(b => b.classList.remove('active'));

        const target = $('page-' + pageId);
        if (target) target.classList.add('active');
        if (btnEl) btnEl.classList.add('active');

        if (pageId === 'drone' && telemetryChart) {
            setTimeout(() => telemetryChart.resize(), 60);
        }
    }

    /* ==========================================================
       👤 FARMER SIGN-IN & REAL GMAIL SMTP SYNC
       ========================================================== */
    function openSignInModal() {
        $('signInModal').classList.add('open');
        if (currentUser) {
            $('loginNameInput').value = currentUser.farmer_name || '';
            $('loginContactInput').value = currentUser.contact_id || '';
            $('signedInProfileBox').classList.remove('hidden');
            $('modalUserSummary').textContent = `Signed in as ${currentUser.farmer_name} (${currentUser.contact_id})`;
        } else {
            $('signedInProfileBox').classList.add('hidden');
        }
    }

    async function submitFarmerLogin() {
        const contact_id = $('loginContactInput').value.trim();
        const farmer_name = $('loginNameInput').value.trim() || 'Farmer';
        const smtp_sender_email = $('smtpSenderInput').value.trim();
        const smtp_app_password = $('smtpPassInput').value.trim();

        if (!contact_id) {
            showToast('Missing Contact Info', 'Please enter your Email Address or Phone Number.', 'warning');
            return;
        }

        try {
            const res = await api('/api/auth/login', 'POST', {
                contact_id,
                farmer_name,
                preferred_lang: currentLang,
                smtp_sender_email,
                smtp_app_password
            });
            currentUser = res.user;
            localStorage.setItem('aquasense_user', JSON.stringify(currentUser));

            if (currentUser.saved_region) $('sel_region').value = currentUser.saved_region;
            if (currentUser.saved_crop) $('sel_crop').value = currentUser.saved_crop;

            $('signInBtnText').textContent = `${currentUser.farmer_name} (${currentUser.contact_id})`;
            $('signedInProfileBox').classList.remove('hidden');
            $('modalUserSummary').textContent = `Signed in as ${currentUser.farmer_name} (${currentUser.contact_id})`;

            speakActionConfirmation(`Welcome ${currentUser.farmer_name}. Your farm profile is saved.`);
            showToast('Signed In & Saved!', res.message, 'success');
            closeModals();
            refreshTelemetry();
        } catch (e) {
            showToast('Sign-In Error', e.message, 'error');
        }
    }

    async function syncUserPreferencesToCloud(silent = false) {
        if (!currentUser || !currentUser.contact_id) return;
        try {
            const res = await api('/api/auth/sync', 'POST', {
                contact_id: currentUser.contact_id,
                preferred_lang: currentLang,
                saved_region: $('sel_region').value,
                saved_crop: $('sel_crop').value
            });
            currentUser.preferred_lang = currentLang;
            currentUser.saved_region = $('sel_region').value;
            currentUser.saved_crop = $('sel_crop').value;
            localStorage.setItem('aquasense_user', JSON.stringify(currentUser));
            if (!silent) {
                speakActionConfirmation('Your farm settings have been saved.');
                showToast('Settings Saved!', res.message, 'success');
            }
        } catch (e) {}
    }

    function logoutFarmer() {
        currentUser = null;
        localStorage.removeItem('aquasense_user');
        $('signInBtnText').textContent = I18N[currentLang].btn_signin;
        $('signedInProfileBox').classList.add('hidden');
        closeModals();
        speakActionConfirmation('You have signed out.');
        showToast('Signed Out', 'You have signed out of your profile.', 'info');
    }

    /* ==========================================================
       🔔 TOAST NOTIFICATION ENGINE
       ========================================================== */
    function showToast(title, message, type = 'info') {
        const container = $('toastContainer');
        if (!container) return;

        const colors = {
            success: { border: '#39FF14', icon: '✔', accent: 'text-[#39FF14]' },
            info:    { border: '#00F0FF', icon: '⚡', accent: 'text-[#00F0FF]' },
            warning: { border: '#FFE600', icon: '🔒', accent: 'text-[#FFE600]' },
            error:   { border: '#FF007F', icon: '🚨', accent: 'text-[#FF007F]' }
        };
        const st = colors[type] || colors.info;

        const toast = document.createElement('div');
        toast.className = 'cyber-toast';
        toast.style.borderColor = st.border;
        toast.innerHTML = `
            <div class="flex items-start justify-between gap-3">
                <div class="flex items-start gap-2.5">
                    <span class="text-base ${st.accent} font-black">${st.icon}</span>
                    <div>
                        <div class="text-xs font-black uppercase tracking-wider mono ${st.accent}">${title}</div>
                        <div class="text-xs text-slate-300 mt-0.5 leading-relaxed">${message}</div>
                    </div>
                </div>
                <button onclick="this.closest('.cyber-toast').remove()" class="text-xs text-slate-400 hover:text-white mono">✕</button>
            </div>
        `;
        container.appendChild(toast);

        setTimeout(() => {
            if (toast && toast.parentNode) toast.remove();
        }, 4500);
    }

    /* ==========================================================
       ☀️ DAYLIGHT FIELD MODE & RBAC CONTROLS
       ========================================================== */
    function toggleFieldMode() {
        fieldModeActive = !fieldModeActive;
        document.body.classList.toggle('field-mode', fieldModeActive);
        speakActionConfirmation(fieldModeActive ? 'Daylight outdoor mode turned on.' : 'Dark mode turned on.');
        showToast(
            fieldModeActive ? 'Daylight Mode ON' : 'Dark Neon Mode ON',
            fieldModeActive ? 'High-contrast sunlight mode enabled.' : 'Restored dark neon theme.',
            'info'
        );
    }

    function getActiveRole() {
        return $('rbacRoleSelect') ? $('rbacRoleSelect').value : 'Operator';
    }

    function onRoleChange() {
        const role = getActiveRole();
        const isAuditor = (role === 'Auditor');
        document.querySelectorAll('.rbac-write-btn').forEach(btn => {
            btn.classList.toggle('rbac-locked', isAuditor);
        });
        document.querySelectorAll('.rbac-input').forEach(inp => {
            inp.disabled = isAuditor;
        });
        speakActionConfirmation(isAuditor ? 'Viewer read only mode active.' : 'Farmer full control mode active.');
        showToast(
            isAuditor ? 'Read-Only Viewer Mode' : 'Full Control Enabled',
            isAuditor ? 'Pump buttons are locked in Viewer mode.' : 'You have full control over all water pumps.',
            isAuditor ? 'warning' : 'success'
        );
    }

    function checkVisualRbacGuard(actionName) {
        if (getActiveRole() === 'Auditor') {
            speakActionConfirmation('Action locked. Please switch from Viewer to Farmer mode at the top right.');
            showToast('Viewer Mode Lock', `Switch from 'Viewer' to 'Farmer' at the top right to run ${actionName}.`, 'warning');
            return false;
        }
        return true;
    }

    function closeModals() {
        $('signInModal').classList.remove('open');
    }

    /* ==========================================================
       API HELPER & CHART INITIALIZATION
       ========================================================== */
    async function api(url, method = 'GET', body = undefined) {
        const options = {
            method,
            headers: {
                'Content-Type': 'application/json',
                'X-User-Role': getActiveRole()
            }
        };
        if (body !== undefined) options.body = JSON.stringify(body);
        const res = await fetch(url, options);
        if (!res.ok) {
            let msg = res.statusText;
            try { const err = await res.json(); msg = err.detail || msg; } catch (e) {}
            throw new Error(msg);
        }
        return res.json();
    }

    function initChart() {
        if (!window.Chart) return;
        const c = $('telemetryChart').getContext('2d');
        telemetryChart = new Chart(c, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    { label: 'Soil Wetness %', data: [], borderColor: '#00F0FF', borderWidth: 2.5, tension: 0.4 },
                    { label: 'Temperature °C', data: [], borderColor: '#FFE600', borderWidth: 2, tension: 0.4 },
                    { label: 'Air Moisture %', data: [], borderColor: '#FF007F', borderWidth: 2, tension: 0.4 }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 300 },
                plugins: { legend: { labels: { color: '#E2F1FF' } } },
                scales: {
                    x: { ticks: { color: '#94a3b8', maxTicksLimit: 10 } },
                    y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(0, 240, 255, 0.08)' } }
                }
            }
        });
    }

    /* ==========================================================
       RENDERERS & LIVE TELEMETRY REFRESH (PLAIN WORDS + NUMBERS)
       ========================================================== */
    function renderThermalGrid(gridData) {
        if (!gridData || !gridData.length) return;
        $('thermalGridContainer').innerHTML = gridData.map(node => {
            const isStress = node.status === 'THERMAL_STRESS';
            const cls = isStress ? 'stress' : '';
            const statusLabel = isStress ? '⚠️ NEEDS WATER' : '✅ GOOD WATER';
            const badgeColor = isStress ? 'text-[#FF007F]' : 'text-[#39FF14]';
            return `
                <div onclick="inspectPlotSection('${node.node_id}', ${node.vwc}, ${node.leaf_temp}, ${isStress})" class="iso-node ${cls}">
                    <div class="flex items-center justify-between text-xs mono">
                        <span class="font-bold text-white">Section ${node.node_id}</span>
                        <span class="${badgeColor} font-bold">${statusLabel}</span>
                    </div>
                    <div class="mt-2 flex items-baseline justify-between">
                        <span class="text-xl font-black mono text-[#00F0FF]">${node.vwc}%</span>
                        <span class="text-xs mono text-[#FFE600]">${node.leaf_temp}°C</span>
                    </div>
                </div>
            `;
        }).join('');
    }

    function inspectPlotSection(nodeId, vwc, temp, isStress) {
        const msg = isStress
            ? `Section ${nodeId} is dry at ${vwc} percent water and needs watering soon.`
            : `Section ${nodeId} is healthy with ${vwc} percent soil water.`;
        speakActionConfirmation(msg);
        showToast(`Farm Section ${nodeId}`, msg, isStress ? 'warning' : 'success');
    }

    function renderBlockchainLedger(blocks) {
        if (!blocks || !blocks.length) return;
        $('blockchainList').innerHTML = blocks.map(b => `
            <div onclick="navigator.clipboard.writeText('${b.block_hash}'); showToast('Copied Security Code', 'Record #${b.block_id} code copied!', 'success')" class="p-3.5 rounded-2xl bg-black/75 border border-[#00F0FF]/30 hover:border-[#00F0FF] cursor-pointer transition text-xs">
                <div class="flex items-center justify-between">
                    <span class="font-bold text-[#39FF14] mono">RECORD #${b.block_id} · ${b.timestamp.slice(11, 19)}</span>
                    <span class="text-[11px] mono text-[#00F0FF]">📋 Tap to Copy</span>
                </div>
                <div class="text-white font-semibold mt-1 text-sm">${b.event_summary}</div>
                <div class="mt-1 flex justify-between text-[11px] mono text-slate-400">
                    <span>By: <b class="text-[#FF007F]">${b.triggered_by}</b></span>
                    <span>Code: ${b.block_hash.slice(0, 14)}...</span>
                </div>
            </div>
        `).join('');
    }

    async function refreshTelemetry() {
        try {
            const data = await api('/api/telemetry');
            const dict = I18N[currentLang] || I18N.en;
            $('connectionStatus').textContent = '● LIVE';

            const zones = data.zones || [];
            const count = zones.length || 1;
            const avgTemp = zones.reduce((a, z) => a + z.ambient_temp_c, 0) / count;
            const avgHum = zones.reduce((a, z) => a + z.humidity_pct, 0) / count;
            const avgMos = zones.reduce((a, z) => a + z.soil_moisture_vwc, 0) / count;
            const avgEc = zones.reduce((a, z) => a + z.electrical_conductivity_ec, 0) / count;
            const rainProb = data.system.rain_probability;

            // Raw Technical Numbers (for Tech Details Toggle)
            $('metric_temp').textContent = avgTemp.toFixed(1);
            $('metric_humidity').textContent = avgHum.toFixed(1);
            $('metric_moisture').textContent = avgMos.toFixed(1);
            $('metric_ec').textContent = avgEc.toFixed(2);
            $('metric_rain').textContent = rainProb.toFixed(0);
            $('metric_flow').textContent = `${data.system.flow_actual_lpm.toFixed(1)} LPM`;
            $('metric_watchdog').textContent = data.system.watchdog === 'NORMAL' ? '✅ Pipes Working Fine' : `⚠️ ${data.system.watchdog}`;

            // UPGRADE 1: Plain-Language Visual Cues on Home KPI Cards
            $('plain_moisture').textContent = getPlainMoistureText(avgMos);
            $('plain_temp').textContent = avgTemp > 34 ? 'Hot Today (' + avgTemp.toFixed(0) + '°C)' : 'Warm & Normal (' + avgTemp.toFixed(0) + '°C)';
            $('plain_rain').textContent = rainProb > 60 ? 'High Chance of Rain (' + rainProb.toFixed(0) + '%)' : 'Low Rain Chance (' + rainProb.toFixed(0) + '%)';
            $('plain_humidity').textContent = avgHum > 75 ? 'Humid Coastal Air' : 'Comfortable Air';
            $('plain_ec').textContent = avgEc > 2.5 ? dict.soil_salty : dict.soil_good;
            $('plain_flow').textContent = data.system.flow_actual_lpm > 1.0 ? dict.pipes_active : dict.pipes_ok;

            // Update Field A & Field B Foolproof Cards
            const zA = zones.find(z => z.zone_id === 'Zone_A_Laterite');
            if (zA) {
                $('zoneA_plain_moisture').textContent = `${getPlainMoistureText(zA.soil_moisture_vwc)} (${zA.soil_moisture_vwc.toFixed(0)}%)`;
                $('zoneA_bar').style.width = zA.soil_moisture_vwc + '%';
                $('zoneA_temp').textContent = zA.ambient_temp_c.toFixed(1) + '°C';
                $('zoneA_hum').textContent = zA.humidity_pct.toFixed(0) + '%';
                $('zoneA_ec').textContent = zA.electrical_conductivity_ec > 2.5 ? 'High Salt' : 'Good';
            }
            const zB = zones.find(z => z.zone_id === 'Zone_B_Alluvial');
            if (zB) {
                $('zoneB_plain_moisture').textContent = `${getPlainMoistureText(zB.soil_moisture_vwc)} (${zB.soil_moisture_vwc.toFixed(0)}%)`;
                $('zoneB_bar').style.width = zB.soil_moisture_vwc + '%';
                $('zoneB_temp').textContent = zB.ambient_temp_c.toFixed(1) + '°C';
                $('zoneB_hum').textContent = zB.humidity_pct.toFixed(0) + '%';
                $('zoneB_ec').textContent = zB.electrical_conductivity_ec > 2.5 ? 'High Salt' : 'Good';
            }

            const sys = data.system;
            $('activeAction').textContent = sys.active_action;
            $('zoneA_badge').textContent = getFriendlyPumpBadge(sys.pump_states.Zone_A_Laterite);
            $('zoneB_badge').textContent = getFriendlyPumpBadge(sys.pump_states.Zone_B_Alluvial);

            if (sys.drone) {
                $('droneStatusBadge').textContent = `${sys.drone.status} (${sys.drone.battery_pct}% BAT)`;
                renderThermalGrid(sys.drone.thermal_grid);
            }

            if (sys.pest_intelligence) {
                const p = sys.pest_intelligence;
                $('pestBadge').textContent = p.risk_level;
                $('pestThreat').textContent = p.threat_name;
                $('pestWetness').textContent = p.leaf_wetness_pct + '%';
                $('pestConfidence').textContent = p.confidence_pct + '%';
                $('pestPrescription').textContent = '💡 Advice: ' + p.bio_prescription;
            }

            if (sys.carbon) {
                $('carbWater').textContent = sys.carbon.total_water_saved_l + ' L';
                $('carbCo2').textContent = sys.carbon.co2_saved_kg + ' kg';
                $('carbKwh').textContent = sys.carbon.kwh_consumed + ' kWh';
                $('carbSolar').textContent = sys.carbon.solar_offset_pct + '%';
                $('carbonGrade').textContent = sys.carbon.efficiency_grade;
            }

            if (sys.yield_forecast) {
                const yf = sys.yield_forecast;
                $('yieldTons').textContent = yf.projected_yield_tons + ' Tons';
                $('yieldRevenue').textContent = '₹' + Number(yf.estimated_market_revenue_inr).toLocaleString('en-IN');
                $('yieldCropLabel').textContent = `Crop: ${yf.crop_variety}`;
                $('yieldUnitPrice').textContent = `Market Rate: ₹${Number(yf.price_per_ton_inr || 28500).toLocaleString('en-IN')}/t`;
                $('yieldEt0').textContent = `${yf.et0_mm_day} mm/day`;
                $('yieldConfBadge').textContent = `${yf.confidence_score}% Accuracy`;
            }

            if (sys.mqtt_bus) {
                $('mqttTopicText').textContent = sys.mqtt_bus.last_topic;
                $('mqttAckText').textContent = `${sys.mqtt_bus.last_payload} (${sys.mqtt_bus.latency_ms}ms)`;
            }
            if (sys.sms_gateway) {
                const targetContact = currentUser ? currentUser.contact_id : sys.sms_gateway.last_recipient;
                const smtpTag = sys.sms_gateway.real_email_configured ? ' [LIVE GMAIL SMTP READY]' : '';
                $('smsStatusText').textContent = `${targetContact} (#${sys.sms_gateway.dispatched_count} sent)${smtpTag}`;
            }

            renderBlockchainLedger(data.blockchain_ledger);

            if (telemetryChart && $('historicalRange').value === 'live') {
                const s = data.recent_series || [];
                telemetryChart.data.labels = s.map(r => r.timestamp.slice(11, 19));
                telemetryChart.data.datasets[0].data = s.map(r => r.moisture);
                telemetryChart.data.datasets[1].data = s.map(r => r.temp);
                telemetryChart.data.datasets[2].data = s.map(r => r.humidity);
                telemetryChart.update('none');
            }
        } catch (err) {
            $('connectionStatus').textContent = '● RECONNECTING';
        }
    }

    /* ==========================================================
       FOOLPROOF ACTION HANDLERS (WITH SPOKEN VOICE PROMPTS)
       ========================================================== */
    async function quickPumpOverride(zone_id, action) {
        if (!checkVisualRbacGuard('Water Pump Control')) return;
        const fieldName = zone_id.includes('Zone_A') ? 'Field A' : 'Field B';
        try {
            const d = await api('/api/irrigation/override', 'POST', { zone_id, action, duration_mins: 10 });
            let voiceMsg = '';
            if (action === 'FORCE_ON') {
                voiceMsg = `Starting water pump for ${fieldName} now.`;
            } else if (action === 'FORCE_OFF') {
                voiceMsg = `Stopping water pump for ${fieldName}.`;
            } else {
                voiceMsg = `Setting ${fieldName} to smart automatic watering.`;
            }
            speakActionConfirmation(voiceMsg);
            showToast(`${fieldName} Updated`, voiceMsg, 'success');
            refreshTelemetry();
        } catch (e) {
            speakActionConfirmation(e.message);
            showToast('Pump Action Blocked', e.message, 'error');
        }
    }

    async function sendOverride() {
        if (!checkVisualRbacGuard('Water Timer')) return;
        const zone_id = $('override_zone').value;
        const action = $('override_action').value;
        const mins = parseInt($('override_duration').value || '10', 10);
        const fieldName = zone_id.includes('Zone_A') ? 'Field A' : 'Field B';

        try {
            const d = await api('/api/irrigation/override', 'POST', { zone_id, action, duration_mins: mins });
            const msg = action === 'FORCE_ON'
                ? `Water timer started for ${fieldName} for ${mins} minutes.`
                : `${fieldName} water pump updated.`;
            $('overrideMsg').textContent = `✔ ${msg} (${d.water_volume_liters} Liters)`;
            speakActionConfirmation(msg);
            showToast('Water Timer Started', msg, 'success');
            refreshTelemetry();
        } catch (e) {
            showToast('Timer Error', e.message, 'error');
        }
    }

    async function sendFertigation() {
        if (!checkVisualRbacGuard('Add Fertilizer')) return;
        const payload = {
            growth_stage: $('fert_stage').value,
            nitrogen_ppm: parseFloat($('fert_n').value || '0'),
            phosphorus_ppm: parseFloat($('fert_p').value || '0'),
            potassium_ppm: parseFloat($('fert_k').value || '0'),
            leaching_risk_flag: false
        };
        try {
            const d = await api('/api/fertigation/configure', 'POST', payload);
            const msg = `Mixing and sending liquid fertilizer to crop roots for ${d.growth_stage} stage.`;
            $('fertMsg').textContent = `✔ ${msg}`;
            speakActionConfirmation(msg);
            showToast('Fertilizer Sent!', msg, 'success');
            refreshTelemetry();
        } catch (e) {
            showToast('Fertilizer Error', e.message, 'error');
        }
    }

    async function publishMQTTCommand() {
        if (!checkVisualRbacGuard('Test Field Valve Box')) return;
        const node_id = $('mqtt_node').value;
        const payload = $('mqtt_payload').value;
        const topic = `aquasense/esp32/${node_id.toLowerCase()}/command`;
        try {
            const d = await api('/api/mqtt/publish', 'POST', { node_id, topic, payload, qos: 1 });
            const msg = `Wireless signal sent to ${d.node_id}. Valve box replied in ${d.esp32_ack_latency_ms} milliseconds.`;
            speakActionConfirmation(msg);
            showToast('Field Valve Box Tested', msg, 'success');
            refreshTelemetry();
        } catch (e) {
            showToast('Signal Error', e.message, 'error');
        }
    }

    async function dispatchSMSAlert() {
        if (!checkVisualRbacGuard('Send Report')) return;
        const recipient = currentUser ? currentUser.contact_id : "+91-9876543210";
        const isEmail = recipient.includes('@');
        const payload = {
            recipient_phone: recipient,
            channel: isEmail ? "EMAIL REPORT" : "WHATSAPP + SMS",
            alert_message: `AquaSense Farm Report [${$('sel_crop').value}]: Soil wetness is ${$('plain_moisture').textContent} (${$('metric_moisture').textContent}%), Temperature is ${$('metric_temp').textContent}°C.`
        };
        try {
            const d = await api('/api/sms/alert', 'POST', payload);
            const emailNote = d.email_delivery && d.email_delivery.note ? d.email_delivery.note : '';
            const msg = `Farm report sent to ${d.recipient}. ${emailNote}`;
            speakActionConfirmation(`Farm report sent to ${d.recipient}.`);
            showToast('Farm Report Dispatched!', msg, 'success');
            refreshTelemetry();
        } catch (e) {
            showToast('Report Error', e.message, 'error');
        }
    }

    async function launchDroneScan() {
        speakActionConfirmation('Launching drone to check all 12 farm sections for dry spots.');
        showToast('Drone Scanning Farm...', 'Checking all 12 farm sections...', 'info');
        try {
            const res = await api('/api/drone/scan', 'POST');
            renderThermalGrid(res.drone.thermal_grid);
            const msg = `Drone scan complete. Found ${res.drone.anomalies_found} sections needing water.`;
            speakActionConfirmation(msg);
            showToast('Drone Scan Finished', msg, 'success');
            refreshTelemetry();
        } catch (e) {
            showToast('Drone Error', e.message, 'error');
        }
    }

    async function executeVoiceTranscript(transcript) {
        $('voiceInputBox').value = transcript;
        $('voiceStateBadge').textContent = 'THINKING...';
        try {
            const res = await api('/api/voice/command', 'POST', { transcript });
            $('voiceReplyBox').textContent = `🤖 "${res.reply}"`;
            $('voiceStateBadge').textContent = 'READY';
            showToast('Farm Helper', res.reply, 'info');
            speakActionConfirmation(res.reply);
            refreshTelemetry();
        } catch (e) {
            $('voiceStateBadge').textContent = 'LOCKED';
            speakActionConfirmation(e.message);
            showToast('Voice Locked', e.message, 'warning');
        }
    }

    function sendVoiceText() {
        const val = $('voiceInputBox').value.trim();
        if (val) executeVoiceTranscript(val);
    }

    function quickVoice(cmd) {
        executeVoiceTranscript(cmd);
    }

    function triggerVoiceAssistant() {
        const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
        if (!SpeechRecognition) {
            speakActionConfirmation('Please tap any of the quick voice buttons below.');
            showToast('Voice Helper', 'Tap any of the quick command buttons below!', 'info');
            return;
        }
        const recognition = new SpeechRecognition();
        recognition.lang = 'en-IN';
        $('voiceStateBadge').textContent = '🔴 LISTENING';
        recognition.onresult = (event) => executeVoiceTranscript(event.results[0][0].transcript);
        recognition.onerror = () => $('voiceStateBadge').textContent = 'READY';
        recognition.start();
    }

    async function applyRegionAndCrop() {
        if (!checkVisualRbacGuard('Save Crop')) return;
        const payload = { region: $('sel_region').value, crop_type: $('sel_crop').value };
        try {
            const data = await api('/api/region/configure', 'POST', payload);
            if (currentUser) syncUserPreferencesToCloud(true);
            const msg = `Saved your crop as ${data.selected_crop}. Expected harvest is ${data.yield_forecast.projected_yield_tons} tons.`;
            speakActionConfirmation(msg);
            showToast('Crop Saved!', msg, 'success');
            refreshTelemetry();
        } catch (e) {
            showToast('Save Error', e.message, 'error');
        }
    }

    async function syncWeatherAPI(silent = false) {
        try {
            const data = await api('/api/weather/sync');
            const w = data.weather;
            $('weatherSyncOutput').innerHTML =
                `<b>${w.source}</b><br>` +
                `Warmth: <b class="text-[#FFE600]">${w.temp}°C</b> · Air Moisture: <b class="text-[#00F0FF]">${w.humidity}%</b> · Rain Chance: <b class="text-[#39FF14]">${w.rain_prob}%</b>`;
            if (!silent) {
                speakActionConfirmation(`Weather updated. Temperature is ${w.temp} degrees Celsius with a ${w.rain_prob} percent chance of rain.`);
                showToast('Weather Updated!', `Udupi Temp: ${w.temp}°C, Rain Chance: ${w.rain_prob}%`, 'success');
            }
            refreshTelemetry();
        } catch (e) {}
    }

    async function runSimulation() {
        const payload = {
            temperature: parseFloat($('sim_temp').value),
            humidity: parseFloat($('sim_humidity').value),
            base_moisture: parseFloat($('sim_moisture').value),
            wind_speed: 2.4,
            solar_radiation: 18.5,
            crop_stage: 'mid_season',
            region: $('sel_region').value,
            crop_type: $('sel_crop').value
        };
        try {
            const d = await api('/api/simulate', 'POST', payload);
            $('simOutput').innerHTML =
                `<div class="text-[#39FF14] font-bold text-sm mb-1">💡 Advice: ${d.prescription.action}</div>` +
                `Recommended Watering Time: <b>${d.prescription.pump_minutes} minutes</b> (${d.prescription.water_volume_liters} Liters of water)<br>` +
                `Expected Harvest: <b class="text-[#00F0FF]">${d.yield_forecast.projected_yield_tons} Tons</b> (₹${Number(d.yield_forecast.estimated_market_revenue_inr).toLocaleString('en-IN')})<br>` +
                `Fertilizer Tip: <span class="text-[#FF007F] font-bold">${d.prescription.npk_advice}</span>`;
            speakActionConfirmation(`${d.prescription.action}. Run water pump for ${d.prescription.pump_minutes} minutes.`);
            showToast('Calculation Complete', `Run pump for ${d.prescription.pump_minutes} mins (${d.prescription.water_volume_liters} L).`, 'success');
            refreshTelemetry();
        } catch (e) {}
    }

    async function emergencyToggle() {
        if (!checkVisualRbacGuard('Emergency Stop')) return;
        const engaged = $('emergencyBtn').dataset.engaged === '1';
        try {
            const d = await api('/api/emergency', 'POST', { engage: !engaged });
            $('emergencyBtn').dataset.engaged = d.emergency_stop ? '1' : '';
            $('emergencyBtn').innerHTML = d.emergency_stop ? '🔄 RESUME NORMAL WATERING' : '⚠️ STOP ALL PUMPS';
            speakActionConfirmation(d.emergency_stop ? 'Emergency stop activated. All water pumps are now turned off.' : 'Normal automatic watering resumed.');
            showToast(d.emergency_stop ? 'ALL PUMPS STOPPED' : 'Normal Watering Resumed', d.active_action, d.emergency_stop ? 'error' : 'success');
            refreshTelemetry();
        } catch (e) {}
    }

    async function refreshHistorical() {
        const range = $('historicalRange').value;
        if (range === 'live') {
            refreshTelemetry();
            return;
        }
        try {
            const d = await api('/api/analytics/historical?range=' + range);
            telemetryChart.data.labels = d.labels.map(l => range === '24h' ? l.slice(11, 16) : l.slice(5, 10));
            telemetryChart.data.datasets[0].data = d.moisture;
            telemetryChart.data.datasets[1].data = d.temp;
            telemetryChart.data.datasets[2].data = d.humidity;
            telemetryChart.update('none');
        } catch (e) {}
    }

    function exportComplianceCSV() {
        speakActionConfirmation('Downloading your full farm report file.');
        showToast('Downloading Report', 'Saving your farm CSV report...', 'success');
        window.location.href = '/api/export/csv';
    }

    function bindSlider(id, labelId, unit) {
        const el = $(id);
        const lbl = $(labelId);
        if (!el || !lbl) return;
        el.addEventListener('input', () => {
            lbl.textContent = parseFloat(el.value).toFixed(1) + unit;
        });
    }

    window.addEventListener('load', () => {
        setInterval(() => {
            if ($('clock')) $('clock').textContent = new Date().toTimeString().slice(0, 8);
        }, 1000);

        initChart();
        bindSlider('sim_temp', 'sim_temp_label', ' °C');
        bindSlider('sim_humidity', 'sim_humidity_label', ' %');
        bindSlider('sim_moisture', 'sim_moisture_label', ' %');

        $('historicalRange').addEventListener('change', refreshHistorical);

        const saved = localStorage.getItem('aquasense_user');
        if (saved) {
            try {
                currentUser = JSON.parse(saved);
                if (currentUser && currentUser.contact_id) {
                    $('signInBtnText').textContent = `${currentUser.farmer_name} (${currentUser.contact_id})`;
                    if (currentUser.preferred_lang) setLanguage(currentUser.preferred_lang);
                }
            } catch (e) {}
        }

        refreshTelemetry();
        syncWeatherAPI(true);
        setInterval(refreshTelemetry, 5000);
    });
</script>

</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def root():
    return DASHBOARD_HTML


# ============================================================
# RUN SERVER
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)