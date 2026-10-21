# ============================================================
# AQUASENSE PRO v2.0 - 3D CYBER-COMMAND & AGRONOMIC AI PLATFORM
# FastAPI + SQLite + macOS 3D Glassmorphism + Cyberpunk Neon
# + ESP32 MQTT Bridge + Twilio SMS/WhatsApp Gateway + Yield AI
# + Multi-Tenant RBAC + Toast Engine + Field Mode + Ctrl+K Palette
# Save as MAIN1.PY and run: python MAIN1.PY
# ============================================================

from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, HTTPException, Query, Response, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

import sqlite3
import threading
import time
import random
import math
import io
import csv
import json
import hashlib
import urllib.request
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
        "provider": "Twilio / Meta WhatsApp Cloud API",
        "last_recipient": "+91-9876543210",
        "last_channel": "WHATSAPP + SMS",
        "last_message": "System armed and monitoring Udupi corridor.",
        "dispatched_count": 1,
        "last_sent": "Boot Baseline"
    }
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
# DATABASE & BLOCKCHAIN LEDGER HELPERS
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

    conn.commit()
    conn.close()


def ensure_config(conn):
    defaults = [
        ("crit_moisture_threshold", "30.0"),
        ("rain_override_threshold", "75.0"),
        ("maintenance_pump_minutes", "10"),
        ("pump_flow_lpm", "10.5")
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
        bio_rx = "Microclimate unfavorable for pathogen sporulation. Maintain standard drip."
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
            detail=f"RBAC Security Lock: 'Auditor' role is strictly Read-Only. Cannot execute '{action_label}'."
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
                    STATE["watchdog"] = "ALERT: Clogged Emitter / Pressure Spike"
                elif expected_flow > 0 and actual_flow > expected_flow * 1.30:
                    STATE["watchdog"] = "ALERT: Pipe Rupture / Pressure Drop"
                else:
                    STATE["watchdog"] = "NORMAL"

                if STATE["emergency_stop"]:
                    STATE["active_action"] = "EMERGENCY PUMP SHUTDOWN"
                elif avg_moisture < crit_threshold:
                    STATE["active_action"] = "Critical Irrigation Required"
                elif avg_moisture > rain_threshold or STATE["rain_probability"] > 80:
                    STATE["active_action"] = "Rain Override Active - Drainage Mode"
                else:
                    STATE["active_action"] = "Maintenance Drip Cycle"

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
                        f"AI Autonomous Rule: {STATE['active_action']}",
                        0.0,
                        0,
                        "AI Autonomous Kernel"
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


app = FastAPI(title="AquaSense Pro v2.0", version="2.0.0", lifespan=lifespan)

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
    alert_message: str = Field(default="CRITICAL ALERT: Pipe pressure anomaly / High pathogen risk detected in Udupi Zone A.")


# ============================================================
# API ENDPOINTS
# ============================================================

@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)


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

            config = {
                "crit_moisture_threshold": get_config(conn, "crit_moisture_threshold", "30.0"),
                "rain_override_threshold": get_config(conn, "rain_override_threshold", "75.0"),
                "maintenance_pump_minutes": get_config(conn, "maintenance_pump_minutes", "10"),
                "pump_flow_lpm": get_config(conn, "pump_flow_lpm", "10.5")
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
    require_write_permission(role, "ESP32 MQTT Hardware Publish")
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

        summary = f"MQTT Publish [{data.node_id}] -> {data.topic}: {data.payload} (QoS {data.qos})"
        with DB_LOCK:
            conn = get_db()
            block_hash = add_audit_and_block(conn, summary, 0.0, 0, f"MQTT Bridge ({role})")
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
    require_write_permission(role, "Twilio SMS / WhatsApp Emergency Alert")
    try:
        ts = utcnow()
        STATE["sms_gateway"]["last_recipient"] = data.recipient_phone
        STATE["sms_gateway"]["last_channel"] = data.channel
        STATE["sms_gateway"]["last_message"] = data.alert_message
        STATE["sms_gateway"]["dispatched_count"] += 1
        STATE["sms_gateway"]["last_sent"] = ts

        summary = f"{data.channel} Alert -> {data.recipient_phone}: {data.alert_message[:48]}..."
        with DB_LOCK:
            conn = get_db()
            block_hash = add_audit_and_block(conn, summary, 0.0, 0, f"Twilio Gateway ({role})")
            conn.commit()
            conn.close()

        return {
            "status": "sms_whatsapp_dispatched",
            "gateway": STATE["sms_gateway"]["provider"],
            "channel": data.channel,
            "recipient": data.recipient_phone,
            "message": data.alert_message,
            "dispatched_count": STATE["sms_gateway"]["dispatched_count"],
            "block_hash": block_hash,
            "timestamp": ts
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/drone/scan")
def api_drone_scan(role: str = Depends(verify_rbac)):
    try:
        generate_thermal_grid()
        STATE["drone"]["status"] = "AIRBORNE - THERMAL SWEEP COMPLETE"
        STATE["drone"]["altitude_m"] = 24.5
        STATE["drone"]["battery_pct"] = max(15, STATE["drone"]["battery_pct"] - random.randint(2, 5))
        STATE["drone"]["last_scan"] = utcnow()

        anomalies = STATE["drone"]["anomalies_found"]
        summary = f"UAV Thermal Sweep: {anomalies} micro-zone thermal/moisture anomalies mapped"

        with DB_LOCK:
            conn = get_db()
            add_audit_and_block(conn, summary, 0.0, 0, f"Autonomous UAV Scout ({role})")
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
            detail="RBAC Lock: 'Auditor' role cannot execute voice hardware mutations."
        )

    with DB_LOCK:
        conn = get_db()

        if "emergency" in cmd or "kill" in cmd or "stop all" in cmd:
            STATE["emergency_stop"] = True
            STATE["active_action"] = "EMERGENCY PUMP SHUTDOWN"
            for z in ZONES:
                STATE["pump_states"][z] = "FORCE_OFF"
            add_audit_and_block(conn, "AquaVoice Command: EMERGENCY KILL SWITCH ENGAGED", 0.0, 0, f"AquaVoice AI ({role})")
            reply = "Emergency kill switch engaged. All irrigation pumps have been forced off immediately."

        elif "reset" in cmd or "resume" in cmd or "auto" in cmd:
            STATE["emergency_stop"] = False
            STATE["active_action"] = "AI Autonomous Control Restored"
            for z in ZONES:
                STATE["pump_states"][z] = "AUTO"
            add_audit_and_block(conn, "AquaVoice Command: Returned to AI Autonomous Mode", 0.0, 0, f"AquaVoice AI ({role})")
            reply = "Emergency lock cleared. All farm zones returned to autonomous AI irrigation control."

        elif "zone a" in cmd and ("on" in cmd or "start" in cmd or "irrigate" in cmd):
            STATE["pump_states"]["Zone_A_Laterite"] = "FORCE_ON"
            add_audit_and_block(conn, "AquaVoice Command: Force ON Zone A Laterite", 105.0, 10, f"AquaVoice AI ({role})")
            reply = "Activating subsurface drip pumps for Zone A Laterite for 10 minutes."

        elif "zone b" in cmd and ("on" in cmd or "start" in cmd or "irrigate" in cmd):
            STATE["pump_states"]["Zone_B_Alluvial"] = "FORCE_ON"
            add_audit_and_block(conn, "AquaVoice Command: Force ON Zone B Alluvial", 105.0, 10, f"AquaVoice AI ({role})")
            reply = "Activating subsurface drip pumps for Zone B Alluvial for 10 minutes."

        elif "drone" in cmd or "scan" in cmd or "thermal" in cmd:
            generate_thermal_grid()
            STATE["drone"]["status"] = "AIRBORNE - VOICE DISPATCHED"
            STATE["drone"]["last_scan"] = utcnow()
            add_audit_and_block(conn, "AquaVoice Command: Dispatched UAV Thermal Scout", 0.0, 0, f"AquaVoice AI ({role})")
            reply = f"Thermal scouting drone launched. Scan complete with {STATE['drone']['anomalies_found']} localized thermal anomalies detected."

        elif "weather" in cmd or "sync" in cmd:
            reply = "Synchronizing live meteorological telemetry for Udyavar and Udupi corridor."

        else:
            avg_m = round((LIVE["Zone_A_Laterite"]["moisture"] + LIVE["Zone_B_Alluvial"]["moisture"]) / 2, 1)
            avg_t = round((LIVE["Zone_A_Laterite"]["temp"] + LIVE["Zone_B_Alluvial"]["temp"]) / 2, 1)
            reply = (
                f"AquaSense Pro status nominal for {STATE['selected_region']}. "
                f"Average soil moisture is {avg_m} percent, ambient temperature is {avg_t} degrees Celsius, "
                f"and current directive is {STATE['active_action']}."
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
    require_write_permission(role, "Region & Crop Profile Reconfiguration")
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
            yield_fc = compute_and_store_yield_forecast(conn, data.crop_type, avg_m, avg_t, avg_h)
            add_audit_and_block(
                conn,
                f"Configured Profile: {data.region} | Crop: {data.crop_type} (Est Yield: {yield_fc['projected_yield_tons']}t)",
                0.0,
                0,
                f"Location Manager ({role})"
            )
            conn.commit()
            conn.close()

        return {
            "status": "success",
            "selected_region": data.region,
            "selected_crop": data.crop_type,
            "yield_forecast": yield_fc,
            "message": "Successfully applied location, crop profile, and recalculated Yield AI"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/weather/sync")
def api_weather_sync(role: str = Depends(verify_rbac)):
    try:
        url = "https://api.open-meteo.com/v1/forecast?latitude=13.3409&longitude=74.7421&current=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation&timezone=auto"
        req = urllib.request.Request(url, headers={"User-Agent": "AquaSensePro-IoT/2.0"})

        with urllib.request.urlopen(req, timeout=4) as response:
            payload = json.loads(response.read().decode())
            current = payload.get("current", {})

            ext_temp = float(current.get("temperature_2m", 30.5))
            ext_hum = float(current.get("relative_humidity_2m", 72.0))
            ext_wind = float(current.get("wind_speed_10m", 2.4))
            ext_precip = float(current.get("precipitation", 0.0))

            rain_prob_calc = min(100.0, max(5.0, ext_precip * 45.0 + (100.0 - ext_hum) * 0.2))

            STATE["external_weather"] = {
                "source": "Open-Meteo Satellite API (Udyavar / Udupi)",
                "temp": ext_temp,
                "humidity": ext_hum,
                "wind_speed": ext_wind,
                "rain_prob": round(rain_prob_calc, 1),
                "last_synced": utcnow()
            }
            STATE["rain_probability"] = round(rain_prob_calc, 1)

            with DB_LOCK:
                conn = get_db()
                add_audit_and_block(conn, f"Weather Sync: {ext_temp}°C, {ext_hum}% RH (Udupi)", 0.0, 0, f"Open-Meteo API ({role})")
                conn.commit()
                conn.close()

            return {
                "status": "success",
                "message": "Synchronized live Udupi meteorological data",
                "weather": STATE["external_weather"]
            }

    except Exception as e:
        fallback_temp = round(random.uniform(28.5, 33.5), 1)
        fallback_hum = round(random.uniform(68.0, 84.0), 1)
        fallback_wind = round(random.uniform(1.8, 4.2), 1)

        STATE["external_weather"] = {
            "source": "Local Udyavar Micro-Station Fallback",
            "temp": fallback_temp,
            "humidity": fallback_hum,
            "wind_speed": fallback_wind,
            "rain_prob": 28.0,
            "last_synced": utcnow()
        }

        return {
            "status": "fallback",
            "message": f"Used local fallback station ({str(e)})",
            "weather": STATE["external_weather"]
        }


@app.post("/api/irrigation/override")
def api_irrigation_override(data: OverrideRequest, role: str = Depends(verify_rbac)):
    require_write_permission(role, "Actuator Valve Override")
    try:
        action = data.action.upper().strip()
        if action not in ("FORCE_ON", "FORCE_OFF", "AUTO"):
            raise HTTPException(status_code=400, detail="action must be FORCE_ON, FORCE_OFF, or AUTO")

        if STATE["emergency_stop"] and action == "FORCE_ON":
            raise HTTPException(status_code=400, detail="Emergency stop is active. Cannot force pump ON.")

        with DB_LOCK:
            conn = get_db()
            flow_rate = float(get_config(conn, "pump_flow_lpm", "10.5"))

            water_volume = 0.0
            duration = data.duration_mins

            if action == "FORCE_ON":
                water_volume = round(duration * flow_rate, 2)
                pump_action = f"{data.zone_id}: Subsurface Drip Active - Manual Force ON"
            elif action == "FORCE_OFF":
                duration = 0
                pump_action = f"{data.zone_id}: Pump Shutdown - Manual Force OFF"
            else:
                duration = 0
                pump_action = f"{data.zone_id}: Returned to AI Autonomous Control"

            add_audit_and_block(conn, pump_action, water_volume, duration, f"Manual Override ({role})")
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
            action = f"Critical Irrigation ({data.crop_type}) - Deficit Correction"
            severity = "critical"
            npk_advice = "Apply High Nitrogen + Potassium Booster"
        elif projected_moisture > rain_threshold:
            pump_minutes = 0
            action = f"Rain Override Active ({data.region}) - Drainage Mode"
            severity = "safe"
            npk_advice = "Hold Fertilization - Leaching Risk"
        else:
            pump_minutes = int(maintenance_minutes * multiplier)
            action = f"Maintenance Drip Cycle ({data.crop_type})"
            severity = "normal"
            npk_advice = "Balanced NPK Mix Recommended"

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
                f"Digital Twin [{data.crop_type}]: {action} | Yield Est: {yield_fc['projected_yield_tons']}t",
                water_volume,
                pump_minutes,
                f"Digital Twin ({role})"
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
            headers={"Content-Disposition": "attachment; filename=aquasense_pro_blockchain_export.csv"}
        )

    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.post("/api/fertigation/configure")
def api_fertigation_configure(data: FertigationRequest, role: str = Depends(verify_rbac)):
    require_write_permission(role, "NPK Fertigation Dosing")
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
                f"Fertigation Dosing ({data.growth_stage}): N={data.nitrogen_ppm} P={data.phosphorus_ppm} K={data.potassium_ppm}",
                0.0,
                0,
                f"Fertigation Engine ({role})"
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
    require_write_permission(role, "Master Emergency Kill Switch")
    try:
        with DB_LOCK:
            conn = get_db()

            if data.engage:
                STATE["emergency_stop"] = True
                STATE["active_action"] = "EMERGENCY PUMP SHUTDOWN"
                for zone in ZONES:
                    STATE["pump_states"][zone] = "FORCE_OFF"
                add_audit_and_block(conn, "EMERGENCY PUMP KILL SWITCH ENGAGED", 0.0, 0, f"Safety Operator ({role})")
            else:
                STATE["emergency_stop"] = False
                STATE["active_action"] = "System Restored - AI Autonomous Control"
                for zone in ZONES:
                    STATE["pump_states"][zone] = "AUTO"
                add_audit_and_block(conn, "Emergency Reset - Returning to Autonomous Control", 0.0, 0, f"Safety Operator ({role})")

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
# FRONTEND: MACOS 3D LIQUID GLASS & CYBERPUNK NEON COMMAND DECK
# ============================================================

DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AquaSense Pro v2.0 // macOS 3D Liquid Glass & Cyberpunk Neon Command Deck</title>

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
        --mood-primary: rgba(0, 240, 255, 0.22);
        --mood-secondary: rgba(255, 0, 127, 0.18);
        --mood-border: rgba(0, 240, 255, 0.42);
    }

    * { box-sizing: border-box; }

    body {
        margin: 0;
        background-color: #030408;
        color: #E2F1FF;
        font-family: 'Space Grotesk', 'Inter', system-ui, sans-serif;
        overflow-x: hidden;
        perspective: 1600px;
        cursor: default;
        transition: background-color 0.4s ease, color 0.4s ease;
    }

    /* CRT Holographic Scanlines Overlay (Ultra-subtle 2.5% opacity) */
    .scanline-overlay {
        position: fixed;
        inset: 0;
        pointer-events: none;
        z-index: 9990;
        background: repeating-linear-gradient(
            to bottom,
            transparent 0px,
            transparent 2px,
            rgba(0, 240, 255, 0.025) 3px,
            rgba(0, 0, 0, 0.06) 4px
        );
    }
    body.field-mode .scanline-overlay {
        display: none;
    }

    /* ==========================================================
       ☀️ OUTDOOR HIGH-CONTRAST FIELD MODE THEME OVERRIDES
       ========================================================== */
    body.field-mode {
        background-color: #F1F5F9 !important;
        color: #0F172A !important;
    }
    body.field-mode .ambient-orb {
        opacity: 0.14 !important;
        filter: blur(130px) !important;
    }
    body.field-mode .cyber-grid {
        background-image:
            linear-gradient(to right, rgba(15, 23, 42, 0.07) 1px, transparent 1px),
            linear-gradient(to bottom, rgba(2, 132, 199, 0.07) 1px, transparent 1px) !important;
    }
    body.field-mode .holo-card {
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.92) 0%, rgba(241, 245, 249, 0.88) 100%) !important;
        border-top: 1.5px solid rgba(255, 255, 255, 1) !important;
        border-left: 1.2px solid rgba(148, 163, 184, 0.5) !important;
        border-right: 1.2px solid rgba(2, 132, 199, 0.45) !important;
        border-bottom: 1.5px solid rgba(148, 163, 184, 0.6) !important;
        box-shadow:
            0 18px 38px -10px rgba(15, 23, 42, 0.14),
            0 4px 12px rgba(2, 132, 199, 0.08),
            inset 0 1px 2px rgba(255, 255, 255, 0.9) !important;
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
    body.field-mode .bg-black\/60,
    body.field-mode .bg-black\/85 {
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
    body.field-mode .iso-node {
        background: #FFFFFF !important;
        border: 1.5px solid #0284C7 !important;
        color: #0F172A !important;
        box-shadow: 0 10px 20px rgba(15, 23, 42, 0.12) !important;
    }
    body.field-mode .iso-node span.text-white { color: #0F172A !important; }
    body.field-mode .mac-tabs-dock {
        background: rgba(255, 255, 255, 0.92) !important;
        border: 1.5px solid #94A3B8 !important;
    }
    body.field-mode .mac-tab-btn { color: #334155 !important; }

    /* Ambient Chromatic Orbs passing behind Frosted Glass */
    .ambient-orb {
        position: fixed;
        border-radius: 50%;
        filter: blur(100px);
        opacity: 0.42;
        pointer-events: none;
        z-index: 0;
        animation: orbFloat 22s ease-in-out infinite alternate;
    }
    .orb-1 {
        width: 600px; height: 600px;
        top: -150px; left: -100px;
        background: radial-gradient(circle, #00F0FF 0%, #0044ff 70%, transparent 100%);
    }
    .orb-2 {
        width: 700px; height: 700px;
        top: 25%; right: -200px;
        background: radial-gradient(circle, #FF007F 0%, #7900ff 70%, transparent 100%);
        animation-delay: -7s;
    }
    .orb-3 {
        width: 550px; height: 550px;
        bottom: -150px; left: 30%;
        background: radial-gradient(circle, #39FF14 0%, #00F0FF 70%, transparent 100%);
        animation-delay: -14s;
    }
    @keyframes orbFloat {
        0% { transform: translate(0, 0) scale(1); }
        50% { transform: translate(60px, 80px) scale(1.12); }
        100% { transform: translate(-50px, 120px) scale(0.95); }
    }

    .cyber-grid {
        position: fixed;
        inset: -50%;
        width: 200%;
        height: 200%;
        z-index: 1;
        pointer-events: none;
        background-image:
            linear-gradient(to right, rgba(0, 240, 255, 0.08) 1px, transparent 1px),
            linear-gradient(to bottom, rgba(255, 0, 127, 0.06) 1px, transparent 1px);
        background-size: 54px 54px;
        transform: perspective(600px) rotateX(64deg) translateY(-120px) translateZ(-250px);
        animation: gridFlow 16s linear infinite;
    }

    @keyframes gridFlow {
        0% { transform: perspective(600px) rotateX(64deg) translateY(0px) translateZ(-250px); }
        100% { transform: perspective(600px) rotateX(64deg) translateY(54px) translateZ(-250px); }
    }

    #cursorTrailCanvas {
        position: fixed;
        inset: 0;
        width: 100vw;
        height: 100vh;
        pointer-events: none;
        z-index: 9999;
    }

    /* Custom Cybernetic Magnetic Reticle Pointer */
    #customPointer {
        position: fixed;
        width: 10px;
        height: 10px;
        background: #00F0FF;
        border-radius: 50%;
        pointer-events: none;
        z-index: 10000;
        box-shadow: 0 0 16px #00F0FF, 0 0 28px #00F0FF;
        transform: translate(-50%, -50%);
        transition: width 0.15s, height 0.15s, background-color 0.2s;
    }
    #customAura {
        position: fixed;
        width: 44px;
        height: 44px;
        border: 1.5px solid rgba(0, 240, 255, 0.65);
        border-radius: 50%;
        pointer-events: none;
        z-index: 9998;
        transform: translate(-50%, -50%);
        box-shadow: 0 0 20px rgba(0, 240, 255, 0.3), inset 0 0 10px rgba(255, 0, 127, 0.25);
        transition: transform 0.08s ease-out, border-color 0.2s, width 0.18s, height 0.18s;
    }

    /* AUTHENTIC macOS FROSTED LIQUID GLASS + TRUE 3D EXTRUSION */
    .holo-card {
        position: relative;
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.09) 0%, rgba(8, 14, 28, 0.62) 50%, rgba(5, 8, 18, 0.78) 100%);
        border-top: 1.5px solid rgba(255, 255, 255, 0.42);
        border-left: 1.2px solid rgba(255, 255, 255, 0.24);
        border-right: 1.2px solid rgba(0, 240, 255, 0.28);
        border-bottom: 1px solid rgba(0, 240, 255, 0.15);
        border-radius: 22px;
        padding: 20px;
        backdrop-filter: blur(35px) saturate(220%);
        -webkit-backdrop-filter: blur(35px) saturate(220%);
        box-shadow:
            0 28px 60px -15px rgba(0, 0, 0, 0.9),
            0 0 25px rgba(0, 240, 255, 0.12),
            inset 0 1px 2px rgba(255, 255, 255, 0.48),
            inset 0 -1px 2px rgba(0, 240, 255, 0.2);
        transform-style: preserve-3d;
        will-change: transform;
        overflow: hidden;
        z-index: 3;
    }

    /* Real-Time Specular Glare Reflection moving across 3D glass */
    .holo-card::after {
        content: "";
        position: absolute;
        inset: 0;
        background: radial-gradient(
            520px circle at var(--glare-x, 50%) var(--glare-y, 50%),
            rgba(255, 255, 255, 0.24) 0%,
            rgba(0, 240, 255, 0.12) 30%,
            transparent 65%
        );
        pointer-events: none;
        z-index: 1;
        opacity: var(--glare-opacity, 0);
        transition: opacity 0.3s ease;
        mix-blend-mode: overlay;
    }

    /* Staggered 3D Floating Layers inside the card */
    .layer-base {
        position: relative;
        z-index: 2;
        transform: translateZ(16px);
        transform-style: preserve-3d;
    }
    .layer-mid {
        transform: translateZ(32px);
        transform-style: preserve-3d;
    }
    .layer-high {
        transform: translateZ(52px);
        transform-style: preserve-3d;
    }

    /* macOS Window Traffic Light Header Bar */
    .mac-bar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding-bottom: 12px;
        margin-bottom: 14px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.12);
        transform: translateZ(28px);
    }
    .mac-dots {
        display: flex;
        align-items: center;
        gap: 7px;
    }
    .mac-dot {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        display: inline-block;
        box-shadow: inset 0 1px 2px rgba(255,255,255,0.45);
        cursor: pointer;
        transition: transform 0.18s cubic-bezier(0.34, 1.56, 0.64, 1);
    }
    .mac-dot:hover { transform: scale(1.18); }
    .mac-dot.red { background: #FF5F56; box-shadow: 0 0 10px rgba(255, 95, 86, 0.7); }
    .mac-dot.yellow { background: #FFBD2E; box-shadow: 0 0 10px rgba(255, 189, 46, 0.7); }
    .mac-dot.green { background: #27C93F; box-shadow: 0 0 10px rgba(39, 201, 63, 0.7); }

    /* macOS Frosted Glass Dock / Tab Switcher */
    .mac-tabs-dock {
        display: inline-flex;
        flex-wrap: wrap;
        gap: 6px;
        padding: 6px;
        border-radius: 18px;
        background: linear-gradient(135deg, rgba(255, 255, 255, 0.08) 0%, rgba(10, 16, 32, 0.75) 100%);
        border: 1.2px solid rgba(255, 255, 255, 0.22);
        backdrop-filter: blur(30px);
        box-shadow: 0 14px 36px rgba(0, 0, 0, 0.75), inset 0 1px 1.5px rgba(255, 255, 255, 0.35);
    }
    .mac-tab-btn {
        padding: 8px 18px;
        border-radius: 13px;
        font-size: 12px;
        font-weight: 700;
        color: #94a3b8;
        transition: all 0.25s cubic-bezier(0.34, 1.56, 0.64, 1);
        cursor: pointer;
        border: 1px solid transparent;
    }
    .mac-tab-btn:hover {
        color: #fff;
        background: rgba(0, 240, 255, 0.14);
        transform: translateY(-1px);
    }
    .mac-tab-btn.active {
        color: #030408;
        background: linear-gradient(135deg, #00F0FF, #39FF14);
        box-shadow: 0 0 25px rgba(0, 240, 255, 0.65), inset 0 1px 2px rgba(255,255,255,0.6);
        font-weight: 900;
        transform: translateY(-1.5px);
    }

    .mono { font-family: 'JetBrains Mono', monospace; }

    .neon-text-cyan { color: #00F0FF; text-shadow: 0 0 14px rgba(0, 240, 255, 0.75); }
    .neon-text-pink { color: #FF007F; text-shadow: 0 0 14px rgba(255, 0, 127, 0.75); }
    .neon-text-green { color: #39FF14; text-shadow: 0 0 14px rgba(57, 255, 20, 0.75); }
    .neon-text-yellow { color: #FFE600; text-shadow: 0 0 14px rgba(255, 230, 0, 0.75); }

    /* Sonar Pulsing Dot */
    .sonar-wrapper {
        position: relative;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 12px;
        height: 12px;
    }
    .sonar-dot {
        width: 9px;
        height: 9px;
        border-radius: 50%;
        background: #39FF14;
        box-shadow: 0 0 14px #39FF14;
        z-index: 2;
    }
    .sonar-ring {
        position: absolute;
        width: 100%;
        height: 100%;
        border-radius: 50%;
        border: 2px solid #00F0FF;
        animation: sonarWave 2s infinite ease-out;
    }
    @keyframes sonarWave {
        0% { transform: scale(0.8); opacity: 1; }
        100% { transform: scale(2.8); opacity: 0; }
    }

    /* 3D Liquid Shimmer Buttons */
    .btn-cyber {
        position: relative;
        overflow: hidden;
        border: 1px solid rgba(255, 255, 255, 0.28);
        border-radius: 14px;
        padding: 10px 18px;
        font-weight: 800;
        letter-spacing: 0.03em;
        cursor: pointer;
        transition: all 0.22s cubic-bezier(0.34, 1.56, 0.64, 1);
        display: inline-flex;
        align-items: center;
        justify-content: center;
        gap: 8px;
        transform: translateZ(28px);
        box-shadow: 0 8px 18px rgba(0,0,0,0.5), inset 0 1px 1px rgba(255,255,255,0.4);
    }
    .btn-cyber::after {
        content: "";
        position: absolute;
        top: -50%;
        left: -60%;
        width: 40%;
        height: 200%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.42), transparent);
        transform: rotate(25deg);
        transition: left 0.55s ease;
    }
    .btn-cyber:hover::after { left: 130%; }
    .btn-cyber:hover {
        transform: translateZ(40px) translateY(-3px) scale(1.03);
        box-shadow: 0 14px 30px -5px rgba(0, 240, 255, 0.65);
    }
    .btn-cyber:active { transform: translateZ(20px) translateY(0px) scale(0.98); }

    /* 🔒 VISUAL RBAC LOCKED BUTTON STATE */
    .rbac-locked {
        opacity: 0.42 !important;
        filter: grayscale(0.85) !important;
        cursor: not-allowed !important;
        border-color: rgba(255, 230, 0, 0.45) !important;
        box-shadow: none !important;
    }
    .rbac-locked:hover {
        transform: translateZ(28px) !important;
    }

    .btn-emerald { background: linear-gradient(135deg, #00F0FF, #39FF14); color: #030408; }
    .btn-cyan { background: linear-gradient(135deg, #00B4D8, #00F0FF); color: #030408; }
    .btn-purple { background: linear-gradient(135deg, #FF007F, #BC13FE); color: #fff; }
    .btn-danger {
        background: linear-gradient(135deg, #FF0055, #FF007F);
        color: #fff;
        box-shadow: 0 0 30px rgba(255, 0, 127, 0.55);
    }

    /* 3D Isometric Farm Node Topography Grid */
    .iso-scene {
        perspective: 1100px;
        padding: 18px 8px;
    }
    .iso-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 12px;
        transform: rotateX(25deg) rotateZ(-4deg);
        transform-style: preserve-3d;
        transition: transform 0.45s cubic-bezier(0.22, 1, 0.36, 1);
    }
    .iso-grid:hover {
        transform: rotateX(10deg) rotateZ(0deg);
    }
    .iso-node {
        position: relative;
        padding: 12px;
        border-radius: 14px;
        background: rgba(8, 14, 30, 0.88);
        border: 1.2px solid rgba(0, 240, 255, 0.35);
        box-shadow: 0 14px 28px rgba(0,0,0,0.7), inset 0 1px 1px rgba(255,255,255,0.25);
        transition: transform 0.25s ease, box-shadow 0.25s ease, border-color 0.25s ease;
        cursor: pointer;
        transform-style: preserve-3d;
    }
    .iso-node:hover {
        transform: translateZ(36px) translateY(-8px) scale(1.05);
        border-color: #00F0FF;
        box-shadow: 0 24px 45px rgba(0, 240, 255, 0.5), inset 0 1px 2px rgba(255,255,255,0.5);
    }
    .iso-node.stress {
        border-color: #FF007F;
        background: linear-gradient(135deg, rgba(255, 0, 127, 0.35), rgba(8, 14, 30, 0.92));
        animation: stressPulse 1.8s infinite;
    }
    .iso-node.saturated {
        border-color: #00F0FF;
        background: linear-gradient(135deg, rgba(0, 240, 255, 0.30), rgba(8, 14, 30, 0.92));
    }
    @keyframes stressPulse {
        0%, 100% { box-shadow: 0 0 14px rgba(255, 0, 127, 0.4); }
        50% { box-shadow: 0 0 35px rgba(255, 0, 127, 0.95); }
    }

    /* Drone Laser Sweep Animation */
    .laser-sweep {
        position: absolute;
        inset: 0;
        background: linear-gradient(180deg, transparent, rgba(0, 240, 255, 0.5), rgba(255, 0, 127, 0.75), transparent);
        height: 35%;
        width: 100%;
        pointer-events: none;
        opacity: 0;
        z-index: 20;
    }
    .laser-sweep.scanning {
        animation: radarSweep 1.8s ease-in-out 2;
    }
    @keyframes radarSweep {
        0% { top: -35%; opacity: 1; }
        100% { top: 100%; opacity: 0; }
    }

    /* Oscilloscope Grid Background behind Chart */
    .oscilloscope-screen {
        position: relative;
        border-radius: 16px;
        padding: 12px;
        background:
            linear-gradient(to right, rgba(0, 240, 255, 0.06) 1px, transparent 1px),
            linear-gradient(to bottom, rgba(0, 240, 255, 0.06) 1px, transparent 1px),
            rgba(3, 6, 16, 0.72);
        background-size: 28px 28px;
        border: 1px solid rgba(0, 240, 255, 0.25);
        box-shadow: inset 0 0 28px rgba(0, 240, 255, 0.08);
    }
    body.field-mode .oscilloscope-screen {
        background:
            linear-gradient(to right, rgba(2, 132, 199, 0.08) 1px, transparent 1px),
            linear-gradient(to bottom, rgba(2, 132, 199, 0.08) 1px, transparent 1px),
            #FFFFFF;
        border-color: #94A3B8;
    }

    /* Form Controls */
    .cyber-input, .cyber-select {
        width: 100%;
        background: rgba(4, 7, 18, 0.85);
        color: #E2F1FF;
        border: 1px solid rgba(0, 240, 255, 0.35);
        border-radius: 12px;
        padding: 10px 14px;
        font-size: 13px;
        outline: none;
        transition: border-color 0.2s, box-shadow 0.2s, transform 0.15s;
        transform: translateZ(24px);
    }
    .cyber-input:focus, .cyber-select:focus {
        border-color: #FF007F;
        box-shadow: 0 0 0 3px rgba(255, 0, 127, 0.3), inset 0 1px 1px rgba(255,255,255,0.2);
        transform: translateZ(30px);
    }

    input[type="range"] {
        accent-color: #00F0FF;
        cursor: pointer;
        transform: translateZ(20px);
    }

    /* 🔔 FLOATING TOAST NOTIFICATION SYSTEM */
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
        position: relative;
        overflow: hidden;
        padding: 14px 16px;
        border-radius: 16px;
        background: rgba(8, 13, 26, 0.92);
        backdrop-filter: blur(24px);
        border: 1.5px solid rgba(0, 240, 255, 0.5);
        color: #E2F1FF;
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.8), 0 0 20px rgba(0, 240, 255, 0.2);
        animation: toastSlideIn 0.32s cubic-bezier(0.22, 1, 0.36, 1) forwards;
    }
    body.field-mode .cyber-toast {
        background: rgba(255, 255, 255, 0.96);
        color: #0F172A;
        box-shadow: 0 16px 32px rgba(15, 23, 42, 0.22);
    }
    @keyframes toastSlideIn {
        from { opacity: 0; transform: translateX(60px) scale(0.95); }
        to { opacity: 1; transform: translateX(0) scale(1); }
    }
    .toast-progress {
        position: absolute;
        bottom: 0;
        left: 0;
        height: 3px;
        background: linear-gradient(90deg, #00F0FF, #39FF14);
        animation: toastProgress 4s linear forwards;
    }
    @keyframes toastProgress {
        from { width: 100%; }
        to { width: 0%; }
    }

    /* ⌨️ MODAL BACKDROP FOR COMMAND PALETTE (Ctrl+K) & HELP (?) */
    .cyber-modal-backdrop {
        position: fixed;
        inset: 0;
        z-index: 10020;
        background: rgba(3, 4, 8, 0.78);
        backdrop-filter: blur(14px);
        display: none;
        align-items: flex-start;
        justify-content: center;
        padding-top: 10vh;
    }
    .cyber-modal-backdrop.open {
        display: flex;
    }
    .cyber-modal-box {
        width: 92%;
        max-width: 620px;
        border-radius: 22px;
        background: linear-gradient(135deg, rgba(16, 24, 48, 0.95), rgba(6, 10, 22, 0.98));
        border: 1.5px solid rgba(0, 240, 255, 0.55);
        box-shadow: 0 30px 70px rgba(0, 0, 0, 0.9), 0 0 40px rgba(0, 240, 255, 0.22);
        overflow: hidden;
    }
    body.field-mode .cyber-modal-box {
        background: #FFFFFF;
        color: #0F172A;
        border-color: #0284C7;
    }

    /* Custom Scrollbar */
    ::-webkit-scrollbar { width: 7px; height: 7px; }
    ::-webkit-scrollbar-track { background: #030408; }
    ::-webkit-scrollbar-thumb { background: rgba(0, 240, 255, 0.4); border-radius: 99px; }
    ::-webkit-scrollbar-thumb:hover { background: #FF007F; }
</style>
</head>

<body>

<!-- Subtle CRT Scanlines Overlay -->
<div class="scanline-overlay"></div>

<!-- Chromatic Ambient Glow Background for True Frosted Glass Blur -->
<div class="ambient-orb orb-1"></div>
<div class="ambient-orb orb-2"></div>
<div class="ambient-orb orb-3"></div>
<div class="cyber-grid"></div>

<!-- Magnetic Cursor Elements -->
<div id="customPointer"></div>
<div id="customAura"></div>
<canvas id="cursorTrailCanvas"></canvas>

<!-- 🔔 Floating Toast Notification Container -->
<div id="toastContainer"></div>

<!-- ⌨️ COMMAND PALETTE MODAL (Ctrl+K) -->
<div id="cmdPaletteModal" class="cyber-modal-backdrop" onclick="if(event.target===this) closeModals()">
    <div class="cyber-modal-box p-5 space-y-4">
        <div class="flex items-center justify-between border-b border-slate-700/80 pb-3">
            <div class="flex items-center gap-2.5">
                <span class="text-lg">⌘</span>
                <span class="font-black tracking-wide neon-text-cyan mono text-sm">AQUASENSE COMMAND PALETTE</span>
            </div>
            <span class="text-[11px] mono px-2 py-0.5 rounded bg-slate-800 text-slate-300">ESC to close</span>
        </div>
        <input id="cmdSearchInput" oninput="filterCommandPalette()" class="cyber-input !transform-none" placeholder="Type a command (e.g. 'drone', 'mqtt', 'field mode', 'weather', 'kill')...">
        <div id="cmdPaletteList" class="space-y-1.5 max-h-[320px] overflow-y-auto pr-1">
            <!-- Populated dynamically via JS -->
        </div>
    </div>
</div>

<!-- ❓ KEYBOARD SHORTCUTS HELP MODAL (?) -->
<div id="shortcutsModal" class="cyber-modal-backdrop" onclick="if(event.target===this) closeModals()">
    <div class="cyber-modal-box p-6 space-y-4">
        <div class="flex items-center justify-between border-b border-slate-700/80 pb-3">
            <h3 class="text-base font-black neon-text-pink mono">⌨️ TACTICAL KEYBOARD SHORTCUTS</h3>
            <button onclick="closeModals()" class="text-xs mono px-2.5 py-1 rounded bg-slate-800 text-slate-300 hover:bg-slate-700">✕ Close</button>
        </div>
        <div class="grid grid-cols-1 sm:grid-cols-2 gap-2.5 text-xs mono">
            <div class="p-2.5 rounded-xl bg-black/60 border border-slate-800 flex justify-between"><span>Command Palette</span><kbd class="text-[#00F0FF] font-bold">Ctrl + K</kbd></div>
            <div class="p-2.5 rounded-xl bg-black/60 border border-slate-800 flex justify-between"><span>Shortcuts Cheat Sheet</span><kbd class="text-[#00F0FF] font-bold">?</kbd></div>
            <div class="p-2.5 rounded-xl bg-black/60 border border-slate-800 flex justify-between"><span>Toggle Field / Cyber Mode</span><kbd class="text-[#39FF14] font-bold">F</kbd></div>
            <div class="p-2.5 rounded-xl bg-black/60 border border-slate-800 flex justify-between"><span>Launch UAV Drone Scan</span><kbd class="text-[#39FF14] font-bold">D</kbd></div>
            <div class="p-2.5 rounded-xl bg-black/60 border border-slate-800 flex justify-between"><span>Sync Satellite Weather</span><kbd class="text-[#FFE600] font-bold">W</kbd></div>
            <div class="p-2.5 rounded-xl bg-black/60 border border-slate-800 flex justify-between"><span>Publish ESP32 MQTT</span><kbd class="text-[#FFE600] font-bold">M</kbd></div>
            <div class="p-2.5 rounded-xl bg-black/60 border border-slate-800 flex justify-between"><span>Activate AquaVoice AI</span><kbd class="text-[#FF007F] font-bold">V</kbd></div>
            <div class="p-2.5 rounded-xl bg-black/60 border border-slate-800 flex justify-between"><span>Close Any Modal</span><kbd class="text-[#FF007F] font-bold">Esc</kbd></div>
        </div>
    </div>
</div>

<div class="relative z-10 max-w-[1640px] mx-auto px-4 py-3 space-y-4">

    <!-- TOP MACOS GLASS COMMAND HEADER -->
    <header class="holo-card flex flex-wrap items-center justify-between gap-4">
        <div class="layer-base flex items-center gap-3.5">
            <div class="mac-dots mr-2">
                <span class="mac-dot red" onclick="emergencyToggle()" title="Emergency Kill Switch"></span>
                <span class="mac-dot yellow" onclick="toggleFieldMode()" title="Toggle Outdoor Field Mode"></span>
                <span class="mac-dot green" onclick="openCommandPalette()" title="Open Command Palette (Ctrl+K)"></span>
            </div>
            <div class="w-12 h-12 rounded-2xl bg-gradient-to-br from-[#00F0FF] via-[#BC13FE] to-[#FF007F] flex items-center justify-center text-2xl shadow-[0_0_30px_rgba(0,240,255,0.7)] layer-high">
                ⚡
            </div>
            <div>
                <div class="flex items-center gap-2.5">
                    <h1 class="text-xl font-black tracking-wider neon-text-cyan layer-mid">
                        AQUASENSE PRO <span class="neon-text-pink">v2.0</span>
                    </h1>
                    <span class="text-[10px] mono font-bold uppercase px-2.5 py-0.5 rounded-full bg-[#00F0FF]/15 text-[#00F0FF] border border-[#00F0FF]/40 layer-high">
                        macOS 3D LIQUID GLASS
                    </span>
                </div>
                <p class="text-xs text-slate-400 mono">Autonomous Agronomic AI · ESP32 MQTT · Twilio SMS · Yield AI · SHA-256</p>
            </div>
        </div>

        <div class="layer-base flex flex-wrap items-center gap-2">
            <!-- UPGRADE 4: RBAC ROLE SELECTOR -->
            <div class="flex items-center gap-2 px-3 py-1.5 rounded-full bg-black/70 border border-[#00F0FF]/40 text-xs mono layer-mid">
                <span class="text-slate-400">🔐 RBAC:</span>
                <select id="rbacRoleSelect" onchange="onRoleChange()" class="bg-transparent text-[#00F0FF] font-bold outline-none cursor-pointer">
                    <option value="Operator" class="bg-slate-950 text-[#00F0FF]" selected>Operator (Read/Write)</option>
                    <option value="Auditor" class="bg-slate-950 text-[#FFE600]">Auditor (Read-Only)</option>
                    <option value="Administrator" class="bg-slate-950 text-[#FF007F]">Administrator (Root)</option>
                </select>
            </div>

            <!-- ☀️ OUTDOOR FIELD MODE TOGGLE -->
            <button onclick="toggleFieldMode()" id="fieldModeBtn" class="px-3 py-1.5 rounded-full bg-black/70 border border-[#FFE600]/40 text-xs font-bold text-[#FFE600] hover:bg-slate-900 transition mono layer-mid">
                ☀️ Field Mode
            </button>

            <!-- ⌨️ COMMAND PALETTE & HELP BUTTONS -->
            <button onclick="openCommandPalette()" class="px-3 py-1.5 rounded-full bg-black/70 border border-[#00F0FF]/40 text-xs font-bold text-[#00F0FF] hover:bg-slate-900 transition mono layer-mid" title="Press Ctrl+K">
                ⌘K Palette
            </button>
            <button onclick="openShortcutsModal()" class="px-2.5 py-1.5 rounded-full bg-black/70 border border-slate-700 text-xs font-bold text-slate-300 hover:bg-slate-900 transition mono layer-mid" title="Keyboard Shortcuts (?)">
                ?
            </button>

            <div class="flex items-center gap-2 px-3 py-1.5 rounded-full bg-black/70 border border-slate-700/80 text-xs layer-mid">
                <span class="sonar-wrapper">
                    <span class="sonar-ring" id="sonarRing"></span>
                    <span class="sonar-dot" id="statusDot"></span>
                </span>
                <span id="connectionStatus" class="font-semibold text-[#39FF14] mono">Live Link</span>
            </div>

            <div class="px-3 py-1.5 rounded-full bg-black/70 border border-slate-700/80 text-xs mono text-[#00F0FF] layer-mid" id="clock">
                --:--:--
            </div>

            <button onclick="toggleAudioSfx()" id="sfxBtn" class="px-3 py-1.5 rounded-full bg-black/70 border border-[#00F0FF]/40 text-xs font-bold text-[#00F0FF] hover:bg-slate-900 transition mono layer-mid">
                🔊 SFX: ON
            </button>

            <button onclick="triggerVoiceAssistant()" id="voiceHeaderBtn" class="btn-cyber btn-purple text-xs py-2 px-3.5">
                🎙️ AquaVoice AI
            </button>

            <button onclick="emergencyToggle()" id="emergencyBtn" class="btn-cyber btn-danger rbac-write-btn text-xs py-2 px-4" data-original-label="⚠️ EMERGENCY KILL SWITCH">
                ⚠️ EMERGENCY KILL SWITCH
            </button>
        </div>
    </header>

    <!-- MACOS FROSTED GLASS TAB BAR & AUTO WEATHER FRESHNESS INDICATOR -->
    <div class="flex flex-wrap items-center justify-between gap-3">
        <div class="mac-tabs-dock">
            <button onclick="switchMacTab('all', this)" class="mac-tab-btn active">⌘ All Command Modules</button>
            <button onclick="switchMacTab('telemetry', this)" class="mac-tab-btn">🚁 3D UAV & Telemetry</button>
            <button onclick="switchMacTab('iot', this)" class="mac-tab-btn">📡 ESP32 MQTT & SMS Gateway</button>
            <button onclick="switchMacTab('ai', this)" class="mac-tab-btn">💹 Yield AI, FAO-56 & Ledger</button>
        </div>
        <div class="flex flex-wrap items-center gap-2.5">
            <!-- 🌐 AUTO WEATHER SYNC FRESHNESS INDICATOR -->
            <div onclick="syncWeatherAPI()" class="cursor-pointer text-xs mono px-3.5 py-2 rounded-2xl bg-black/70 border border-[#39FF14]/40 text-[#39FF14] hover:border-[#00F0FF] transition flex items-center gap-2" title="Click to force immediate satellite weather sync">
                <span>🌐</span>
                <span id="weatherFreshnessBadge">Weather Sync: Boot Baseline</span>
            </div>
            <div id="rbacStatusBanner" class="text-xs mono px-4 py-2 rounded-2xl bg-gradient-to-r from-[#00F0FF]/15 to-[#39FF14]/15 border border-[#00F0FF]/40 text-[#00F0FF] shadow-lg">
                Active Session: <b>Operator</b> · Full Hardware & Relay Actuation Enabled
            </div>
        </div>
    </div>

    <!-- 6-METRIC HOLOGRAPHIC KPI BAR (3D FLOATING CARDS) -->
    <section class="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
        <div class="holo-card border-l-4 border-l-[#FFE600]">
            <div class="layer-base">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400 mono">Ambient Temp</div>
                <div class="text-2xl font-black mono neon-text-yellow mt-1 layer-high" id="metric_temp">--</div>
                <div class="text-[11px] text-slate-400 mt-0.5">°C Microclimate</div>
            </div>
        </div>

        <div class="holo-card border-l-4 border-l-[#00F0FF]">
            <div class="layer-base">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400 mono">Rel. Humidity</div>
                <div class="text-2xl font-black mono neon-text-cyan mt-1 layer-high" id="metric_humidity">--</div>
                <div class="text-[11px] text-slate-400 mt-0.5">% Coastal Air</div>
            </div>
        </div>

        <div class="holo-card border-l-4 border-l-[#39FF14]">
            <div class="layer-base">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400 mono">Soil Moisture VWC</div>
                <div class="text-2xl font-black mono neon-text-green mt-1 layer-high" id="metric_moisture">--</div>
                <div class="text-[11px] text-slate-400 mt-0.5">% Volumetric</div>
            </div>
        </div>

        <div class="holo-card border-l-4 border-l-[#FF007F]">
            <div class="layer-base">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400 mono">Salinity / EC</div>
                <div class="text-2xl font-black mono neon-text-pink mt-1 layer-high" id="metric_ec">--</div>
                <div class="text-[11px] text-slate-400 mt-0.5">dS/m Conductivity</div>
            </div>
        </div>

        <div class="holo-card border-l-4 border-l-[#00F0FF]">
            <div class="layer-base">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400 mono">24h Rain Prob</div>
                <div class="text-2xl font-black mono neon-text-cyan mt-1 layer-high" id="metric_rain">--</div>
                <div class="text-[11px] text-slate-400 mt-0.5">% Satellite Model</div>
            </div>
        </div>

        <div class="holo-card border-l-4 border-l-[#39FF14]">
            <div class="layer-base">
                <div class="text-[11px] font-bold uppercase tracking-wider text-slate-400 mono">Flow Watchdog</div>
                <div class="text-lg font-black mono neon-text-green mt-1.5 layer-high" id="metric_flow">--</div>
                <div class="text-[11px] text-slate-400 truncate mt-0.5 mono" id="metric_watchdog">--</div>
            </div>
        </div>
    </section>

    <!-- MAIN COMMAND GRID -->
    <div class="grid grid-cols-1 lg:grid-cols-12 gap-4">

        <!-- LEFT COLUMN: CONTROLS & SANDBOX -->
        <div class="lg:col-span-4 space-y-4">

            <!-- 1. AQUAVOICE AI CONVERSATIONAL CONSOLE -->
            <div class="holo-card tab-section" data-tab="all telemetry ai">
                <div class="layer-base">
                    <div class="mac-bar">
                        <div class="mac-dots">
                            <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                            <span class="text-[10px] mono text-slate-300 ml-1">aquavoice://nlp-synth</span>
                        </div>
                        <span id="voiceStateBadge" class="text-[10px] mono px-2.5 py-0.5 rounded-full bg-[#FF007F]/20 text-[#FF007F] border border-[#FF007F]/50">
                            READY
                        </span>
                    </div>
                    <h2 class="text-base font-extrabold neon-text-pink flex items-center gap-2 layer-mid">
                        🎙️ AquaVoice AI Assistant
                    </h2>
                    <p class="text-xs text-slate-400 mt-1">Speak or click a voice directive for hands-free control.</p>

                    <div class="flex gap-2 mt-3">
                        <input id="voiceInputBox" class="cyber-input" placeholder="Say e.g. 'Scan drone' or 'Force on Zone A'...">
                        <button onclick="sendVoiceText()" class="btn-cyber btn-purple text-xs px-3">Send</button>
                    </div>

                    <div class="flex flex-wrap gap-1.5 mt-2.5">
                        <button onclick="quickVoice('System status report')" class="text-[11px] px-2.5 py-1 rounded-xl bg-black/60 hover:bg-[#FF007F]/30 border border-[#00F0FF]/40 text-[#00F0FF] transition mono layer-mid">📊 Status</button>
                        <button onclick="quickVoice('Launch drone thermal scan')" class="text-[11px] px-2.5 py-1 rounded-xl bg-black/60 hover:bg-[#FF007F]/30 border border-[#00F0FF]/40 text-[#00F0FF] transition mono layer-mid">🚁 Scan Drone</button>
                        <button onclick="quickVoice('Force on Zone A')" class="text-[11px] px-2.5 py-1 rounded-xl bg-black/60 hover:bg-[#FF007F]/30 border border-[#00F0FF]/40 text-[#00F0FF] transition mono layer-mid">💧 Irrigate Zone A</button>
                        <button onclick="quickVoice('Reset to auto mode')" class="text-[11px] px-2.5 py-1 rounded-xl bg-black/60 hover:bg-[#FF007F]/30 border border-[#00F0FF]/40 text-[#00F0FF] transition mono layer-mid">🤖 Auto Mode</button>
                    </div>

                    <div id="voiceReplyBox" class="mt-3 p-3 rounded-2xl bg-black/75 border border-[#FF007F]/40 text-xs text-pink-200 leading-relaxed mono shadow-inner layer-mid">
                        "Hello! Click 'AquaVoice AI' to speak into your microphone or choose a quick command chip."
                    </div>
                </div>
            </div>

            <!-- 2. REGIONAL, SOIL & WEATHER SYNC -->
            <div class="holo-card tab-section" data-tab="all ai telemetry">
                <div class="layer-base">
                    <div class="mac-bar">
                        <div class="mac-dots">
                            <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                            <span class="text-[10px] mono text-slate-300 ml-1">open-meteo://udupi-corridor</span>
                        </div>
                        <span class="text-[10px] mono text-[#00F0FF]">13.34°N 74.74°E</span>
                    </div>
                    <h2 class="text-base font-extrabold neon-text-cyan layer-mid">🌍 Agro-Climatic Region & Weather</h2>
                    <p class="text-xs text-slate-400 mt-0.5">Calibrated for Udyavar / Udupi Coastal Karnataka.</p>

                    <div class="mt-3 space-y-2.5">
                        <div>
                            <label class="text-[11px] font-bold uppercase text-slate-400 mono">Region / Soil Profile</label>
                            <select id="sel_region" class="cyber-select rbac-input mt-1">
                                <option value="Coastal Laterite Soil (Udyavar / Udupi Corridor)" selected>Coastal Laterite Soil (Udyavar / Udupi)</option>
                                <option value="Alluvial River Basin (Swarna River Belt)">Alluvial River Basin (Swarna Belt)</option>
                                <option value="Highland Western Ghats Loam">Highland Western Ghats Loam</option>
                                <option value="Arid Red Lateritic Plateau">Arid Red Lateritic Plateau</option>
                            </select>
                        </div>

                        <div>
                            <label class="text-[11px] font-bold uppercase text-slate-400 mono">Primary Crop Variety</label>
                            <select id="sel_crop" class="cyber-select rbac-input mt-1">
                                <option value="Paddy" selected>Paddy (Rice - High Water Demand)</option>
                                <option value="Arecanut">Arecanut (Coastal Plantation)</option>
                                <option value="Pepper">Black Pepper (Vines)</option>
                                <option value="Sugarcane">Sugarcane (Heavy Consumer)</option>
                                <option value="Vegetables (General)">Vegetables (General)</option>
                            </select>
                        </div>

                        <div class="grid grid-cols-2 gap-2 pt-1">
                            <button onclick="applyRegionAndCrop()" class="btn-cyber btn-emerald rbac-write-btn text-xs" data-original-label="Apply Profile">
                                Apply Profile
                            </button>
                            <button onclick="syncWeatherAPI()" class="btn-cyber btn-cyan text-xs">
                                ☁️ Sync Udupi API
                            </button>
                        </div>
                    </div>

                    <div id="weatherSyncOutput" class="mt-3 p-3 rounded-2xl bg-black/75 border border-[#00F0FF]/40 text-xs text-slate-300 leading-relaxed mono layer-mid">
                        Active Hub: <span class="text-[#39FF14] font-bold">Udyavar / Udupi</span> · Click Sync for live Open-Meteo telemetry.
                    </div>
                </div>
            </div>

            <!-- 3. DIGITAL TWIN SIMULATION SANDBOX -->
            <div class="holo-card tab-section" data-tab="all ai">
                <div class="layer-base">
                    <div class="mac-bar">
                        <div class="mac-dots">
                            <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                            <span class="text-[10px] mono text-slate-300 ml-1">fao56://penman-monteith</span>
                        </div>
                        <span class="text-[10px] mono text-[#39FF14]">3D SANDBOX</span>
                    </div>
                    <h2 class="text-base font-extrabold neon-text-green layer-mid">🧪 Digital Twin FAO-56 Simulator</h2>
                    <p class="text-xs text-slate-400 mt-0.5">Test extreme microclimate stress & AI prescriptions.</p>

                    <div class="mt-3 space-y-2.5 text-xs">
                        <div>
                            <div class="flex justify-between"><span class="text-slate-400">Temperature</span><span class="mono text-[#00F0FF] font-bold" id="sim_temp_label">34.0 °C</span></div>
                            <input type="range" id="sim_temp" min="15" max="50" step="0.5" value="34" class="w-full mt-1">
                        </div>
                        <div>
                            <div class="flex justify-between"><span class="text-slate-400">Humidity</span><span class="mono text-[#00F0FF] font-bold" id="sim_humidity_label">68.0 %</span></div>
                            <input type="range" id="sim_humidity" min="10" max="95" step="0.5" value="68" class="w-full mt-1">
                        </div>
                        <div>
                            <div class="flex justify-between"><span class="text-slate-400">Baseline Soil VWC</span><span class="mono text-[#00F0FF] font-bold" id="sim_moisture_label">36.0 %</span></div>
                            <input type="range" id="sim_moisture" min="10" max="90" step="0.5" value="36" class="w-full mt-1">
                        </div>
                        <div>
                            <div class="flex justify-between"><span class="text-slate-400">Wind Speed</span><span class="mono text-[#00F0FF] font-bold" id="sim_wind_label">2.4 m/s</span></div>
                            <input type="range" id="sim_wind" min="0" max="15" step="0.1" value="2.4" class="w-full mt-1">
                        </div>
                        <div>
                            <div class="flex justify-between"><span class="text-slate-400">Solar Radiation</span><span class="mono text-[#00F0FF] font-bold" id="sim_solar_label">18.5 MJ/m²/d</span></div>
                            <input type="range" id="sim_solar" min="2" max="35" step="0.5" value="18.5" class="w-full mt-1">
                        </div>
                        <div>
                            <label class="text-[11px] font-bold uppercase text-slate-400 mono">Crop Lifecycle Stage</label>
                            <select id="sim_stage" class="cyber-select mt-1">
                                <option value="initial">Initial Sprouting (Kc 0.40)</option>
                                <option value="development">Vegetative Development (Kc 0.70)</option>
                                <option value="mid_season" selected>Mid-Season Peak (Kc 1.15)</option>
                                <option value="late_season">Late Ripening (Kc 0.70)</option>
                            </select>
                        </div>
                    </div>

                    <button onclick="runSimulation()" class="btn-cyber btn-emerald w-full mt-3.5 text-xs">
                        ⚡ Run Predictive Digital Twin
                    </button>

                    <div id="simOutput" class="mt-3 p-3 rounded-2xl bg-black/75 border border-[#39FF14]/40 text-xs text-slate-300 leading-relaxed mono layer-mid">
                        Simulation output ready.
                    </div>
                </div>
            </div>

            <!-- 4. MANUAL OVERRIDE & NPK FERTIGATION -->
            <div class="holo-card tab-section" data-tab="all iot">
                <div class="layer-base">
                    <div class="mac-bar">
                        <div class="mac-dots">
                            <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                            <span class="text-[10px] mono text-slate-300 ml-1">actuators://solenoid-npk</span>
                        </div>
                        <span id="rbacCardBadge" class="text-[10px] mono text-[#FF007F]">RBAC GUARDED</span>
                    </div>
                    <h2 class="text-base font-extrabold neon-text-cyan layer-mid">🎛️ Actuator Override & NPK Dosing</h2>

                    <div class="grid grid-cols-2 gap-2 mt-3">
                        <div>
                            <label class="text-[10px] font-bold uppercase text-slate-400 mono">Target Zone</label>
                            <select id="override_zone" class="cyber-select rbac-input mt-1">
                                <option value="Zone_A_Laterite">Zone A (Laterite)</option>
                                <option value="Zone_B_Alluvial">Zone B (Alluvial)</option>
                            </select>
                        </div>
                        <div>
                            <label class="text-[10px] font-bold uppercase text-slate-400 mono">Relay Command</label>
                            <select id="override_action" class="cyber-select rbac-input mt-1">
                                <option value="FORCE_ON">FORCE ON</option>
                                <option value="FORCE_OFF">FORCE OFF</option>
                                <option value="AUTO">AI AUTO</option>
                            </select>
                        </div>
                    </div>

                    <div class="flex gap-2 mt-2 items-end">
                        <div class="flex-1">
                            <label class="text-[10px] font-bold uppercase text-slate-400 mono">Runtime (Mins)</label>
                            <input id="override_duration" type="number" value="10" min="1" max="240" class="cyber-input rbac-input mt-1">
                        </div>
                        <button onclick="sendOverride()" class="btn-cyber btn-cyan rbac-write-btn text-xs py-2.5" data-original-label="Execute Relay">
                            Execute Relay
                        </button>
                    </div>
                    <div id="overrideMsg" class="mt-2 text-xs text-[#00F0FF] mono"></div>

                    <hr class="border-slate-800 my-3">

                    <div class="grid grid-cols-4 gap-1.5">
                        <div class="col-span-1">
                            <label class="text-[10px] font-bold uppercase text-slate-400 mono">Stage</label>
                            <select id="fert_stage" class="cyber-select rbac-input mt-1 px-1.5">
                                <option value="Vegetative">Veg</option>
                                <option value="Flowering">Flower</option>
                                <option value="Ripening">Ripen</option>
                            </select>
                        </div>
                        <div>
                            <label class="text-[10px] font-bold uppercase text-slate-400 mono">N ppm</label>
                            <input id="fert_n" type="number" value="180" class="cyber-input rbac-input mt-1">
                        </div>
                        <div>
                            <label class="text-[10px] font-bold uppercase text-slate-400 mono">P ppm</label>
                            <input id="fert_p" type="number" value="60" class="cyber-input rbac-input mt-1">
                        </div>
                        <div>
                            <label class="text-[10px] font-bold uppercase text-slate-400 mono">K ppm</label>
                            <input id="fert_k" type="number" value="210" class="cyber-input rbac-input mt-1">
                        </div>
                    </div>
                    <button onclick="sendFertigation()" class="btn-cyber btn-emerald rbac-write-btn w-full mt-2.5 text-xs" data-original-label="🧪 Inject NPK Fertigation Profile">
                        🧪 Inject NPK Fertigation Profile
                    </button>
                    <div id="fertMsg" class="mt-2 text-xs text-[#39FF14] mono"></div>
                </div>
            </div>

        </div>

        <!-- RIGHT COLUMN: 3D TOPOGRAPHY, OSCILLOSCOPE TELEMETRY & ENTERPRISE INTELLIGENCE MODULES -->
        <div class="lg:col-span-8 space-y-4">

            <!-- 1. 3D ISOMETRIC FARM TOPOGRAPHY & DRONE THERMAL SCOUTING -->
            <div class="holo-card tab-section" data-tab="all telemetry">
                <div class="laser-sweep" id="droneLaser"></div>
                <div class="layer-base">
                    <div class="mac-bar">
                        <div class="mac-dots">
                            <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                            <span class="text-[10px] mono text-slate-300 ml-1">uav-scout://isometric-12node-grid</span>
                        </div>
                        <span id="droneStatusBadge" class="text-[11px] mono px-2.5 py-0.5 rounded-full bg-[#00F0FF]/15 text-[#00F0FF] border border-[#00F0FF]/40">
                            UAV: STANDBY
                        </span>
                    </div>

                    <div class="flex flex-wrap items-center justify-between gap-2">
                        <div>
                            <h2 class="text-base font-extrabold neon-text-cyan flex items-center gap-2 layer-mid">
                                🚁 3D Isometric Farm Topography & UAV Thermal Scouting
                            </h2>
                            <p class="text-xs text-slate-400">Live multispectral canopy temperature & volumetric moisture node matrix (Click any node to inspect)</p>
                        </div>
                        <button onclick="launchDroneScan()" class="btn-cyber btn-cyan text-xs">
                            🛰️ Launch Drone Thermal Sweep
                        </button>
                    </div>

                    <div class="iso-scene mt-2">
                        <div class="iso-grid" id="thermalGridContainer">
                            <!-- Populated dynamically via JS -->
                        </div>
                    </div>
                </div>
            </div>

            <!-- 2. 📊 LIVE OSCILLOSCOPE TELEMETRY CHART -->
            <div class="holo-card tab-section" data-tab="all telemetry">
                <div class="layer-base">
                    <div class="mac-bar">
                        <div class="mac-dots">
                            <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                            <span class="text-[10px] mono text-slate-300 ml-1">telemetry://oscilloscope-60pt-waveform</span>
                        </div>
                        <div class="flex items-center gap-2">
                            <span class="inline-block w-2 h-2 rounded-full bg-[#39FF14] animate-ping"></span>
                            <span class="text-[10px] mono text-[#39FF14]">OSCILLOSCOPE 60Hz LOCK</span>
                        </div>
                    </div>

                    <div class="flex flex-wrap items-center justify-between gap-2 mb-3">
                        <div>
                            <h2 class="text-base font-extrabold neon-text-pink layer-mid">📊 Live Oscilloscope Multi-Sensor Waveform</h2>
                            <p class="text-xs text-slate-400">Smooth real-time scrolling VWC, ambient temperature & coastal humidity oscilloscope</p>
                        </div>
                        <div class="flex items-center gap-2">
                            <select id="historicalRange" class="cyber-select !w-36 text-xs mono">
                                <option value="live" selected>⚡ Live Stream</option>
                                <option value="24h">Last 24 Hours</option>
                                <option value="7d">Last 7 Days</option>
                                <option value="30d">Last 30 Days</option>
                            </select>
                            <button onclick="refreshHistorical()" class="btn-cyber btn-cyan text-xs py-2 px-3">Refresh</button>
                            <button onclick="exportComplianceCSV()" class="btn-cyber btn-emerald text-xs py-2 px-3">📥 Export CSV</button>
                        </div>
                    </div>

                    <div class="oscilloscope-screen h-[290px] w-full layer-mid">
                        <canvas id="telemetryChart"></canvas>
                    </div>
                </div>
            </div>

            <!-- 3. ESP32 MQTT BRIDGE + TWILIO SMS/WHATSAPP + PREDICTIVE YIELD AI -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 tab-section" data-tab="all iot ai">

                <!-- ESP32 MQTT Broker & Twilio SMS/WhatsApp Emergency Gateway -->
                <div class="holo-card">
                    <div class="layer-base">
                        <div class="mac-bar">
                            <div class="mac-dots">
                                <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                                <span class="text-[10px] mono text-slate-300 ml-1">mqtts://esp32-lora-twilio</span>
                            </div>
                            <span class="text-[10px] mono px-2.5 py-0.5 rounded-full bg-[#00F0FF]/20 text-[#00F0FF] border border-[#00F0FF]/40" id="mqttBadge">
                                12 ESP32 NODES
                            </span>
                        </div>

                        <h2 class="text-base font-extrabold neon-text-cyan layer-mid">📡 ESP32 MQTT Bridge & WhatsApp Alert</h2>
                        <p class="text-xs text-slate-400 mt-0.5">Direct wireless field node propagation & emergency telecom dispatch</p>

                        <div class="grid grid-cols-2 gap-2 mt-3">
                            <div>
                                <label class="text-[10px] font-bold uppercase text-slate-400 mono">ESP32 Node</label>
                                <select id="mqtt_node" class="cyber-select rbac-input mt-1 text-xs mono">
                                    <option value="ESP32-NODE-A1">ESP32-NODE-A1 (Laterite)</option>
                                    <option value="ESP32-NODE-B1">ESP32-NODE-B1 (Alluvial)</option>
                                    <option value="LORA-GATEWAY-01">LORA-GATEWAY-01</option>
                                </select>
                            </div>
                            <div>
                                <label class="text-[10px] font-bold uppercase text-slate-400 mono">Hardware Payload</label>
                                <select id="mqtt_payload" class="cyber-select rbac-input mt-1 text-xs mono">
                                    <option value="SOLENOID_PULSE_ON">SOLENOID_PULSE_ON</option>
                                    <option value="SOLENOID_LOCK_OFF">SOLENOID_LOCK_OFF</option>
                                    <option value="CALIBRATE_CAPACITIVE_ADC">CALIBRATE_ADC</option>
                                </select>
                            </div>
                        </div>

                        <div class="flex gap-2 mt-2.5">
                            <button onclick="publishMQTTCommand()" class="btn-cyber btn-cyan rbac-write-btn flex-1 text-xs py-2" data-original-label="📡 Publish ESP32 MQTT">
                                📡 Publish ESP32 MQTT
                            </button>
                            <button onclick="dispatchSMSAlert()" class="btn-cyber btn-purple rbac-write-btn flex-1 text-xs py-2" data-original-label="💬 Send WhatsApp / SMS">
                                💬 Send WhatsApp / SMS
                            </button>
                        </div>

                        <div id="iotGatewayOutput" class="mt-3 p-3 rounded-2xl bg-black/75 border border-[#00F0FF]/40 text-[11px] mono text-slate-300 space-y-1 layer-mid">
                            <div>Broker: <span class="text-[#00F0FF]" id="mqttTopicText">aquasense/esp32/boot</span></div>
                            <div>Last ACK: <span class="text-[#39FF14]" id="mqttAckText">CLUSTER_ONLINE_ACK (14ms)</span></div>
                            <div>Twilio Gateway: <span class="text-[#FF007F]" id="smsStatusText">Armed (+91-9876543210)</span></div>
                        </div>
                    </div>
                </div>

                <!-- Predictive Harvest Yield & Market Revenue AI -->
                <div class="holo-card">
                    <div class="layer-base">
                        <div class="mac-bar">
                            <div class="mac-dots">
                                <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                                <span class="text-[10px] mono text-slate-300 ml-1">yield-ai://fao56-econometrics</span>
                            </div>
                            <span id="yieldConfBadge" class="text-[10px] mono px-2.5 py-0.5 rounded-full bg-[#39FF14]/20 text-[#39FF14] border border-[#39FF14]/40">
                                95.8% CONF
                            </span>
                        </div>

                        <h2 class="text-base font-extrabold neon-text-green layer-mid">💹 Predictive Yield & Revenue AI</h2>
                        <p class="text-xs text-slate-400 mt-0.5">Correlates real-time VWC & FAO-56 ET0 with harvest tonnage & INR price</p>

                        <div class="grid grid-cols-2 gap-2.5 mt-3">
                            <div class="p-3 rounded-2xl bg-black/75 border border-[#39FF14]/50 layer-mid">
                                <div class="text-[10px] uppercase text-slate-400 mono">Projected Harvest (5 Ha)</div>
                                <div class="text-xl font-black mono neon-text-green mt-1" id="yieldTons">-- Tons</div>
                                <div class="text-[10px] text-slate-400 mono mt-0.5" id="yieldCropLabel">Crop: Paddy</div>
                            </div>
                            <div class="p-3 rounded-2xl bg-black/75 border border-[#FF007F]/50 layer-mid">
                                <div class="text-[10px] uppercase text-slate-400 mono">Est. Market Revenue</div>
                                <div class="text-xl font-black mono neon-text-pink mt-1" id="yieldRevenue">₹--</div>
                                <div class="text-[10px] text-slate-400 mono mt-0.5" id="yieldUnitPrice">APMC Rate: ₹--/t</div>
                            </div>
                        </div>

                        <div class="mt-2.5 p-3 rounded-2xl bg-black/75 border border-slate-800 flex items-center justify-between text-xs mono layer-mid">
                            <span class="text-slate-400">Real-Time FAO-56 Reference ET0:</span>
                            <span class="text-[#FFE600] font-bold" id="yieldEt0">-- mm/day</span>
                        </div>
                    </div>
                </div>

            </div>

            <!-- 4. PREDICTIVE PEST ENGINE + CARBON & SOLAR OPTIMIZER -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 tab-section" data-tab="all ai telemetry">

                <!-- Predictive Pest & Disease Outbreak Warning Engine -->
                <div class="holo-card">
                    <div class="layer-base">
                        <div class="mac-bar">
                            <div class="mac-dots">
                                <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                                <span class="text-[10px] mono text-slate-300 ml-1">epidemiology://leaf-wetness</span>
                            </div>
                            <span id="pestBadge" class="text-[10px] mono font-bold px-2.5 py-0.5 rounded-full bg-[#39FF14]/20 text-[#39FF14] border border-[#39FF14]/40">
                                ANALYZING
                            </span>
                        </div>

                        <h2 class="text-base font-extrabold neon-text-pink layer-mid">🦠 Predictive Pathogen & Pest AI</h2>
                        <p class="text-xs text-slate-400 mt-0.5">Coastal humidity & leaf-wetness epidemiological model</p>

                        <div class="mt-3 p-3.5 rounded-2xl bg-black/75 border border-slate-800 space-y-2 text-xs layer-mid">
                            <div class="flex justify-between">
                                <span class="text-slate-400">Primary Monitored Threat:</span>
                                <span id="pestThreat" class="font-bold text-[#FFE600] text-right mono">--</span>
                            </div>
                            <div class="flex justify-between">
                                <span class="text-slate-400">Canopy Leaf Wetness Proxy:</span>
                                <span id="pestWetness" class="mono font-bold text-[#00F0FF]">--%</span>
                            </div>
                            <div class="flex justify-between">
                                <span class="text-slate-400">Outbreak Probability Index:</span>
                                <span id="pestConfidence" class="mono font-bold text-[#FF007F]">--%</span>
                            </div>
                            <div class="pt-1 border-t border-slate-800/80 text-[#39FF14] leading-relaxed mono" id="pestPrescription">
                                Evaluating microclimate vectors...
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Carbon Footprint & Solar Energy Optimization Calculator -->
                <div class="holo-card">
                    <div class="layer-base">
                        <div class="mac-bar">
                            <div class="mac-dots">
                                <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                                <span class="text-[10px] mono text-slate-300 ml-1">esg-telemetry://solar-carbon</span>
                            </div>
                            <span id="carbonGrade" class="text-[10px] mono font-bold px-2.5 py-0.5 rounded-full bg-[#39FF14]/20 text-[#39FF14] border border-[#39FF14]/40">
                                A+ GREEN
                            </span>
                        </div>

                        <h2 class="text-base font-extrabold neon-text-yellow layer-mid">⚡ Carbon & Solar Energy Optimizer</h2>
                        <p class="text-xs text-slate-400 mt-0.5">Real-time pump kWh, solar PV offset & CO₂ mitigation</p>

                        <div class="grid grid-cols-2 gap-2.5 mt-3">
                            <div class="p-3 rounded-2xl bg-black/75 border border-slate-800 layer-mid">
                                <div class="text-[10px] uppercase text-slate-400 mono">Water Conserved</div>
                                <div class="text-lg font-black mono neon-text-cyan mt-0.5" id="carbWater">-- L</div>
                            </div>
                            <div class="p-3 rounded-2xl bg-black/75 border border-slate-800 layer-mid">
                                <div class="text-[10px] uppercase text-slate-400 mono">CO₂ Emissions Saved</div>
                                <div class="text-lg font-black mono neon-text-green mt-0.5" id="carbCo2">-- kg</div>
                            </div>
                            <div class="p-3 rounded-2xl bg-black/75 border border-slate-800 layer-mid">
                                <div class="text-[10px] uppercase text-slate-400 mono">Pump Energy Used</div>
                                <div class="text-base font-bold mono neon-text-yellow mt-0.5" id="carbKwh">-- kWh</div>
                            </div>
                            <div class="p-3 rounded-2xl bg-black/75 border border-slate-800 layer-mid">
                                <div class="text-[10px] uppercase text-slate-400 mono">Solar PV Offset</div>
                                <div class="text-base font-bold mono neon-text-pink mt-0.5" id="carbSolar">84.0%</div>
                            </div>
                        </div>
                    </div>
                </div>

            </div>

            <!-- 5. AI ACTION PLAN & IMMUTABLE SHA-256 BLOCKCHAIN LEDGER (WITH CLICK-TO-COPY HASH) -->
            <div class="grid grid-cols-1 md:grid-cols-12 gap-4 tab-section" data-tab="all ai iot">

                <!-- AI Autonomous Action Plan (5 cols) -->
                <div class="holo-card md:col-span-5">
                    <div class="layer-base">
                        <div class="mac-bar">
                            <div class="mac-dots">
                                <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                                <span class="text-[10px] mono text-slate-300 ml-1">kernel://state-machine</span>
                            </div>
                            <span class="text-[10px] mono text-[#00F0FF]">CLOSED-LOOP</span>
                        </div>

                        <h2 class="text-base font-extrabold neon-text-cyan layer-mid">🧠 AI Autonomous Action Plan</h2>
                        <div id="activeAction" class="text-xl font-black neon-text-green mt-2 leading-tight mono layer-high">
                            Initializing AI Kernel...
                        </div>
                        <div id="aiPrescription" class="text-xs text-slate-300 mt-2 p-3 rounded-2xl bg-black/75 border border-slate-800 mono layer-mid">
                            Awaiting live sensor fusion...
                        </div>

                        <div class="mt-3 grid grid-cols-2 gap-2 text-xs">
                            <div class="p-3 rounded-2xl bg-black/75 border border-slate-800 layer-mid">
                                <div class="text-[10px] font-bold uppercase text-slate-400 mono">Actuator Relays</div>
                                <div id="pumpStates" class="mono text-[#00F0FF] mt-1 leading-relaxed">--</div>
                            </div>
                            <div class="p-3 rounded-2xl bg-black/75 border border-slate-800 layer-mid">
                                <div class="text-[10px] font-bold uppercase text-slate-400 mono">Active Profile</div>
                                <div id="configBox" class="mono text-[#39FF14] mt-1 leading-relaxed text-[11px]">--</div>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- SHA-256 Blockchain Compliance Ledger (7 cols) -->
                <div class="holo-card md:col-span-7">
                    <div class="layer-base">
                        <div class="mac-bar">
                            <div class="mac-dots">
                                <span class="mac-dot red"></span><span class="mac-dot yellow"></span><span class="mac-dot green"></span>
                                <span class="text-[10px] mono text-slate-300 ml-1">sha256://immutable-audit-chain</span>
                            </div>
                            <span class="text-[10px] mono px-2.5 py-0.5 rounded-full bg-[#39FF14]/20 text-[#39FF14] border border-[#39FF14]/40">
                                ✔ CHAIN VERIFIED · CLICK HASH TO COPY
                            </span>
                        </div>

                        <div class="flex items-center justify-between">
                            <div>
                                <h2 class="text-base font-extrabold neon-text-yellow layer-mid">⛓️ Immutable SHA-256 Compliance Ledger</h2>
                                <p class="text-xs text-slate-400">Cryptographically chained water, MQTT, SMS & RBAC audit trail</p>
                            </div>
                        </div>

                        <div id="blockchainList" class="mt-3 space-y-2 max-h-[240px] overflow-y-auto pr-1 layer-mid">
                            <!-- Populated dynamically -->
                        </div>
                    </div>
                </div>

            </div>

        </div>

    </div>

</div>

<script>
    const $ = (id) => document.getElementById(id);
    let telemetryChart = null;
    let sfxEnabled = true;
    let audioCtx = null;
    let fieldModeActive = false;
    let lastWeatherSyncEpoch = Date.now();

    /* ==========================================================
       🔔 1. FLOATING TOAST NOTIFICATION ENGINE
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
            <div class="toast-progress" style="background:${st.border}"></div>
        `;
        container.appendChild(toast);

        setTimeout(() => {
            if (toast && toast.parentNode) {
                toast.style.opacity = '0';
                toast.style.transform = 'translateX(40px)';
                toast.style.transition = 'all 0.25s ease';
                setTimeout(() => toast.remove(), 260);
            }
        }, 4000);
    }

    /* ==========================================================
       ☀️ 2. OUTDOOR FIELD MODE TOGGLE (HIGH-CONTRAST LIGHT THEME)
       ========================================================== */
    function toggleFieldMode() {
        playSfx('click');
        fieldModeActive = !fieldModeActive;
        document.body.classList.toggle('field-mode', fieldModeActive);
        $('fieldModeBtn').innerHTML = fieldModeActive ? '🌙 Cyber Mode' : '☀️ Field Mode';

        if (telemetryChart) {
            const tickColor = fieldModeActive ? '#1E293B' : '#64748b';
            const legendColor = fieldModeActive ? '#0F172A' : '#E2F1FF';
            telemetryChart.options.plugins.legend.labels.color = legendColor;
            telemetryChart.options.scales.x.ticks.color = tickColor;
            telemetryChart.options.scales.y.ticks.color = tickColor;
            telemetryChart.update('none');
        }

        showToast(
            fieldModeActive ? 'Outdoor Field Mode Active' : 'Cyberpunk Neon Mode Active',
            fieldModeActive
                ? 'Switched to high-contrast daylight theme for outdoor agricultural tablet readability.'
                : 'Restored 3D holographic dark glassmorphism & neon command aesthetic.',
            'info'
        );
    }

    /* ==========================================================
       🔒 3. VISUAL RBAC LOCKING & MACOS GLASS TAB SWITCHER
       ========================================================== */
    function switchMacTab(tabName, btnEl) {
        playSfx('click');
        document.querySelectorAll('.mac-tab-btn').forEach(b => b.classList.remove('active'));
        if (btnEl) btnEl.classList.add('active');

        document.querySelectorAll('.tab-section').forEach(sec => {
            const tags = (sec.dataset.tab || 'all').split(' ');
            if (tabName === 'all' || tags.includes(tabName)) {
                sec.style.display = '';
            } else {
                sec.style.display = 'none';
            }
        });
        showToast('Workspace View Switched', `Displaying module group: ${tabName.toUpperCase()}`, 'info');
    }

    function getActiveRole() {
        return $('rbacRoleSelect') ? $('rbacRoleSelect').value : 'Operator';
    }

    function onRoleChange() {
        playSfx('click');
        const role = getActiveRole();
        const banner = $('rbacStatusBanner');
        const isAuditor = (role === 'Auditor');

        // Visually lock/unlock all write buttons & inputs
        document.querySelectorAll('.rbac-write-btn').forEach(btn => {
            const orig = btn.getAttribute('data-original-label') || btn.textContent;
            if (isAuditor) {
                btn.classList.add('rbac-locked');
                btn.innerHTML = `🔒 ${orig.replace(/^🔒\s*/, '')}`;
            } else {
                btn.classList.remove('rbac-locked');
                btn.innerHTML = orig;
            }
        });

        document.querySelectorAll('.rbac-input').forEach(inp => {
            inp.disabled = isAuditor;
            inp.style.opacity = isAuditor ? '0.5' : '1';
        });

        if ($('rbacCardBadge')) {
            $('rbacCardBadge').textContent = isAuditor ? '🔒 READ-ONLY LOCKED' : 'RBAC AUTHORIZED';
            $('rbacCardBadge').className = isAuditor ? 'text-[10px] mono text-[#FFE600] font-bold' : 'text-[10px] mono text-[#39FF14]';
        }

        if (role === 'Auditor') {
            banner.className = 'text-xs mono px-4 py-2 rounded-2xl bg-[#FFE600]/15 border border-[#FFE600]/60 text-[#FFE600] shadow-lg';
            banner.innerHTML = 'Active Session: <b>Auditor (Read-Only Compliance Mode)</b> · All Hardware Overrides Visually Locked';
            showToast('RBAC Read-Only Lock Engaged', 'Switched to Government Auditor role. All hardware mutation buttons are now locked.', 'warning');
        } else if (role === 'Administrator') {
            banner.className = 'text-xs mono px-4 py-2 rounded-2xl bg-[#FF007F]/15 border border-[#FF007F]/60 text-[#FF007F] shadow-lg';
            banner.innerHTML = 'Active Session: <b>Administrator (Root Kernel Access)</b> · Full Multi-Tenant & Security Override Authority';
            showToast('RBAC Root Administrator', 'Full multi-tenant hardware & configuration privileges unlocked.', 'success');
        } else {
            banner.className = 'text-xs mono px-4 py-2 rounded-2xl bg-gradient-to-r from-[#00F0FF]/15 to-[#39FF14]/15 border border-[#00F0FF]/40 text-[#00F0FF] shadow-lg';
            banner.innerHTML = 'Active Session: <b>Operator</b> · Full Hardware & Relay Actuation Enabled';
            showToast('RBAC Operator Active', 'Standard field operator write privileges enabled.', 'info');
        }
    }

    function checkVisualRbacGuard(actionName) {
        if (getActiveRole() === 'Auditor') {
            playSfx('alert');
            showToast('RBAC Security Lock (HTTP 403)', `'Auditor' role is strictly Read-Only. Cannot execute ${actionName}. Switch role to Operator or Administrator.`, 'warning');
            return false;
        }
        return true;
    }

    /* ==========================================================
       📋 4. CLICK-TO-COPY SHA-256 BLOCKCHAIN HASH
       ========================================================== */
    async function copyBlockHash(blockId, fullHash) {
        playSfx('click');
        try {
            await navigator.clipboard.writeText(fullHash);
            showToast(`Copied Block #${blockId} SHA-256`, `Full hash (${fullHash.slice(0, 20)}...) copied to clipboard!`, 'success');
        } catch (err) {
            // Fallback copy method
            const ta = document.createElement('textarea');
            ta.value = fullHash;
            document.body.appendChild(ta);
            ta.select();
            document.execCommand('copy');
            ta.remove();
            showToast(`Copied Block #${blockId} SHA-256`, `Full hash (${fullHash.slice(0, 20)}...) copied to clipboard!`, 'success');
        }
    }

    /* ==========================================================
       🌐 5. AUTO WEATHER SYNC FRESHNESS INDICATOR
       ========================================================== */
    function updateWeatherFreshnessCounter() {
        const elapsedSec = Math.max(0, Math.floor((Date.now() - lastWeatherSyncEpoch) / 1000));
        const badge = $('weatherFreshnessBadge');
        if (!badge) return;
        if (elapsedSec < 60) {
            badge.textContent = `Weather Synced: ${elapsedSec}s ago (Auto-Poll Active)`;
        } else {
            const mins = Math.floor(elapsedSec / 60);
            badge.textContent = `Weather Synced: ${mins}m ${elapsedSec % 60}s ago`;
        }
    }

    /* ==========================================================
       ⌨️ 6. COMMAND PALETTE (Ctrl+K) & KEYBOARD SHORTCUTS (?)
       ========================================================== */
    const COMMAND_LIST = [
        { title: '🚁 Launch UAV Thermal Drone Sweep', desc: 'Scans 12-node isometric farm matrix for canopy stress', run: () => launchDroneScan() },
        { title: '📡 Publish ESP32 MQTT Solenoid Command', desc: 'Dispatches wireless QoS-1 packet to ESP32 field node', run: () => publishMQTTCommand() },
        { title: '💬 Dispatch WhatsApp & Twilio SMS Alert', desc: 'Sends emergency mobile notification to farm operator', run: () => dispatchSMSAlert() },
        { title: '☁️ Sync Open-Meteo Satellite Weather (Udupi)', desc: 'Pulls live temperature, humidity & rain probability', run: () => syncWeatherAPI() },
        { title: '☀️ Toggle Outdoor Field Mode / Cyber Mode', desc: 'Switches between high-contrast daylight and neon dark UI', run: () => toggleFieldMode() },
        { title: '⚡ Run FAO-56 Digital Twin Simulation', desc: 'Computes Penman-Monteith ET0, Kc & harvest yield in INR', run: () => runSimulation() },
        { title: '🔒 Switch RBAC to Auditor (Read-Only Lock)', desc: 'Engages visual RBAC button locking for compliance demo', run: () => { $('rbacRoleSelect').value = 'Auditor'; onRoleChange(); } },
        { title: '🔓 Switch RBAC to Operator (Read/Write)', desc: 'Restores full actuator & hardware write permissions', run: () => { $('rbacRoleSelect').value = 'Operator'; onRoleChange(); } },
        { title: '📥 Export SHA-256 Blockchain Compliance CSV', desc: 'Downloads full cryptographic audit & sensor log', run: () => exportComplianceCSV() },
        { title: '⚠️ Toggle Master Emergency Kill Switch', desc: 'Engages or resets immediate all-zone pump shutdown', run: () => emergencyToggle() }
    ];

    function openCommandPalette() {
        playSfx('click');
        $('cmdPaletteModal').classList.add('open');
        $('cmdSearchInput').value = '';
        filterCommandPalette();
        setTimeout(() => $('cmdSearchInput').focus(), 50);
    }

    function openShortcutsModal() {
        playSfx('click');
        $('shortcutsModal').classList.add('open');
    }

    function closeModals() {
        $('cmdPaletteModal').classList.remove('open');
        $('shortcutsModal').classList.remove('open');
    }

    function filterCommandPalette() {
        const q = ($('cmdSearchInput').value || '').toLowerCase().trim();
        const filtered = COMMAND_LIST.filter(c => c.title.toLowerCase().includes(q) || c.desc.toLowerCase().includes(q));
        $('cmdPaletteList').innerHTML = filtered.map((cmd, idx) => `
            <div onclick="executePaletteCommand(${COMMAND_LIST.indexOf(cmd)})" class="p-3 rounded-xl bg-black/60 hover:bg-[#00F0FF]/20 border border-slate-800 hover:border-[#00F0FF] cursor-pointer transition flex items-center justify-between">
                <div>
                    <div class="text-xs font-bold text-white mono">${cmd.title}</div>
                    <div class="text-[11px] text-slate-400">${cmd.desc}</div>
                </div>
                <span class="text-[10px] mono px-2 py-0.5 rounded bg-[#00F0FF]/15 text-[#00F0FF]">RUN ↵</span>
            </div>
        `).join('') || '<div class="text-xs text-slate-400 p-3 mono">No matching commands found.</div>';
    }

    function executePaletteCommand(index) {
        closeModals();
        if (COMMAND_LIST[index]) COMMAND_LIST[index].run();
    }

    window.addEventListener('keydown', (e) => {
        const tag = (e.target && e.target.tagName) ? e.target.tagName.toLowerCase() : '';
        const isTyping = (tag === 'input' || tag === 'textarea' || tag === 'select');

        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
            e.preventDefault();
            openCommandPalette();
            return;
        }
        if (e.key === 'Escape') {
            closeModals();
            return;
        }
        if (isTyping) return;

        if (e.key === '?') {
            e.preventDefault();
            openShortcutsModal();
        } else if (e.key.toLowerCase() === 'f') {
            e.preventDefault();
            toggleFieldMode();
        } else if (e.key.toLowerCase() === 'd') {
            e.preventDefault();
            launchDroneScan();
        } else if (e.key.toLowerCase() === 'w') {
            e.preventDefault();
            syncWeatherAPI();
        } else if (e.key.toLowerCase() === 'm') {
            e.preventDefault();
            publishMQTTCommand();
        } else if (e.key.toLowerCase() === 'v') {
            e.preventDefault();
            triggerVoiceAssistant();
        }
    });

    /* ==========================================================
       7. SCI-FI WEB AUDIO SYNTHESIZER
       ========================================================== */
    function playSfx(type = 'click') {
        if (!sfxEnabled) return;
        try {
            if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();
            osc.connect(gain);
            gain.connect(audioCtx.destination);

            const now = audioCtx.currentTime;
            if (type === 'click') {
                osc.type = 'sine';
                osc.frequency.setValueAtTime(680, now);
                osc.frequency.exponentialRampToValueAtTime(1120, now + 0.06);
                gain.gain.setValueAtTime(0.06, now);
                gain.gain.exponentialRampToValueAtTime(0.001, now + 0.07);
                osc.start(now);
                osc.stop(now + 0.07);
            } else if (type === 'scan') {
                osc.type = 'triangle';
                osc.frequency.setValueAtTime(320, now);
                osc.frequency.exponentialRampToValueAtTime(980, now + 0.35);
                gain.gain.setValueAtTime(0.08, now);
                gain.gain.exponentialRampToValueAtTime(0.001, now + 0.38);
                osc.start(now);
                osc.stop(now + 0.38);
            } else if (type === 'alert') {
                osc.type = 'sawtooth';
                osc.frequency.setValueAtTime(240, now);
                osc.frequency.setValueAtTime(180, now + 0.12);
                gain.gain.setValueAtTime(0.09, now);
                gain.gain.exponentialRampToValueAtTime(0.001, now + 0.28);
                osc.start(now);
                osc.stop(now + 0.28);
            }
        } catch (e) {}
    }

    function toggleAudioSfx() {
        sfxEnabled = !sfxEnabled;
        $('sfxBtn').textContent = sfxEnabled ? '🔊 SFX: ON' : '🔇 SFX: OFF';
        if (sfxEnabled) playSfx('click');
        showToast('Web Audio Synthesizer', sfxEnabled ? 'Procedural sci-fi audio feedback enabled.' : 'Audio feedback muted.', 'info');
    }

    /* ==========================================================
       8. SMOOTH 3D PERSPECTIVE INERTIAL TILT & SPECULAR GLARE
       ========================================================== */
    function init3DPhysicsCards() {
        document.querySelectorAll('.holo-card').forEach(card => {
            let targetRotX = 0, targetRotY = 0;
            let currentRotX = 0, currentRotY = 0;
            let rafId = null;

            function updateTilt() {
                currentRotX += (targetRotX - currentRotX) * 0.14;
                currentRotY += (targetRotY - currentRotY) * 0.14;

                card.style.transform = `perspective(1200px) rotateX(${currentRotX}deg) rotateY(${currentRotY}deg) translateY(-4px) scale3d(1.01, 1.01, 1.01)`;

                if (Math.abs(targetRotX - currentRotX) > 0.01 || Math.abs(targetRotY - currentRotY) > 0.01) {
                    rafId = requestAnimationFrame(updateTilt);
                }
            }

            card.addEventListener('mousemove', (e) => {
                const rect = card.getBoundingClientRect();
                const x = e.clientX - rect.left;
                const y = e.clientY - rect.top;

                const xc = rect.width / 2;
                const yc = rect.height / 2;
                const dx = (x - xc) / xc;
                const dy = (y - yc) / yc;

                targetRotY = dx * 9.5;
                targetRotX = -dy * 9.5;

                card.style.setProperty('--glare-x', `${(x / rect.width) * 100}%`);
                card.style.setProperty('--glare-y', `${(y / rect.height) * 100}%`);
                card.style.setProperty('--glare-opacity', '1');

                cancelAnimationFrame(rafId);
                rafId = requestAnimationFrame(updateTilt);
            });

            card.addEventListener('mouseleave', () => {
                targetRotX = 0;
                targetRotY = 0;
                card.style.setProperty('--glare-opacity', '0');
                cancelAnimationFrame(rafId);
                rafId = requestAnimationFrame(updateTilt);
            });
        });
    }

    /* ==========================================================
       9. HIGH-PRECISION DUAL-RING RETICLE + PARTICLE SPARK TRAIL
       ========================================================== */
    const canvas = $('cursorTrailCanvas');
    const ctx = canvas.getContext('2d');
    const pointer = $('customPointer');
    const aura = $('customAura');

    let mouseX = window.innerWidth / 2, mouseY = window.innerHeight / 2;
    let auraX = mouseX, auraY = mouseY;
    let particles = [];

    function resizeCanvas() {
        canvas.width = window.innerWidth;
        canvas.height = window.innerHeight;
    }
    window.addEventListener('resize', resizeCanvas);
    resizeCanvas();

    const neonPalette = ['#00F0FF', '#FF007F', '#39FF14', '#FFE600', '#BC13FE'];

    window.addEventListener('mousemove', (e) => {
        mouseX = e.clientX;
        mouseY = e.clientY;

        pointer.style.left = `${mouseX}px`;
        pointer.style.top = `${mouseY}px`;

        for (let i = 0; i < 3; i++) {
            particles.push({
                x: mouseX,
                y: mouseY,
                size: Math.random() * 3.2 + 1.2,
                speedX: (Math.random() - 0.5) * 2.8,
                speedY: (Math.random() - 0.5) * 2.8,
                life: 1.0,
                color: neonPalette[Math.floor(Math.random() * neonPalette.length)]
            });
        }
    });

    document.addEventListener('mouseover', (e) => {
        if (e.target.closest('button, select, input, .iso-node, .mac-dot')) {
            aura.style.width = '64px';
            aura.style.height = '64px';
            aura.style.borderColor = '#FF007F';
            pointer.style.backgroundColor = '#FF007F';
        } else {
            aura.style.width = '44px';
            aura.style.height = '44px';
            aura.style.borderColor = 'rgba(0, 240, 255, 0.65)';
            pointer.style.backgroundColor = '#00F0FF';
        }
    });

    function animateCursorParticles() {
        auraX += (mouseX - auraX) * 0.22;
        auraY += (mouseY - auraY) * 0.22;
        aura.style.left = `${auraX}px`;
        aura.style.top = `${auraY}px`;

        ctx.clearRect(0, 0, canvas.width, canvas.height);
        for (let i = particles.length - 1; i >= 0; i--) {
            const p = particles[i];
            p.x += p.speedX;
            p.y += p.speedY;
            p.life -= 0.038;
            if (p.life <= 0) {
                particles.splice(i, 1);
            } else {
                ctx.fillStyle = p.color;
                ctx.globalAlpha = p.life * 0.85;
                ctx.beginPath();
                ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
                ctx.fill();
            }
        }
        ctx.globalAlpha = 1.0;
        requestAnimationFrame(animateCursorParticles);
    }
    requestAnimationFrame(animateCursorParticles);

    /* ==========================================================
       10. API HELPER & OSCILLOSCOPE CHART.JS INITIALIZATION
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

        const gradCyan = c.createLinearGradient(0, 0, 0, 280);
        gradCyan.addColorStop(0, 'rgba(0, 240, 255, 0.45)');
        gradCyan.addColorStop(1, 'rgba(0, 240, 255, 0.0)');

        const gradPink = c.createLinearGradient(0, 0, 0, 280);
        gradPink.addColorStop(0, 'rgba(255, 0, 127, 0.35)');
        gradPink.addColorStop(1, 'rgba(255, 0, 127, 0.0)');

        telemetryChart = new Chart(c, {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: 'Soil Moisture VWC %',
                        data: [],
                        borderColor: '#00F0FF',
                        backgroundColor: gradCyan,
                        borderWidth: 2.8,
                        fill: true,
                        tension: 0.42,
                        pointRadius: 2,
                        pointHoverRadius: 5,
                        pointBackgroundColor: '#00F0FF'
                    },
                    {
                        label: 'Temperature °C',
                        data: [],
                        borderColor: '#FFE600',
                        borderWidth: 2.2,
                        tension: 0.42,
                        pointRadius: 1.5,
                        pointHoverRadius: 5,
                        pointBackgroundColor: '#FFE600'
                    },
                    {
                        label: 'Humidity %',
                        data: [],
                        borderColor: '#FF007F',
                        backgroundColor: gradPink,
                        borderWidth: 2.2,
                        fill: true,
                        tension: 0.42,
                        pointRadius: 1.5,
                        pointHoverRadius: 5,
                        pointBackgroundColor: '#FF007F'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 320, easing: 'easeOutQuad' },
                plugins: {
                    legend: { labels: { color: '#E2F1FF', font: { family: 'JetBrains Mono', weight: '700', size: 11 } } }
                },
                scales: {
                    x: { grid: { display: false }, ticks: { color: '#64748b', maxTicksLimit: 10, font: { family: 'JetBrains Mono', size: 10 } } },
                    y: { grid: { color: 'rgba(0, 240, 255, 0.09)' }, ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 } } }
                }
            }
        });
    }

    /* ==========================================================
       11. RENDERERS FOR 3D THERMAL GRID & BLOCKCHAIN LEDGER
       ========================================================== */
    function renderThermalGrid(gridData) {
        if (!gridData || !gridData.length) return;
        $('thermalGridContainer').innerHTML = gridData.map(node => {
            const cls = node.status === 'THERMAL_STRESS' ? 'stress' : (node.status === 'SATURATED' ? 'saturated' : '');
            const badgeColor = node.status === 'THERMAL_STRESS' ? 'text-[#FF007F]' : (node.status === 'SATURATED' ? 'text-[#00F0FF]' : 'text-[#39FF14]');
            return `
                <div onclick="inspectThermalNode('${node.node_id}', '${node.status}', ${node.vwc}, ${node.leaf_temp})" class="iso-node ${cls}">
                    <div class="flex items-center justify-between text-[10px] mono text-slate-300">
                        <span class="font-bold text-white">${node.node_id}</span>
                        <span class="${badgeColor} font-bold">${node.status}</span>
                    </div>
                    <div class="mt-1.5 flex items-baseline justify-between">
                        <span class="text-base font-black mono text-[#00F0FF]">${node.vwc}%</span>
                        <span class="text-xs mono text-[#FFE600]">${node.leaf_temp}°C</span>
                    </div>
                </div>
            `;
        }).join('');
    }

    function inspectThermalNode(nodeId, status, vwc, temp) {
        playSfx('click');
        const type = status === 'THERMAL_STRESS' ? 'error' : (status === 'SATURATED' ? 'warning' : 'success');
        showToast(`Sub-Zone Node ${nodeId}`, `Status: ${status} · Soil VWC: ${vwc}% · Canopy Temp: ${temp}°C`, type);
    }

    function renderBlockchainLedger(blocks) {
        if (!blocks || !blocks.length) return;
        $('blockchainList').innerHTML = blocks.map(b => `
            <div class="p-3 rounded-2xl bg-black/75 border border-[#00F0FF]/30 hover:border-[#00F0FF] transition text-xs shadow-md">
                <div class="flex items-center justify-between">
                    <span class="font-bold text-[#39FF14] mono">BLOCK #${b.block_id} · ${b.timestamp.slice(11, 19)}</span>
                    <button onclick="copyBlockHash(${b.block_id}, '${b.block_hash}')" class="text-[10px] mono px-2 py-0.5 rounded bg-[#00F0FF]/15 hover:bg-[#00F0FF]/30 text-[#00F0FF] border border-[#00F0FF]/40 transition cursor-pointer">
                        📋 Copy Hash
                    </button>
                </div>
                <div class="text-slate-200 font-medium mt-1 mono text-[11px]">${b.event_summary}</div>
                <div class="mt-1.5 flex items-center justify-between text-[10px] mono text-slate-400">
                    <span class="truncate max-w-[45%]">BY: <b class="text-[#FF007F]">${b.triggered_by}</b></span>
                    <span onclick="copyBlockHash(${b.block_id}, '${b.block_hash}')" class="text-[#00F0FF] truncate max-w-[50%] font-bold cursor-pointer hover:underline" title="Click to copy full 64-char SHA-256 hash">
                        SHA256: ${b.block_hash.slice(0, 18)}...
                    </span>
                </div>
            </div>
        `).join('');
    }

    function updateAtmosphericMood(actionText, emergency) {
        const orb1 = document.querySelector('.orb-1');
        const orb2 = document.querySelector('.orb-2');
        if (emergency || actionText.includes('Critical')) {
            orb1.style.background = 'radial-gradient(circle, #FF007F 0%, #ff0033 70%, transparent 100%)';
            orb2.style.background = 'radial-gradient(circle, #FFE600 0%, #ff5500 70%, transparent 100%)';
        } else if (actionText.includes('Rain Override')) {
            orb1.style.background = 'radial-gradient(circle, #00F0FF 0%, #0088ff 70%, transparent 100%)';
            orb2.style.background = 'radial-gradient(circle, #BC13FE 0%, #6600ff 70%, transparent 100%)';
        } else {
            orb1.style.background = 'radial-gradient(circle, #00F0FF 0%, #0044ff 70%, transparent 100%)';
            orb2.style.background = 'radial-gradient(circle, #FF007F 0%, #7900ff 70%, transparent 100%)';
        }
    }

    /* ==========================================================
       12. LIVE TELEMETRY POLLING ENGINE
       ========================================================== */
    async function refreshTelemetry() {
        try {
            const data = await api('/api/telemetry');
            $('connectionStatus').textContent = 'Live Link';
            $('statusDot').style.background = '#39FF14';

            const zones = data.zones || [];
            const count = zones.length || 1;
            const avgTemp = zones.reduce((a, z) => a + z.ambient_temp_c, 0) / count;
            const avgHum = zones.reduce((a, z) => a + z.humidity_pct, 0) / count;
            const avgMos = zones.reduce((a, z) => a + z.soil_moisture_vwc, 0) / count;
            const avgEc = zones.reduce((a, z) => a + z.electrical_conductivity_ec, 0) / count;

            $('metric_temp').textContent = avgTemp.toFixed(1);
            $('metric_humidity').textContent = avgHum.toFixed(1);
            $('metric_moisture').textContent = avgMos.toFixed(1);
            $('metric_ec').textContent = avgEc.toFixed(2);
            $('metric_rain').textContent = data.system.rain_probability.toFixed(0);
            $('metric_flow').textContent = `${data.system.flow_actual_lpm.toFixed(1)} / ${data.system.flow_expected_lpm.toFixed(1)} LPM`;
            $('metric_watchdog').textContent = data.system.watchdog;

            const sys = data.system;
            $('activeAction').textContent = sys.active_action;
            updateAtmosphericMood(sys.active_action, sys.emergency_stop);

            const isAuditor = (getActiveRole() === 'Auditor');
            if (sys.emergency_stop) {
                $('emergencyBtn').innerHTML = isAuditor ? '🔒 RESET EMERGENCY STOP' : '🔄 RESET EMERGENCY STOP';
                $('emergencyBtn').dataset.engaged = '1';
            } else {
                $('emergencyBtn').innerHTML = isAuditor ? '🔒 EMERGENCY KILL SWITCH' : '⚠️ EMERGENCY KILL SWITCH';
                $('emergencyBtn').dataset.engaged = '';
            }

            $('pumpStates').innerHTML = Object.entries(sys.pump_states).map(([z, st]) => `${z.replace('Zone_', '')}: <b>${st}</b>`).join('<br>');
            $('configBox').innerHTML = `Crop: <b>${sys.selected_crop}</b><br>Crit VWC: ${data.config.crit_moisture_threshold}%<br>Rain Cap: ${data.config.rain_override_threshold}%`;

            if (sys.drone) {
                $('droneStatusBadge').textContent = `${sys.drone.status} · BAT ${sys.drone.battery_pct}%`;
                renderThermalGrid(sys.drone.thermal_grid);
            }

            if (sys.pest_intelligence) {
                const p = sys.pest_intelligence;
                $('pestBadge').textContent = p.risk_level;
                $('pestThreat').textContent = p.threat_name;
                $('pestWetness').textContent = p.leaf_wetness_pct + '%';
                $('pestConfidence').textContent = p.confidence_pct + '%';
                $('pestPrescription').textContent = 'Rx: ' + p.bio_prescription;
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
                $('yieldCropLabel').textContent = `Crop: ${yf.crop_variety} (VWC ${yf.avg_vwc_pct}%)`;
                $('yieldUnitPrice').textContent = `APMC Rate: ₹${Number(yf.price_per_ton_inr || 28500).toLocaleString('en-IN')}/t`;
                $('yieldEt0').textContent = `${yf.et0_mm_day} mm/day`;
                $('yieldConfBadge').textContent = `${yf.confidence_score}% CONF`;
            }

            if (sys.mqtt_bus) {
                $('mqttTopicText').textContent = sys.mqtt_bus.last_topic;
                $('mqttAckText').textContent = `${sys.mqtt_bus.last_payload} (${sys.mqtt_bus.latency_ms}ms)`;
            }
            if (sys.sms_gateway) {
                $('smsStatusText').textContent = `${sys.sms_gateway.last_channel} -> ${sys.sms_gateway.last_recipient} (#${sys.sms_gateway.dispatched_count})`;
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
            $('connectionStatus').textContent = 'Reconnecting...';
            $('statusDot').style.background = '#FFE600';
        }
    }

    /* ==========================================================
       13. INTERACTIVE ACTION HANDLERS (WITH TOAST NOTIFICATIONS)
       ========================================================== */
    function exportComplianceCSV() {
        playSfx('click');
        showToast('Exporting Compliance Audit CSV', 'Streaming sensor telemetry & SHA-256 blockchain ledger download...', 'success');
        window.location.href = '/api/export/csv';
    }

    async function publishMQTTCommand() {
        if (!checkVisualRbacGuard('ESP32 MQTT Publish')) return;
        playSfx('scan');
        const node_id = $('mqtt_node').value;
        const payload = $('mqtt_payload').value;
        const topic = `aquasense/esp32/${node_id.toLowerCase()}/command`;
        try {
            const d = await api('/api/mqtt/publish', 'POST', { node_id, topic, payload, qos: 1 });
            $('mqttTopicText').textContent = d.topic;
            $('mqttAckText').textContent = `ACK: ${d.payload} (${d.esp32_ack_latency_ms}ms)`;
            showToast('ESP32 MQTT Packet ACK', `Dispatched ${d.payload} to ${d.node_id} (${d.esp32_ack_latency_ms}ms latency).`, 'success');
            refreshTelemetry();
        } catch (e) {
            playSfx('alert');
            $('mqttAckText').textContent = '✖ ' + e.message;
            showToast('MQTT Publish Blocked', e.message, 'error');
        }
    }

    async function dispatchSMSAlert() {
        if (!checkVisualRbacGuard('WhatsApp / SMS Alert Dispatch')) return;
        playSfx('scan');
        const payload = {
            recipient_phone: "+91-9876543210",
            channel: "WHATSAPP + TWILIO SMS",
            alert_message: `AquaSense Pro [${$('sel_crop').value}]: Telemetry & Pathogen Watch active for Udupi Sector.`
        };
        try {
            const d = await api('/api/sms/alert', 'POST', payload);
            $('smsStatusText').textContent = `✔ Dispatched #${d.dispatched_count} to ${d.recipient}`;
            showToast('WhatsApp & Twilio SMS Sent', `Dispatched alert #${d.dispatched_count} to ${d.recipient}.`, 'success');
            refreshTelemetry();
        } catch (e) {
            playSfx('alert');
            $('smsStatusText').textContent = '✖ ' + e.message;
            showToast('SMS Gateway Blocked', e.message, 'error');
        }
    }

    async function launchDroneScan() {
        playSfx('scan');
        const laser = $('droneLaser');
        laser.classList.remove('scanning');
        void laser.offsetWidth;
        laser.classList.add('scanning');
        $('droneStatusBadge').textContent = 'UAV: SCANNING CANOPY...';
        showToast('UAV Thermal Scout Airborne', 'Sweeping 12 sub-zone nodes at 24.5m altitude...', 'info');

        try {
            const res = await api('/api/drone/scan', 'POST');
            renderThermalGrid(res.drone.thermal_grid);
            $('droneStatusBadge').textContent = `${res.drone.status} (${res.drone.anomalies_found} Alerts)`;
            showToast('UAV Thermal Sweep Complete', `Mapped 12 farm sub-zones. Detected ${res.drone.anomalies_found} thermal/moisture anomalies.`, 'success');
            refreshTelemetry();
        } catch (e) {
            showToast('Drone Scan Error', e.message, 'error');
        }
    }

    function speakText(text) {
        if (!('speechSynthesis' in window)) return;
        window.speechSynthesis.cancel();
        const utter = new SpeechSynthesisUtterance(text);
        utter.rate = 1.04;
        utter.pitch = 1.05;
        window.speechSynthesis.speak(utter);
    }

    async function executeVoiceTranscript(transcript) {
        playSfx('click');
        $('voiceInputBox').value = transcript;
        $('voiceStateBadge').textContent = 'PROCESSING...';
        try {
            if (transcript.toLowerCase().includes('weather')) {
                await syncWeatherAPI();
            }
            const res = await api('/api/voice/command', 'POST', { transcript });
            $('voiceReplyBox').textContent = `🤖 AquaVoice: "${res.reply}"`;
            $('voiceStateBadge').textContent = 'SPOKEN';
            showToast('AquaVoice AI Directive', res.reply, 'info');
            speakText(res.reply);
            refreshTelemetry();
        } catch (e) {
            playSfx('alert');
            $('voiceReplyBox').textContent = '✖ ' + e.message;
            $('voiceStateBadge').textContent = 'LOCKED';
            showToast('AquaVoice RBAC Lock', e.message, 'warning');
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
        playSfx('scan');
        const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
        if (!SpeechRecognition) {
            const msg = 'Browser speech recognition unavailable—use any quick command chip or input box!';
            $('voiceReplyBox').textContent = '🎙️ ' + msg;
            showToast('AquaVoice Microphone', msg, 'info');
            speakText('Please select a quick command chip or type your directive.');
            return;
        }
        const recognition = new SpeechRecognition();
        recognition.lang = 'en-IN';
        recognition.interimResults = false;
        $('voiceStateBadge').textContent = '🔴 LISTENING...';
        showToast('AquaVoice Listening', 'Speak your agronomic command into the microphone...', 'info');
        recognition.onresult = (event) => {
            const transcript = event.results[0][0].transcript;
            executeVoiceTranscript(transcript);
        };
        recognition.onerror = () => {
            $('voiceStateBadge').textContent = 'READY';
        };
        recognition.start();
    }

    async function applyRegionAndCrop() {
        if (!checkVisualRbacGuard('Region & Crop Profile Update')) return;
        playSfx('click');
        const payload = { region: $('sel_region').value, crop_type: $('sel_crop').value };
        try {
            const data = await api('/api/region/configure', 'POST', payload);
            $('weatherSyncOutput').innerHTML = `<span class="text-[#39FF14] font-bold">Profile Applied:</span> ${data.selected_region} · Crop: <b>${data.selected_crop}</b>`;
            showToast('Agro-Climatic Profile Updated', `Configured ${data.selected_crop} for ${data.selected_region}.`, 'success');
            refreshTelemetry();
        } catch (e) {
            playSfx('alert');
            $('weatherSyncOutput').textContent = '✖ ' + e.message;
            showToast('Profile Update Blocked', e.message, 'error');
        }
    }

    async function syncWeatherAPI(silent = false) {
        if (!silent) playSfx('scan');
        $('weatherSyncOutput').textContent = '🛰️ Contacting Open-Meteo Satellite API for Udyavar / Udupi...';
        try {
            const data = await api('/api/weather/sync');
            const w = data.weather;
            lastWeatherSyncEpoch = Date.now();
            updateWeatherFreshnessCounter();
            $('weatherSyncOutput').innerHTML =
                `<b>${w.source}</b><br>` +
                `Temp: <span class="text-[#FFE600] font-bold">${w.temp}°C</span> · Humidity: <span class="text-[#00F0FF] font-bold">${w.humidity}%</span> · Wind: <b>${w.wind_speed} m/s</b> · Rain Prob: <b>${w.rain_prob}%</b>`;
            if (!silent) {
                showToast('Satellite Weather Synced', `Udupi Corridor: ${w.temp}°C, ${w.humidity}% RH, Rain Prob ${w.rain_prob}%`, 'success');
            }
            refreshTelemetry();
        } catch (e) {
            $('weatherSyncOutput').textContent = 'Sync failed: ' + e.message;
            if (!silent) showToast('Weather Sync Error', e.message, 'error');
        }
    }

    async function runSimulation() {
        playSfx('scan');
        const payload = {
            temperature: parseFloat($('sim_temp').value),
            humidity: parseFloat($('sim_humidity').value),
            base_moisture: parseFloat($('sim_moisture').value),
            wind_speed: parseFloat($('sim_wind').value),
            solar_radiation: parseFloat($('sim_solar').value),
            crop_stage: $('sim_stage').value,
            region: $('sel_region').value,
            crop_type: $('sel_crop').value
        };
        $('simOutput').textContent = 'Computing FAO-56 Penman-Monteith & Yield AI projection...';
        try {
            const d = await api('/api/simulate', 'POST', payload);
            $('simOutput').innerHTML =
                `<div class="text-[#39FF14] font-bold">${d.prescription.action}</div>` +
                `Projected VWC: <b>${d.blueprint_model.projected_soil_moisture}%</b> (Loss: -${d.blueprint_model.moisture_loss_pct}%)<br>` +
                `ET0: <b>${d.faO56.et0} mm/d</b> · Kc: <b>${d.faO56.kc}</b> · ETc: <b>${d.faO56.etc} mm/d</b><br>` +
                `Pump Runtime: <b>${d.prescription.pump_minutes} min</b> (${d.prescription.water_volume_liters} L)<br>` +
                `Yield Est: <b class="text-[#00F0FF]">${d.yield_forecast.projected_yield_tons} Tons</b> (₹${Number(d.yield_forecast.estimated_market_revenue_inr).toLocaleString('en-IN')})<br>` +
                `<span class="text-[#FF007F]">${d.prescription.npk_advice}</span>`;
            $('aiPrescription').textContent = `${d.prescription.action} · ${d.prescription.npk_advice}`;
            showToast('FAO-56 Simulation Complete', `${d.prescription.action} · Est Yield: ${d.yield_forecast.projected_yield_tons}t`, 'success');
            refreshTelemetry();
        } catch (e) {
            $('simOutput').textContent = 'Simulation error: ' + e.message;
            showToast('Simulation Failed', e.message, 'error');
        }
    }

    async function sendOverride() {
        if (!checkVisualRbacGuard('Actuator Valve Override')) return;
        playSfx('click');
        const payload = {
            zone_id: $('override_zone').value,
            action: $('override_action').value,
            duration_mins: parseInt($('override_duration').value || '10', 10)
        };
        try {
            const d = await api('/api/irrigation/override', 'POST', payload);
            $('overrideMsg').textContent = `✔ [${d.role}] ${d.zone_id} -> ${d.action} (${d.water_volume_liters} L)`;
            showToast('Actuator Relay Executed', `${d.zone_id} set to ${d.action} (${d.water_volume_liters} L).`, 'success');
            refreshTelemetry();
        } catch (e) {
            playSfx('alert');
            $('overrideMsg').textContent = '✖ ' + e.message;
            showToast('Relay Override Blocked', e.message, 'error');
        }
    }

    async function sendFertigation() {
        if (!checkVisualRbacGuard('NPK Fertigation Dosing')) return;
        playSfx('click');
        const payload = {
            growth_stage: $('fert_stage').value,
            nitrogen_ppm: parseFloat($('fert_n').value || '0'),
            phosphorus_ppm: parseFloat($('fert_p').value || '0'),
            potassium_ppm: parseFloat($('fert_k').value || '0'),
            leaching_risk_flag: false
        };
        try {
            const d = await api('/api/fertigation/configure', 'POST', payload);
            $('fertMsg').textContent = `✔ Dosed ${d.growth_stage}: N=${d.npk.nitrogen_ppm} P=${d.npk.phosphorus_ppm} K=${d.npk.potassium_ppm} (Leach Risk: ${d.leaching_risk_flag ? 'HIGH' : 'LOW'})`;
            showToast(
                'NPK Fertigation Injected',
                `Stage: ${d.growth_stage} (N:${d.npk.nitrogen_ppm} P:${d.npk.phosphorus_ppm} K:${d.npk.potassium_ppm} ppm)`,
                d.leaching_risk_flag ? 'warning' : 'success'
            );
            refreshTelemetry();
        } catch (e) {
            playSfx('alert');
            $('fertMsg').textContent = '✖ ' + e.message;
            showToast('Fertigation Blocked', e.message, 'error');
        }
    }

    async function emergencyToggle() {
        if (!checkVisualRbacGuard('Master Emergency Kill Switch')) return;
        playSfx('alert');
        const engaged = $('emergencyBtn').dataset.engaged === '1';
        try {
            const d = await api('/api/emergency', 'POST', { engage: !engaged });
            showToast(
                d.emergency_stop ? 'EMERGENCY KILL SWITCH ENGAGED' : 'Autonomous Control Restored',
                d.active_action,
                d.emergency_stop ? 'error' : 'success'
            );
            refreshTelemetry();
        } catch (e) {
            showToast('Emergency Toggle Blocked', e.message, 'error');
        }
    }

    async function refreshHistorical() {
        playSfx('click');
        const range = $('historicalRange').value;
        if (range === 'live') {
            await refreshTelemetry();
            showToast('Live Stream Restored', 'Streaming real-time 5-second oscilloscope telemetry.', 'info');
            return;
        }
        try {
            const d = await api('/api/analytics/historical?range=' + range);
            telemetryChart.data.labels = d.labels.map(l => range === '24h' ? l.slice(11, 16) : l.slice(5, 10));
            telemetryChart.data.datasets[0].data = d.moisture;
            telemetryChart.data.datasets[1].data = d.temp;
            telemetryChart.data.datasets[2].data = d.humidity;
            telemetryChart.update('none');
            showToast('Historical Horizon Loaded', `Displaying aggregated sensor curves for ${range.toUpperCase()}.`, 'info');
        } catch (e) {}
    }

    function bindSlider(id, labelId, unit) {
        const el = $(id);
        const lbl = $(labelId);
        el.addEventListener('input', () => {
            lbl.textContent = parseFloat(el.value).toFixed(1) + unit;
        });
    }

    window.addEventListener('load', () => {
        setInterval(() => {
            $('clock').textContent = new Date().toTimeString().slice(0, 8);
            updateWeatherFreshnessCounter();
        }, 1000);

        init3DPhysicsCards();
        initChart();

        bindSlider('sim_temp', 'sim_temp_label', ' °C');
        bindSlider('sim_humidity', 'sim_humidity_label', ' %');
        bindSlider('sim_moisture', 'sim_moisture_label', ' %');
        bindSlider('sim_wind', 'sim_wind_label', ' m/s');
        bindSlider('sim_solar', 'sim_solar_label', ' MJ/m²/d');

        $('historicalRange').addEventListener('change', refreshHistorical);

        refreshTelemetry();
        syncWeatherAPI(true);
        setInterval(refreshTelemetry, 5000);
        setInterval(() => syncWeatherAPI(true), 120000);

        setTimeout(() => {
            showToast('AquaSense Pro v2.0 Online', 'Press Ctrl+K for Command Palette or ? for Keyboard Shortcuts.', 'info');
        }, 600);
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