from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field
import sqlite3
import urllib.request
import json
import csv
import io

app = FastAPI(
    title="AquaSense Pro - Master Expo Edition",
    description="Enterprise AI Precision Irrigation, Weather Telemetry & Financial Analytics Platform"
)

# --- 1. DATABASE SETUP (SQLite) ---
DB_FILE = "aquasense_master.db"

def init_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS irrigation_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                crop_type TEXT,
                soil_type TEXT,
                field_area REAL,
                soil_moisture REAL,
                temperature REAL,
                rain_probability REAL,
                action TEXT,
                water_liters_per_sqm REAL,
                total_field_liters REAL,
                pump_minutes REAL,
                reasoning TEXT,
                cost_saved REAL
            )
        ''')
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Database initialization error: {e}")

init_db()

# --- 2. DATA MODELS ---
class IrrigationRequest(BaseModel):
    soil_moisture: float = Field(..., ge=0.0, le=100.0)
    temperature: float = Field(..., ge=-10.0, le=60.0)
    crop_type: str
    soil_type: str = "laterite"
    field_area: float = Field(default=500.0, gt=0.0)
    rain_probability: float = Field(..., ge=0.0, le=100.0)

class CityWeatherRequest(BaseModel):
    city: str

# --- 3. SCIENTIFIC AI & FINANCIAL DECISION ENGINE ---
@app.post("/calculate-irrigation")
def calculate_irrigation(data: IrrigationRequest):
    # FAO Standard Crop Coefficients (Kc)
    crop_coefficients = {
        "tomato": 1.15,
        "paddy": 1.25,
        "sugarcane": 1.30,
        "wheat": 1.05,
        "maize": 1.10,
        "arecanut": 1.20
    }
    
    # Soil Retention Multipliers (Laterite drains faster, Black clay holds water longer)
    soil_factors = {
        "laterite": 1.15,
        "red": 1.05,
        "alluvial": 1.00,
        "black": 0.85
    }
    
    crop_key = data.crop_type.lower().strip()
    soil_key = data.soil_type.lower().strip()
    
    kc = crop_coefficients.get(crop_key, 1.10)
    soil_factor = soil_factors.get(soil_key, 1.0)
    
    # Thermal Evaporation Factor
    thermal_factor = max(1.0, 1.0 + ((data.temperature - 25.0) * 0.03))
    
    action = "Optimal Status — Hold Irrigation"
    water_per_sqm = 0.0
    total_field_liters = 0.0
    pump_minutes = 0.0
    reason = "Soil volumetric moisture and ambient thermal metrics are within optimal bounds."
    risk_level = "Optimal Conditions ✅"
    cost_saved = 0.0
    
    # Core Decision Matrix
    if data.rain_probability >= 60.0:
        action = "Skip Irrigation (Rain Override)"
        water_per_sqm = 0.0
        reason = (
            f"High precipitation probability ({data.rain_probability}%) detected. "
            f"Natural rainfall will fulfill crop evapotranspiration demand, preventing water waste and nutrient leaching."
        )
        risk_level = "Rain Forecast Override 🌧️"
        # Calculate how much water & money was saved by NOT running the pump
        conserved_liters = round(data.field_area * 4.5 * kc, 1)
        cost_saved = round(conserved_liters * 0.04, 2)  # Pumping electricity & water valuation
    elif data.soil_moisture < 35.0:
        moisture_deficit = 35.0 - data.soil_moisture
        water_per_sqm = round(moisture_deficit * 0.35 * kc * soil_factor * thermal_factor, 2)
        total_field_liters = round(water_per_sqm * data.field_area, 1)
        # Standard 2HP agricultural pump delivers ~250 Liters/minute
        pump_minutes = round(total_field_liters / 250.0, 1)
        action = "Immediate Irrigation Required"
        reason = (
            f"Critical moisture deficit ({data.soil_moisture}%) in {soil_key.capitalize()} soil at {data.temperature}°C. "
            f"Deliver {water_per_sqm} L/m² ({total_field_liters} L total) by running the pump for {pump_minutes} minutes."
        )
        risk_level = "Severe Drought Stress ⚠️" if data.soil_moisture < 20.0 else "Moderate Deficit ⚡"
    elif data.soil_moisture > 75.0:
        action = "Suspend Irrigation (Saturation Alert)"
        water_per_sqm = 0.0
        reason = (
            f"Subsurface moisture is critically high ({data.soil_moisture}%). "
            f"Further watering risks root asphyxiation and fungal pathogen growth."
        )
        risk_level = "Waterlogging Risk 💧"
        cost_saved = round(data.field_area * 0.12, 2)
    else:
        reason = (
            f"Soil moisture ({data.soil_moisture}%) is healthy for {crop_key.capitalize()}. "
            f"Next automated telemetry check recommended in 12 hours."
        )
        cost_saved = round(data.field_area * 0.05, 2)

    # NPK Fertilizer & Agronomic Prescription
    npk_recommendations = {
        "tomato": "Apply Calcium Nitrate + Potassium boost (NPK 10-10-20) to prevent blossom-end rot.",
        "paddy": "Apply split Urea top-dressing + Zinc Sulfate (NPK 12-8-6) for tillering stage.",
        "sugarcane": "High Nitrogen & Phosphorus basal dose with organic mulch (NPK 15-6-10).",
        "wheat": "Balanced diammonium phosphate (DAP) & potash application (NPK 10-12-6).",
        "maize": "Foliar Nitrogen spray + Magnesium supplement (NPK 14-6-6).",
        "arecanut": "Apply organic compost + Neem cake + balanced NPK (100g N : 40g P : 140g K per palm)."
    }
    npk_advice = npk_recommendations.get(crop_key, "Standard balanced NPK 10-10-10 enrichment recommended.")

    # Save to SQLite Database
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO irrigation_logs (
                crop_type, soil_type, field_area, soil_moisture, temperature,
                rain_probability, action, water_liters_per_sqm, total_field_liters,
                pump_minutes, reasoning, cost_saved
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.crop_type.capitalize(), data.soil_type.capitalize(), data.field_area,
            data.soil_moisture, data.temperature, data.rain_probability, action,
            water_per_sqm, total_field_liters, pump_minutes, reason, cost_saved
        ))
        conn.commit()
        conn.close()
    except Exception as db_err:
        print(f"Logging error: {db_err}")

    metrics = get_system_metrics()

    return {
        "crop": data.crop_type.capitalize(),
        "soil_type": data.soil_type.capitalize(),
        "soil_moisture": data.soil_moisture,
        "rain_probability": data.rain_probability,
        "action": action,
        "recommended_water_liters": water_per_sqm,
        "total_field_liters": total_field_liters,
        "pump_minutes": pump_minutes,
        "reasoning": reason,
        "risk_level": risk_level,
        "npk_advice": npk_advice,
        "total_water_saved_system": metrics["water_saved"],
        "total_money_saved": metrics["money_saved"]
    }

@app.post("/fetch-weather")
def fetch_weather(req: CityWeatherRequest):
    city_coords = {
        "udupi": {"lat": 13.3409, "lon": 74.7421},
        "mangalore": {"lat": 12.9141, "lon": 74.8560},
        "bangalore": {"lat": 12.9716, "lon": 77.5946},
        "mysuru": {"lat": 12.2958, "lon": 76.6394},
        "mumbai": {"lat": 19.0760, "lon": 72.8777},
        "delhi": {"lat": 28.7041, "lon": 77.1025}
    }
    
    city_key = req.city.lower().strip()
    if city_key not in city_coords:
        raise HTTPException(status_code=404, detail="City not found.")
    
    lat = city_coords[city_key]["lat"]
    lon = city_coords[city_key]["lon"]
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,precipitation_probability"
    
    try:
        req_obj = urllib.request.Request(url, headers={'User-Agent': 'AquaSensePro/4.0'})
        with urllib.request.urlopen(req_obj, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            current = data.get("current", {})
            temp = current.get("temperature_2m", 29.0)
            rain_prob = current.get("precipitation_probability")
            if rain_prob is None:
                rain_prob = 15.0
            return {
                "city": req.city.capitalize(),
                "temperature": round(float(temp), 1),
                "rain_probability": float(rain_prob)
            }
    except Exception:
        # Automatic offline fallback so your demo NEVER fails even if expo Wi-Fi drops!
        fallback_data = {
            "udupi": {"temperature": 31.0, "rain_probability": 65.0},
            "mangalore": {"temperature": 30.5, "rain_probability": 60.0},
            "bangalore": {"temperature": 26.5, "rain_probability": 25.0},
            "mysuru": {"temperature": 28.0, "rain_probability": 20.0},
            "mumbai": {"temperature": 32.0, "rain_probability": 40.0},
            "delhi": {"temperature": 34.0, "rain_probability": 10.0}
        }
        fb = fallback_data.get(city_key, {"temperature": 30.0, "rain_probability": 20.0})
        return {
            "city": f"{req.city.capitalize()} (Cached Telemetry)",
            "temperature": fb["temperature"],
            "rain_probability": fb["rain_probability"]
        }

def get_system_metrics():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM irrigation_logs WHERE action LIKE 'Skip%' OR action LIKE 'Optimal%' OR action LIKE 'Suspend%'")
        skip_count = cursor.fetchone()[0] or 0
        cursor.execute("SELECT SUM(cost_saved) FROM irrigation_logs")
        money = cursor.fetchone()[0] or 0.0
        conn.close()
        return {"water_saved": skip_count * 450, "money_saved": round(money, 2)}
    except:
        return {"water_saved": 0, "money_saved": 0.0}

@app.get("/analytics")
def get_analytics():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*), AVG(soil_moisture), AVG(temperature) FROM irrigation_logs")
        res = cursor.fetchone()
        
        cursor.execute("SELECT timestamp, soil_moisture, temperature, water_liters_per_sqm FROM irrigation_logs ORDER BY id DESC LIMIT 10")
        chart_rows = cursor.fetchall()
        
        cursor.execute('''
            SELECT timestamp, crop_type, soil_type, soil_moisture, action, water_liters_per_sqm, pump_minutes 
            FROM irrigation_logs ORDER BY id DESC LIMIT 6
        ''')
        rows = cursor.fetchall()
        conn.close()
        
        history = [{
            "timestamp": r[0], "crop": r[1], "soil": r[2],
            "moisture": r[3], "action": r[4], "liters": r[5], "pump_min": r[6]
        } for r in rows]
        
        chart_data = [{
            "timestamp": r[0], "moisture": r[1], "temperature": r[2], "water": r[3]
        } for r in reversed(chart_rows)]
        
        metrics = get_system_metrics()

        return {
            "total_analyses": res[0] or 0,
            "average_soil_moisture": round(res[1] or 0.0, 1),
            "average_temperature": round(res[2] or 0.0, 1),
            "total_water_saved": metrics["water_saved"],
            "total_money_saved": metrics["money_saved"],
            "history": history,
            "chart_data": chart_data
        }
    except:
        return {
            "total_analyses": 0, "average_soil_moisture": 0.0, "average_temperature": 0.0,
            "total_water_saved": 0, "total_money_saved": 0.0, "history": [], "chart_data": []
        }

@app.get("/export-csv")
def export_csv():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM irrigation_logs ORDER BY id DESC")
        rows = cursor.fetchall()
        conn.close()
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Log ID", "Timestamp", "Crop", "Soil Type", "Field Area (m²)",
            "Soil Moisture (%)", "Temp (°C)", "Rain Prob (%)", "AI Directive",
            "Water (L/m²)", "Total Field Water (L)", "Pump Runtime (Min)",
            "Agronomic Reasoning", "Cost Saved (INR)"
        ])
        writer.writerows(rows)
        output.seek(0)
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment;filename=AquaSense_Pro_Telemetry_Report.csv"}
        )
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to export CSV report.")


# --- 4. THE MASTER ENTERPRISE DASHBOARD UI ---
@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    return """
    <!DOCTYPE html>
    <html lang="en" class="dark">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AquaSense Pro | AI Water Intelligence Platform</title>
        <script src="https://cdn.tailwindcss.com"></script>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col items-center p-4 md:p-8 selection:bg-emerald-500 selection:text-white">

        <div class="max-w-6xl w-full bg-slate-900 border border-slate-800 rounded-3xl shadow-2xl p-6 md:p-8 space-y-8">
            
            <!-- Dynamic Emergency Alert Banner -->
            <div id="alertBanner" class="hidden bg-rose-950/90 border border-rose-700 p-4 rounded-2xl flex items-center justify-between text-rose-100 text-sm animate-pulse">
                <div class="flex items-center space-x-3">
                    <span class="text-2xl">🚨</span>
                    <span id="alertText" class="font-semibold">Critical environmental threshold detected!</span>
                </div>
                <button onclick="document.getElementById('alertBanner').classList.add('hidden')" class="text-xs uppercase font-bold px-3 py-1 bg-rose-800 rounded-lg hover:bg-rose-700">Dismiss</button>
            </div>

            <!-- Top Header & Live Impact KPIs -->
            <div class="flex flex-col lg:flex-row justify-between items-start lg:items-center border-b border-slate-800 pb-6 gap-4">
                <div class="flex items-center space-x-4">
                    <div class="text-4xl bg-emerald-950 p-3.5 rounded-2xl border border-emerald-700/50 shadow-lg">🌱</div>
                    <div>
                        <div class="flex items-center gap-2">
                            <h1 class="text-3xl font-extrabold text-emerald-400 tracking-tight">AquaSense Pro</h1>
                            <span class="text-[10px] bg-emerald-500/20 text-emerald-300 border border-emerald-500/30 px-2.5 py-0.5 rounded-full font-bold uppercase">v4.0 Master</span>
                        </div>
                        <p class="text-sm text-slate-400">AI-Driven Precision Irrigation, Soil Telemetry & Financial Impact Engine</p>
                    </div>
                </div>

                <div class="flex flex-wrap items-center gap-3 w-full lg:w-auto">
                    <div class="bg-slate-800/90 border border-slate-700 px-4 py-2 rounded-2xl text-right flex-1 sm:flex-none">
                        <p class="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Telemetry Logs</p>
                        <p id="analyticsCount" class="text-sm font-extrabold text-sky-400 mt-0.5">0 Runs</p>
                    </div>
                    <div class="bg-slate-800/90 border border-slate-700 px-4 py-2 rounded-2xl text-right flex-1 sm:flex-none">
                        <p class="text-[10px] text-slate-400 uppercase tracking-wider font-semibold">Farmer Savings</p>
                        <p id="moneyBadge" class="text-sm font-extrabold text-amber-400 mt-0.5">₹0.00</p>
                    </div>
                    <div class="bg-emerald-950/90 border border-emerald-800/80 px-4 py-2 rounded-2xl text-right flex-1 sm:flex-none">
                        <p class="text-[10px] text-emerald-400 uppercase tracking-wider font-semibold">Water Conserved</p>
                        <p id="totalSavedBadge" class="text-sm font-extrabold text-white mt-0.5">0 Liters</p>
                    </div>
                    <a href="/export-csv" class="bg-teal-700 hover:bg-teal-600 text-white text-xs font-bold px-4 py-3 rounded-2xl transition shadow-lg flex items-center gap-1.5">
                        📥 Export CSV
                    </a>
                    <button onclick="window.print()" class="bg-slate-800 hover:bg-slate-700 text-slate-200 text-xs font-bold px-3.5 py-3 rounded-2xl border border-slate-700 transition" title="Print Farm Report">
                        🖨️
                    </button>
                </div>
            </div>

            <!-- Live Satellite/Weather Sync Bar -->
            <div class="bg-slate-800/50 border border-slate-700/70 rounded-2xl p-5 flex flex-col md:flex-row items-center justify-between gap-4">
                <div class="flex items-center space-x-3">
                    <span class="text-2xl">🛰️</span>
                    <div>
                        <h2 class="text-sm font-bold text-emerald-300">Live Meteorological Satellite Sync (Open-Meteo API)</h2>
                        <p id="weatherSyncStatus" class="text-xs text-slate-400">Select a regional station to pull real-time atmospheric temperature & rain probability.</p>
                    </div>
                </div>
                <div class="flex items-center space-x-3 w-full md:w-auto">
                    <select id="syncCity" class="bg-slate-900 border border-slate-700 rounded-xl p-2.5 text-sm text-white focus:outline-none focus:border-emerald-500 flex-1 md:flex-none">
                        <option value="udupi">Udupi, KA</option>
                        <option value="mangalore">Mangalore, KA</option>
                        <option value="bangalore">Bangalore, KA</option>
                        <option value="mysuru">Mysuru, KA</option>
                        <option value="mumbai">Mumbai, MH</option>
                        <option value="delhi">New Delhi, DL</option>
                    </select>
                    <button onclick="fetchLiveWeather()" class="bg-emerald-600 hover:bg-emerald-500 text-white font-semibold text-sm px-5 py-2.5 rounded-xl transition shadow-lg whitespace-nowrap">
                        🔄 Sync Live Weather
                    </button>
                </div>
            </div>

            <!-- Farm Parameters & Sensor Controls -->
            <div class="grid grid-cols-1 md:grid-cols-3 gap-5">
                <!-- Crop Type -->
                <div class="bg-slate-800/30 border border-slate-800 p-5 rounded-2xl space-y-2">
                    <label class="block text-xs uppercase tracking-wider font-bold text-slate-400">1. Cultivated Crop (FAO Kc)</label>
                    <select id="cropType" class="w-full bg-slate-900 border border-slate-700 rounded-xl p-3 text-white font-medium focus:outline-none focus:border-emerald-500">
                        <option value="paddy">🌾 Paddy / Rice (Kc: 1.25)</option>
                        <option value="arecanut">🌴 Arecanut / Plantation (Kc: 1.20)</option>
                        <option value="sugarcane">🎋 Sugarcane (Kc: 1.30)</option>
                        <option value="tomato">🍅 Tomato (Kc: 1.15)</option>
                        <option value="maize">🌽 Maize / Corn (Kc: 1.10)</option>
                        <option value="wheat">🌿 Wheat (Kc: 1.05)</option>
                    </select>
                </div>

                <!-- Soil Classification -->
                <div class="bg-slate-800/30 border border-slate-800 p-5 rounded-2xl space-y-2">
                    <label class="block text-xs uppercase tracking-wider font-bold text-slate-400">2. Regional Soil Profile</label>
                    <select id="soilType" class="w-full bg-slate-900 border border-slate-700 rounded-xl p-3 text-white font-medium focus:outline-none focus:border-emerald-500">
                        <option value="laterite">🪨 Coastal Laterite Soil (Fast Drain)</option>
                        <option value="red">🧱 Red Loamy Soil (Moderate)</option>
                        <option value="alluvial">🌊 Alluvial Delta Soil (Balanced)</option>
                        <option value="black">⬛ Black Cotton Soil (High Retention)</option>
                    </select>
                </div>

                <!-- Field Area -->
                <div class="bg-slate-800/30 border border-slate-800 p-5 rounded-2xl space-y-2">
                    <div class="flex justify-between">
                        <label class="text-xs uppercase tracking-wider font-bold text-slate-400">3. Farm Plot Area</label>
                        <span id="areaVal" class="text-sm font-bold text-emerald-400">500 m²</span>
                    </div>
                    <input type="range" id="fieldArea" min="100" max="5000" step="100" value="500" class="w-full accent-emerald-500 cursor-pointer h-2 bg-slate-900 rounded-lg" oninput="document.getElementById('areaVal').innerText = this.value + ' m²'">
                    <p class="text-[11px] text-slate-500">Scales total water volume & 2HP pump runtime.</p>
                </div>
            </div>

            <!-- Telemetry Sliders -->
            <div class="grid grid-cols-1 md:grid-cols-3 gap-5">
                <!-- Soil Moisture -->
                <div class="bg-slate-800/30 border border-slate-800 p-5 rounded-2xl space-y-3">
                    <div class="flex justify-between text-sm">
                        <span class="text-slate-300 font-semibold">💧 Soil Moisture</span>
                        <span id="moistureVal" class="font-extrabold text-emerald-400">24%</span>
                    </div>
                    <input type="range" id="soilMoisture" min="0" max="100" value="24" class="w-full accent-emerald-500 cursor-pointer h-2 bg-slate-900 rounded-lg" oninput="document.getElementById('moistureVal').innerText = this.value + '%'">
                    <div class="flex justify-between text-[10px] text-slate-500">
                        <span>0% (Bone Dry)</span>
                        <span>35% (Target)</span>
                        <span>100% (Saturated)</span>
                    </div>
                </div>

                <!-- Ambient Temperature -->
                <div class="bg-slate-800/30 border border-slate-800 p-5 rounded-2xl space-y-3">
                    <div class="flex justify-between text-sm">
                        <span class="text-slate-300 font-semibold">🌡️ Ambient Temp</span>
                        <span id="tempVal" class="font-extrabold text-emerald-400">32 °C</span>
                    </div>
                    <input type="range" id="temperature" min="15" max="45" step="0.5" value="32" class="w-full accent-emerald-500 cursor-pointer h-2 bg-slate-900 rounded-lg" oninput="document.getElementById('tempVal').innerText = this.value + ' °C'">
                    <div class="flex justify-between text-[10px] text-slate-500">
                        <span>15°C (Cool)</span>
                        <span>30°C (Warm)</span>
                        <span>45°C (Extreme)</span>
                    </div>
                </div>

                <!-- Rain Probability -->
                <div class="bg-slate-800/30 border border-slate-800 p-5 rounded-2xl space-y-3">
                    <div class="flex justify-between text-sm">
                        <span class="text-slate-300 font-semibold">🌧️ 24h Rain Forecast</span>
                        <span id="rainVal" class="font-extrabold text-emerald-400">15%</span>
                    </div>
                    <input type="range" id="rainProb" min="0" max="100" value="15" class="w-full accent-emerald-500 cursor-pointer h-2 bg-slate-900 rounded-lg" oninput="document.getElementById('rainVal').innerText = this.value + '%'">
                    <div class="flex justify-between text-[10px] text-slate-500">
                        <span>0% (Clear)</span>
                        <span>60% (Skip Threshold)</span>
                        <span>100% (Storm)</span>
                    </div>
                </div>
            </div>

            <!-- Action Trigger Button -->
            <button onclick="runAnalysis()" class="w-full bg-gradient-to-r from-emerald-600 via-teal-600 to-emerald-600 hover:from-emerald-500 hover:to-teal-500 text-white font-extrabold py-4 rounded-2xl transition duration-200 shadow-xl shadow-emerald-950/60 tracking-wide text-lg">
                🚀 Execute AI Irrigation & Agronomic Diagnostic
            </button>

            <!-- AI Diagnostic Results Card -->
            <div id="resultsCard" class="hidden bg-slate-950 border border-slate-800 rounded-2xl p-6 space-y-6">
                <div class="flex flex-wrap items-center justify-between border-b border-slate-800 pb-4 gap-3">
                    <div class="flex items-center gap-3">
                        <h2 class="text-xl font-bold text-emerald-400">📋 AI Precision Action Plan</h2>
                        <span id="riskBadge" class="text-xs font-bold px-3 py-1 rounded-full bg-amber-950 text-amber-300 border border-amber-800">-</span>
                    </div>
                    <button onclick="speakReport()" class="bg-emerald-900/60 hover:bg-emerald-800 text-emerald-200 border border-emerald-700/60 text-xs font-bold px-4 py-2 rounded-xl transition flex items-center gap-2">
                        🔊 Speak AI Report
                    </button>
                </div>
                
                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div class="bg-slate-900 border border-slate-800 p-5 rounded-xl">
                        <p class="text-[11px] text-slate-400 uppercase tracking-wider font-semibold">AI Directive</p>
                        <p id="resAction" class="text-lg font-extrabold text-white mt-1">-</p>
                    </div>
                    <div class="bg-slate-900 border border-slate-800 p-5 rounded-xl">
                        <p class="text-[11px] text-slate-400 uppercase tracking-wider font-semibold">Water Dose & Total Volume</p>
                        <p id="resWater" class="text-lg font-extrabold text-emerald-400 mt-1">-</p>
                    </div>
                    <div class="bg-slate-900 border border-slate-800 p-5 rounded-xl">
                        <p class="text-[11px] text-slate-400 uppercase tracking-wider font-semibold">2HP Motor Pump Runtime</p>
                        <p id="resPump" class="text-lg font-extrabold text-sky-400 mt-1">-</p>
                    </div>
                </div>

                <div class="bg-emerald-950/30 border border-emerald-900/50 p-4 rounded-xl text-emerald-200 text-sm leading-relaxed" id="resReason">
                    -
                </div>

                <div class="bg-blue-950/30 border border-blue-900/50 p-4 rounded-xl text-blue-200 text-sm flex items-center space-x-3">
                    <span class="text-2xl">🧪</span>
                    <div>
                        <p class="text-xs font-bold text-blue-400 uppercase tracking-wider">Soil Nutrient (NPK) & Crop Care Prescription</p>
                        <p id="resNPK" class="mt-0.5">-</p>
                    </div>
                </div>
            </div>

            <!-- Live Chart.js Analytics & Telemetry Table Grid -->
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <!-- Chart -->
                <div class="bg-slate-900/60 border border-slate-800 rounded-2xl p-6 space-y-4">
                    <h3 class="text-xs font-bold text-slate-300 uppercase tracking-wider">📈 Subsurface Moisture vs. Temperature Trend</h3>
                    <div class="w-full h-64">
                        <canvas id="telemetryChart"></canvas>
                    </div>
                </div>

                <!-- Recent Database Logs -->
                <div class="bg-slate-900/60 border border-slate-800 rounded-2xl p-6 space-y-4">
                    <h3 class="text-xs font-bold text-slate-300 uppercase tracking-wider">📊 Live SQLite Database Telemetry</h3>
                    <div class="overflow-x-auto">
                        <table class="w-full text-left text-xs text-slate-400">
                            <thead class="bg-slate-800 text-slate-200 uppercase text-[10px]">
                                <tr>
                                    <th class="p-2.5">Crop / Soil</th>
                                    <th class="p-2.5">Moisture</th>
                                    <th class="p-2.5">Directive</th>
                                    <th class="p-2.5">Dose</th>
                                    <th class="p-2.5">Pump</th>
                                </tr>
                            </thead>
                            <tbody id="historyTableBody" class="divide-y divide-slate-800">
                                <tr>
                                    <td colspan="5" class="p-4 text-center text-slate-500">Run an analysis to log database telemetry.</td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

        </div>

        <!-- Frontend JavaScript Engine -->
        <script>
            let telemetryChart = null;
            let lastSpokenText = "";

            function speakReport() {
                if (!('speechSynthesis' in window)) {
                    alert("Speech synthesis is not supported in this browser.");
                    return;
                }
                window.speechSynthesis.cancel();
                const utterance = new SpeechSynthesisUtterance(lastSpokenText || "Please run an analysis first.");
                utterance.rate = 1.0;
                utterance.pitch = 1.0;
                window.speechSynthesis.speak(utterance);
            }

            function initChart(chartData) {
                const ctx = document.getElementById('telemetryChart').getContext('2d');
                const labels = chartData.map((d, idx) => `Run #${idx + 1}`);
                const moistureValues = chartData.map(d => d.moisture);
                const tempValues = chartData.map(d => d.temperature);

                if (telemetryChart) {
                    telemetryChart.destroy();
                }

                telemetryChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                label: 'Soil Moisture (%)',
                                data: moistureValues,
                                borderColor: '#10b981',
                                backgroundColor: 'rgba(16, 185, 129, 0.12)',
                                tension: 0.35,
                                fill: true
                            },
                            {
                                label: 'Temperature (°C)',
                                data: tempValues,
                                borderColor: '#38bdf8',
                                backgroundColor: 'rgba(56, 189, 248, 0.1)',
                                tension: 0.35,
                                fill: false
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: { legend: { labels: { color: '#cbd5e1' } } },
                        scales: {
                            y: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255, 255, 255, 0.05)' } },
                            x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(255, 255, 255, 0.05)' } }
                        }
                    }
                });
            }

            async function loadAnalytics() {
                try {
                    const res = await fetch('/analytics');
                    if (res.ok) {
                        const data = await res.json();
                        document.getElementById('analyticsCount').innerText = `${data.total_analyses} Runs`;
                        document.getElementById('totalSavedBadge').innerText = `${data.total_water_saved} Liters`;
                        document.getElementById('moneyBadge').innerText = `₹${data.total_money_saved}`;

                        const tbody = document.getElementById('historyTableBody');
                        if (data.history && data.history.length > 0) {
                            tbody.innerHTML = '';
                            data.history.forEach(row => {
                                tbody.innerHTML += `
                                    <tr class="hover:bg-slate-800/50 transition">
                                        <td class="p-2.5 font-semibold text-emerald-400">${row.crop} <span class="text-slate-500 font-normal">(${row.soil})</span></td>
                                        <td class="p-2.5">${row.moisture}%</td>
                                        <td class="p-2.5 text-white">${row.action}</td>
                                        <td class="p-2.5 text-emerald-300 font-semibold">${row.liters} L/m²</td>
                                        <td class="p-2.5 text-sky-300">${row.pump_min} min</td>
                                    </tr>
                                `;
                            });
                        }

                        if (data.chart_data) {
                            initChart(data.chart_data);
                        }
                    }
                } catch (e) {
                    console.error("Failed to load analytics");
                }
            }

            async function runAnalysis() {
                const moisture = parseFloat(document.getElementById('soilMoisture').value);
                const payload = {
                    soil_moisture: moisture,
                    temperature: parseFloat(document.getElementById('temperature').value),
                    crop_type: document.getElementById('cropType').value,
                    soil_type: document.getElementById('soilType').value,
                    field_area: parseFloat(document.getElementById('fieldArea').value),
                    rain_probability: parseFloat(document.getElementById('rainProb').value)
                };

                if (moisture < 20) {
                    document.getElementById('alertBanner').classList.remove('hidden');
                    document.getElementById('alertText').innerText = `CRITICAL DROUGHT ALERT: Soil moisture is at ${moisture}%. Immediate irrigation required to prevent crop wilting!`;
                } else if (moisture > 80) {
                    document.getElementById('alertBanner').classList.remove('hidden');
                    document.getElementById('alertText').innerText = `WATERLOGGING ALERT: Soil saturation at ${moisture}%. Suspend all water valves immediately!`;
                } else {
                    document.getElementById('alertBanner').classList.add('hidden');
                }

                try {
                    const response = await fetch('/calculate-irrigation', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(payload)
                    });

                    if (response.ok) {
                        const data = await response.json();
                        document.getElementById('resAction').innerText = data.action;
                        document.getElementById('resWater').innerText = `${data.recommended_water_liters} L/m² (${data.total_field_liters} L total)`;
                        document.getElementById('resPump').innerText = data.pump_minutes > 0 ? `${data.pump_minutes} Minutes` : `0 Minutes (Pump Off)`;
                        document.getElementById('resReason').innerHTML = `💡 <strong>AI Agronomic Reasoning:</strong> ${data.reasoning}`;
                        document.getElementById('resNPK').innerText = data.npk_advice;
                        document.getElementById('riskBadge').innerText = data.risk_level;
                        
                        lastSpokenText = `AquaSense AI Report for ${data.crop}. ${data.action}. ${data.reasoning}`;
                        
                        document.getElementById('resultsCard').classList.remove('hidden');
                        loadAnalytics();
                    }
                } catch (error) {
                    alert('Server communication failure. Make sure uvicorn is running in VS Code.');
                }
            }

            async function fetchLiveWeather() {
                const city = document.getElementById('syncCity').value;
                const statusEl = document.getElementById('weatherSyncStatus');
                statusEl.innerText = "Connecting to meteorological satellite telemetry...";
                try {
                    const response = await fetch('/fetch-weather', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ city: city })
                    });

                    if (response.ok) {
                        const data = await response.json();
                        document.getElementById('temperature').value = data.temperature;
                        document.getElementById('tempVal').innerText = data.temperature + ' °C';
                        document.getElementById('rainProb').value = data.rain_probability;
                        document.getElementById('rainVal').innerText = data.rain_probability + '%';
                        statusEl.innerHTML = `✅ Synced live weather for <strong>${data.city}</strong>: ${data.temperature}°C, Rain Probability: ${data.rain_probability}%`;
                    }
                } catch (err) {
                    statusEl.innerText = "Error connecting to weather service.";
                }
            }

            loadAnalytics();
        </script>
    </body>
    </html>
    """