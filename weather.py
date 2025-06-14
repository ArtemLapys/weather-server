from flask import Flask, request, jsonify
import requests
import sqlite3
import threading
import time
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# Настройки API
API_KEY = ''
WEATHER_API_KEY = ''
WEATHER_URL = 'https://api.openweathermap.org/data/2.5/weather'
UPDATE_INTERVAL = 3600  # в секундах

# === ИНИЦИАЛИЗАЦИЯ БД ===
def init_db():
    with sqlite3.connect('weather.db') as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lat REAL,
                lon REAL,
                location_key TEXT UNIQUE,
                weather_json TEXT,
                last_updated INTEGER
            )
        ''')
        conn.commit()

init_db()

# === УТИЛИТЫ ===
def round_coord(coord):
    return round(coord, 1)

def get_location_key(lat, lon):
    return f"{round_coord(lat)}_{round_coord(lon)}"

def fetch_weather(lat, lon):
    try:
        params = {
            'lat': lat,
            'lon': lon,
            'appid': WEATHER_API_KEY,
            'units': 'metric',
            'lang': 'ru'
        }
        response = requests.get(WEATHER_URL, params=params)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print("Ошибка при получении погоды:", e)
    return None

# === ФОНОВОЕ ОБНОВЛЕНИЕ ===
def update_all_weather():
    print("Обновление погоды...")
    with sqlite3.connect('weather.db') as conn:
        c = conn.cursor()
        c.execute('SELECT lat, lon, location_key FROM locations')
        for lat, lon, key in c.fetchall():
            data = fetch_weather(lat, lon)
            if data:
                c.execute('''
                    UPDATE locations SET weather_json = ?, last_updated = ?
                    WHERE location_key = ?
                ''', (jsonify_weather(data), int(time.time()), key))
        conn.commit()
    print("Обновление завершено.")

scheduler = BackgroundScheduler()
scheduler.add_job(update_all_weather, 'interval', seconds=UPDATE_INTERVAL)
scheduler.start()

# === ФОРМАТИРОВКА JSON ===
def jsonify_weather(data):
    return jsonify({
        "location": data.get("name"),
        "temperature": data["main"]["temp"],
        "description": data["weather"][0]["description"],
        "icon": data["weather"][0]["icon"]
    }).get_data(as_text=True)

def parse_weather_json(raw_json):
    import json
    return json.loads(raw_json)

# === ОБРАБОТКА ЗАПРОСОВ ===
@app.route('/weather', methods=['POST'])
def handle_request():
    client_key = request.headers.get("Authorization")
    if client_key != API_KEY:
        return jsonify({"error": "Unauthorized"}), 401
        
    data = request.get_json()
    lat = data.get('lat')
    lon = data.get('lon')

    if lat is None or lon is None:
        return jsonify({
            'status': 'error',
            'message': 'lat and lon required'
        }), 400

    key = get_location_key(lat, lon)
    rounded_lat = round_coord(lat)
    rounded_lon = round_coord(lon)

    with sqlite3.connect('weather.db') as conn:
        c = conn.cursor()
        c.execute('SELECT weather_json, last_updated FROM locations WHERE location_key = ?', (key,))
        row = c.fetchone()

        if row:
            weather_data = parse_weather_json(row[0])
            return jsonify({
                'status': 'ok',
                'data': weather_data,
                'timestamp': row[1]
            })
        else:
            # Новая зона — делаем 1 запрос
            data = fetch_weather(rounded_lat, rounded_lon)
            #data = fetch_weather(lat, lon)
            if data:
                weather_json = jsonify_weather(data)
                c.execute('''
                    INSERT INTO locations (lat, lon, location_key, weather_json, last_updated)
                    VALUES (?, ?, ?, ?, ?)
                ''', (rounded_lat, rounded_lon, key, weather_json, int(time.time())))
                conn.commit()

                return jsonify({
                    'status': 'ok',
                    'data': parse_weather_json(weather_json),
                    'timestamp': int(time.time())
                })
            else:
                return jsonify({
                    'status': 'wait',
                    'message': 'Информация пока не доступна. Повторите запрос через 5 минут.',
                    'retry_after': 300
                })

if __name__ == '__main__':
    app.run(debug=True)
