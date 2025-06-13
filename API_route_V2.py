#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import psycopg2
import socket
import json
from datetime import datetime
import time
import requests
from requests.exceptions import RequestException
import asyncio
import asyncpg
from psycopg2 import pool
import nest_asyncio


# In[2]:


# Substitui com os teus dados reais
HOST = "aws-0-eu-west-3.pooler.supabase.com"
DB = "postgres"
USER = "postgres.nhbyslgfifrxsubejasw"
PASSWORD = "Database_99_!_route"  # Substitui por completo
PORT = "6543"


# In[3]:


#Connect to the server

print("🟡 A ligar ao Supabase...")
connection = psycopg2.connect(
    host=HOST,
    database=DB,
    user=USER,
    password=PASSWORD,
    port=PORT
)

print("✅ Ligação estabelecida com sucesso!")

cursor = connection.cursor()
cursor.execute("SELECT NOW();")
resultado = cursor.fetchone()
print("🕒 Hora atual no servidor:", resultado)

# ✈️ API key and airport
API_KEY = '428c81-3e9216'  # Replace with your actual API key


# In[4]:


# — Helper to format a timedelta into d h m s —
def format_duration(td):
    total_seconds = int(td.total_seconds())
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)

# 🔁 Buscar a lista de IATA dos aeroportos da Europa
def get_european_airports():
    cursor.execute('SELECT "IATA" FROM public."Airports" WHERE "IATA" IS NOT NULL;')
    return [row[0] for row in cursor.fetchall()]

# 📱 Obter voos para um aeroporto
def get_flights(flight_type, airport_code, max_retries=3):
    url = f'https://aviation-edge.com/v2/public/timetable?key={API_KEY}&iataCode={airport_code}&type={flight_type}'
    for attempt in range(1, max_retries+1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            print(f"✅ {flight_type.capitalize()} flights fetched successfully for {airport_code}.")
            return response.json()
        except RequestException as e:
            # This will catch DNS errors, timeouts, HTTP errors, etc.
            wait = 2 ** (attempt - 1)
            print(f"⚠️ [{airport_code}] Attempt {attempt}/{max_retries} failed: {e!r}")
            if attempt < max_retries:
                print(f"   ↳ retrying in {wait}s…")
                time.sleep(wait)
            else:
                print(f"❌ All {max_retries} attempts failed for {airport_code}. Skipping.")
    return []


# 📂 Guardar os voos na base de dados
def save_flights(flights, flight_type):
    today = datetime.now().date()
    total = 0
    for flight in flights:
        if not isinstance(flight, dict):
            print(f"⚠️ Skipping non-dict flight record: {flight}")
            continue
        try:
            dep = flight.get('departure', {})
            arr = flight.get('arrival', {})
            num = flight.get('flight', {}).get('iataNumber', '')
            dep_time = dep.get('scheduledTime')
            arr_time = arr.get('scheduledTime')
            # skip duplicates
            cursor.execute('''
                SELECT 1 FROM flights
                WHERE flight_number=%s AND dep_time=%s AND arr_time=%s
            ''', (num, dep_time, arr_time))
            if cursor.fetchone():
                continue
            # insert
            cursor.execute('''
                INSERT INTO flights(
                    flight_number, airline_iata, airline_name,
                    dep_iata, dep_time, arr_iata, arr_time,
                    reg_number, status, direction, fetch_date
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ''', (
                num,
                flight.get('airline', {}).get('iataCode',''),
                flight.get('airline', {}).get('name',''),
                dep.get('iataCode',''),
                dep_time,
                arr.get('iataCode',''),
                arr_time,
                flight.get('regNumber',''),
                flight.get('status',''),
                flight_type,
                today
            ))
            total += 1
        except Exception as e:
            print(f"⚠️ Error inserting flight {num}: {e}")
            connection.rollback()
    connection.commit()
    print(f"✅ {total} new {flight_type} flights saved to the database.")

# 🕒 Timer start
process_start = datetime.now()
last_checkpoint = process_start
print(f"🚀 Process started at: {process_start.strftime('%Y-%m-%d %H:%M:%S')}")

# 🚀 Executar para todos os aeroportos
airports = get_european_airports()
total = len(airports)

for idx, code in enumerate(airports, start=1):
    print(f"\n🌍 Processing airport {idx}/{total}: {code}")

    deps = get_flights('departure', code)
    arrs = get_flights('arrival', code)
    save_flights(deps, 'departure')
    save_flights(arrs, 'arrival')

    now = datetime.now()
    lap = now - last_checkpoint
    total_elapsed = now - process_start
    last_checkpoint = now

    print(f"✅ Completed {idx}/{total}")
    print(f"⏱️ {format_duration(lap)} since last • {format_duration(total_elapsed)} total")
    print("-" * 40)

# 🕒 Timer end
end = datetime.now()
overall = end - process_start

print(f"\n🎉 All {total} airports processed.")
print(f"🕔 Completed at: {end.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⏱️ Total duration: {format_duration(overall)}")


# Properly close the connection
cursor.close()
connection.close()
print("🔴 Ligação encerrada com sucesso!")


# In[ ]:




