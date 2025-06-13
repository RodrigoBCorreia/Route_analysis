#!/usr/bin/env python
# coding: utf-8

import os
import requests
import psycopg2
import json
from datetime import datetime
import time
from requests.exceptions import RequestException
import nest_asyncio

# 🔐 Load credentials from environment variables
HOST = os.environ.get("SUPABASE_HOST")
DB = os.environ.get("SUPABASE_DB")
USER = os.environ.get("SUPABASE_USER")
PASSWORD = os.environ.get("SUPABASE_PASSWORD")
PORT = os.environ.get("SUPABASE_PORT")
API_KEY = os.environ.get("AVIATION_API_KEY")

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

def get_european_airports(cursor):
    cursor.execute('SELECT "IATA" FROM public."Airports" WHERE "IATA" IS NOT NULL;')
    return [row[0] for row in cursor.fetchall()]

def get_flights(flight_type, airport_code, max_retries=3):
    url = f'https://aviation-edge.com/v2/public/timetable?key={API_KEY}&iataCode={airport_code}&type={flight_type}'
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            print(f"✅ {flight_type.capitalize()} flights fetched successfully for {airport_code}.")
            return response.json()
        except RequestException as e:
            wait = 2 ** (attempt - 1)
            print(f"⚠️ [{airport_code}] Attempt {attempt}/{max_retries} failed: {e!r}")
            if attempt < max_retries:
                print(f"   ↳ retrying in {wait}s…")
                time.sleep(wait)
            else:
                print(f"❌ All {max_retries} attempts failed for {airport_code}. Skipping.")
    return []

def save_flights(flights, flight_type, cursor, connection):
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

            cursor.execute('''
                SELECT 1 FROM flights
                WHERE flight_number=%s AND dep_time=%s AND arr_time=%s
            ''', (num, dep_time, arr_time))
            if cursor.fetchone():
                continue

            cursor.execute('''
                INSERT INTO flights(
                    flight_number, airline_iata, airline_name,
                    dep_iata, dep_time, arr_iata, arr_time,
                    reg_number, status, direction, fetch_date
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ''', (
                num,
                flight.get('airline', {}).get('iataCode', ''),
                flight.get('airline', {}).get('name', ''),
                dep.get('iataCode', ''),
                dep_time,
                arr.get('iataCode', ''),
                arr_time,
                flight.get('regNumber', ''),
                flight.get('status', ''),
                flight_type,
                today
            ))
            total += 1
        except Exception as e:
            print(f"⚠️ Error inserting flight {num}: {e}")
            connection.rollback()
    connection.commit()
    print(f"✅ {total} new {flight_type} flights saved to the database.")

def main():
    print("🟡 Connecting to Supabase...")
    connection = psycopg2.connect(
        host=HOST,
        database=DB,
        user=USER,
        password=PASSWORD,
        port=PORT
    )
    print("✅ Connected successfully.")

    cursor = connection.cursor()
    cursor.execute("SELECT NOW();")
    server_time = cursor.fetchone()
    print("🕒 Server time:", server_time)

    process_start = datetime.now()
    last_checkpoint = process_start
    print(f"🚀 Process started at: {process_start.strftime('%Y-%m-%d %H:%M:%S')}")

    airports = get_european_airports(cursor)
    total = len(airports)

    for idx, code in enumerate(airports, start=1):
        print(f"\n🌍 Processing airport {idx}/{total}: {code}")
        deps = get_flights('departure', code)
        arrs = get_flights('arrival', code)
        save_flights(deps, 'departure', cursor, connection)
        save_flights(arrs, 'arrival', cursor, connection)

        now = datetime.now()
        lap = now - last_checkpoint
        total_elapsed = now - process_start
        last_checkpoint = now

        print(f"✅ Completed {idx}/{total}")
        print(f"⏱️ {format_duration(lap)} since last • {format_duration(total_elapsed)} total")
        print("-" * 40)

    end = datetime.now()
    overall = end - process_start
    print(f"\n🎉 All {total} airports processed.")
    print(f"🕔 Completed at: {end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️ Total duration: {format_duration(overall)}")

    cursor.close()
    connection.close()
    print("🔴 Connection closed.")

if __name__ == "__main__":
    main()
