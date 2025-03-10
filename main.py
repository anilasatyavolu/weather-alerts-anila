from flask import Flask, request, jsonify
from google.cloud import bigquery
import requests
import os

app = Flask(__name__)
client = bigquery.Client()

# BigQuery dataset and table details
DATASET_ID = 'user_data' 
TABLE_ID = 'user_input_table'
WEATHER_TABLE_ID = 'weather_data'  # New table for storing weather data

# Weatherstack API Configuration
API_KEY = "a8da22591d11224f67c9a5ec111e0e0a"
BASE_URL = "http://api.weatherstack.com"

@app.route('/subscribe', methods=['POST'])
def subscribe():
    data = request.json

    required_fields = ["user_id", "location", "notification_method"]
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    user_id = data["user_id"]

    if user_exists(user_id):
        return jsonify({"error": "User ID already exists. Please choose a different one."}), 400

    email_id = data.get("email_id")
    phone_number = data.get("phone_number")
    
    if not email_id and not phone_number:
        return jsonify({"error": "Either email_id or phone number is required."}), 400

    notification_method = data["notification_method"]
    valid_methods = ["email", "SMS"]
    if not isinstance(notification_method, list) or not all(method in valid_methods for method in notification_method):
        return jsonify({"error": "Invalid notification method. Choose 'email' or 'SMS'."}), 400

    preferred_units = data.get("preferred_units", "Celsius")
    if preferred_units not in ["Celsius", "Fahrenheit"]:
        return jsonify({"error": "Invalid preferred_units. Choose 'Celsius' or 'Fahrenheit'."}), 400

    save_user_to_bigquery(user_id, email_id, phone_number, data["location"], notification_method, preferred_units)

    # Call Service 2 to fetch weather details
    weather_data = fetch_weather_data(data["location"])
    if weather_data:
        save_weather_to_bigquery(user_id, data["location"], weather_data)

    return jsonify({"message": "User subscribed successfully!"}), 201

@app.route('/users', methods=['GET'])
def get_users():
    users_data = get_users_from_bigquery()
    return jsonify(users_data)

def user_exists(user_id):
    query = f"""
    SELECT COUNT(*) as user_count
    FROM `{DATASET_ID}.{TABLE_ID}`
    WHERE user_id = @user_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("user_id", "STRING", user_id)]
    )
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()
    return list(result)[0].user_count > 0

def save_user_to_bigquery(user_id, email_id, phone_number, location, notification_method, preferred_units):
    rows_to_insert = [
        {
            "user_id": user_id,
            "email_id": email_id,
            "phone_number": phone_number,
            "location": location,
            "notification_method": notification_method,
            "preferred_units": preferred_units,
        }
    ]
    errors = client.insert_rows_json(f"{DATASET_ID}.{TABLE_ID}", rows_to_insert)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")

def get_users_from_bigquery():
    query = f"SELECT * FROM `{DATASET_ID}.{TABLE_ID}`"
    query_job = client.query(query)
    results = query_job.result()
    return [dict(row) for row in results]

def fetch_weather_data(location):
    """Fetch weather data from Weatherstack API."""
    params = {"query": location, "access_key": API_KEY}
    response = requests.get(f"{BASE_URL}/current", params=params)

    if response.status_code != 200:
        print("Failed to fetch weather data")
        return None

    return response.json()

def save_weather_to_bigquery(user_id, location, weather_data):
    """Save weather data to BigQuery."""
    rows_to_insert = [
        {
            "user_id": user_id,
            "location": location,
            "temperature": weather_data['current']['temperature'],
            "weather_description": weather_data['current']['weather_descriptions'][0],
            "humidity": weather_data['current']['humidity'],
        }
    ]
    errors = client.insert_rows_json(f"{DATASET_ID}.{WEATHER_TABLE_ID}", rows_to_insert)
    if errors:
        print(f"Encountered errors while inserting weather data: {errors}")

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
