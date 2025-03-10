from flask import Flask, request, jsonify
import requests
from google.cloud import bigquery

app = Flask(__name__)
client = bigquery.Client()

# BigQuery dataset and table details
DATASET_ID = 'user_data'
TABLE_ID = 'user_input_table'

SERVICE_2_URL = "http://service2:8080/current_weather"  # Update with actual Service 2 URL

@app.route('/subscribe', methods=['POST'])
def subscribe():
    """
    Registers a new user, stores details in BigQuery, 
    calls Service 2 for weather, and stores weather info.
    """
    data = request.json

    required_fields = ["user_id", "location", "notification_method"]
    if not all(field in data for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    user_id = data["user_id"]
    location = data["location"]

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

    # Save user to BigQuery
    save_user_to_bigquery(user_id, email_id, phone_number, location, notification_method, preferred_units)

    # Fetch weather data from Service 2
    weather_data = fetch_weather_data(location)

    if "error" in weather_data:
        return jsonify({"error": "Failed to fetch weather data"}), 500

    # Store weather data in BigQuery
    save_weather_to_bigquery(user_id, location, weather_data)

    return jsonify({"message": "User subscribed successfully!", "weather": weather_data}), 201

def fetch_weather_data(location):
    """Calls Service 2 to get weather details."""
    response = requests.get(SERVICE_2_URL, params={"location": location})
    
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": "Weather service unavailable"}

def save_weather_to_bigquery(user_id, location, weather_data):
    """Stores weather details for the user in BigQuery."""
    temperature = weather_data.get("current", {}).get("temperature")
    weather_desc = weather_data.get("current", {}).get("weather_descriptions", [""])[0]
    humidity = weather_data.get("current", {}).get("humidity")

    rows_to_insert = [{
        "user_id": user_id,
        "location": location,
        "temperature": temperature,
        "weather_description": weather_desc,
        "humidity": humidity
    }]

    errors = client.insert_rows_json(f"{DATASET_ID}.weather_data", rows_to_insert)
    if errors:
        print(f"Error inserting weather data: {errors}")

if __name__ == '__main__':
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
