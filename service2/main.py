from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

# Weatherstack API Configuration
API_KEY = "a8da22591d11224f67c9a5ec111e0e0a"
BASE_URL = "http://api.weatherstack.com"

def fetch_weather_data(endpoint, params):
    """Helper function to fetch weather data from Weatherstack API"""
    params["access_key"] = API_KEY
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url, params=params)

    if response.status_code != 200:
        return {"error": "Failed to fetch weather data"}, response.status_code

    return response.json()

@app.route('/current_weather', methods=['GET'])
def get_current_weather():
    location = request.args.get('location')
    if not location:
        return jsonify({"error": "Location parameter is required"}), 400

    data = fetch_weather_data("current", {"query": location})
    return jsonify(data)

@app.route('/historical_weather', methods=['GET'])
def get_historical_weather():
    location = request.args.get('location')
    date = request.args.get('date')  # Format: YYYY-MM-DD
    if not location or not date:
        return jsonify({"error": "Location and date parameters are required"}), 400

    data = fetch_weather_data("historical", {"query": location, "historical_date": date})
    return jsonify(data)

@app.route('/historical_timeseries', methods=['GET'])
def get_historical_timeseries():
    location = request.args.get('location')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not location or not start_date or not end_date:
        return jsonify({"error": "Location, start_date, and end_date are required"}), 400

    data = fetch_weather_data("historical", {
        "query": location,
        "historical_date_start": start_date,
        "historical_date_end": end_date
    })
    return jsonify(data)

@app.route('/weather_forecast', methods=['GET'])
def get_weather_forecast():
    location = request.args.get('location')
    if not location:
        return jsonify({"error": "Location parameter is required"}), 400

    data = fetch_weather_data("forecast", {"query": location})
    return jsonify(data)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
