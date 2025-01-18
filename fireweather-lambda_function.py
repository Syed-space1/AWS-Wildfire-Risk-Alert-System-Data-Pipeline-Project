import json
import boto3
import os
from datetime import datetime
from decimal import Decimal
from botocore.vendored import requests
from pip._vendor import requests

# This is the Tomorrow.io API Key
API_KEY = "Redacted for security purposes"  

# Coordinates for Kelowna location (latitude and longitude)
LOCATION = "49.887951,-119.496010" 

# API URL
URL = f"https://api.tomorrow.io/v4/timelines"

# Initializing DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("WildfireMonitoring")  

# Function to fetch the weather data and calculate the risk of wildfire
def lambda_handler(event, context):
  
    # The parameters for the API request
    params = {
        "location": LOCATION,
        "fields": ["temperature", "humidity", "windSpeed", "windGust", "dewPoint", "precipitationProbability"],
        "timesteps": "current",
        "units": "metric",
        "apikey": API_KEY
    }

    # The API call is made
    response = requests.get(URL, params=params)
    if response.status_code != 200:
        return {
            "statusCode": response.status_code,
            "body": f"Error fetching weather data: {response.text}"
        }

    # Parsing the weather data
    data = response.json()["data"]["timelines"][0]["intervals"][0]["values"]
    temperature = data.get("temperature", 0)
    humidity = data.get("humidity", 0)
    wind_speed = data.get("windSpeed", 0)
    wind_gust = data.get("windGust", 0)
    dew_point = data.get("dewPoint", 0)
    precipitation_probability = data.get("precipitationProbability", 0)

    # Calculating wildfire risk logic
    if temperature > 25 and humidity < 30 and wind_speed > 15:
        risk_level = "Very High"
    elif temperature > 20 and humidity < 40 and wind_speed > 10:
        risk_level = "High"
    elif temperature > 15 and humidity < 50 and wind_speed > 5:
        risk_level = "Moderate"
    else:
        risk_level = "Low"

    # Converting the float values to Decimal for DynamoDB
    item = {
        "timestamp": datetime.utcnow().isoformat(),  
        "location": "Kelowna, BC",  
        "wildfireRisk": risk_level,
        "temperature": Decimal(str(temperature)), 
        "humidity": Decimal(str(humidity)),       
        "windSpeed": Decimal(str(wind_speed)),    
        "windGust": Decimal(str(wind_gust)),       
        "dewPoint": Decimal(str(dew_point)),    
        "precipitationProbability": Decimal(str(precipitation_probability)), 
    }

    # Insertng the items into the DynamoDB table
    try:
        table.put_item(Item=item)
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error saving data to DynamoDB: {str(e)}"
        }

    # This returns the calculated wildfire risk and weather data
    return {
        "statusCode": 200,
        "body": json.dumps({
            "wildfireRisk": risk_level,
            "temperature": temperature,
            "humidity": humidity,
            "windSpeed": wind_speed,
            "windGust": wind_gust,
            "dewPoint": dew_point,
            "precipitationProbability": precipitation_probability,
        })
    }
