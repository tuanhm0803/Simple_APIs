import requests
import psycopg2
import datetime
import time

now = datetime.datetime.now()
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "250717"
DB_HOST = "host.docker.internal"
DB_PORT = 5432

# Replace with your actual API key
API_KEY = "8a86d68d-5008-4ebb-99d5-5a9ea7eca3eb"

# Specify the endpoint and its parameters
endpoint = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
params = {
    "start": 1,
    "limit": 100,
    "convert": "USD"
}

# Set the headers with your API key
headers = {
    "Accepts": "application/json",
    "X-CMC_PRO_API_KEY": API_KEY
}

def insert_data(data):
    try:
    #connect db
        conn = psycopg2.connect(dbname = DB_NAME,user  = DB_USER, password = DB_PASSWORD,host = DB_HOST, port = DB_PORT)            
        cursor = conn.cursor()
        #Insert data
        for item in data["data"]:
            name = item["name"]
            symbol = item["symbol"]
            price = float(item["quote"]["USD"]["price"])
            circulating_supply = item["circulating_supply"]
            total_supply = item["total_supply"]
            max_supply = item["max_supply"]
            source_code = "CMC"
            record_time = now
            currency_code = 'USD'
            try:
                cursor.execute("INSERT INTO stg.crypto_cur_info_v2(name,price,circulating_supply,total_supply,max_supply,record_time,source_code,currency_code,symbol) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)", (name,price,circulating_supply,total_supply,max_supply,record_time,source_code,currency_code,symbol))
            except ValueError:    
                print(f"Error: Invalid price data for {name}")
        conn.commit()
        print("data written into database")
    except (Exception, psycopg2.Error) as error:
        print("Postgres connection error", error)
    finally:
        if conn:    
            conn.close()  
while True:
    # Make the API call
    response = requests.get(endpoint, headers=headers, params=params)
    # Check for successful response
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        if __name__ == "__main__":
            insert_data(data)
    else:
        print(f"Error: {response.status_code}")
    #sleep for 10 mins
    time.sleep(60*10)
    
