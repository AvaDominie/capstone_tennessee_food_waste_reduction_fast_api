# To run: fastapi run main.py
from fastapi import FastAPI
import snowflake.connector
import logging 
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()



logger = logging.getLogger("data_staging")
logger.setLevel(logging.INFO)



# Snowflake connection function
def get_snowflake_connection(schema: str):
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=schema,
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )




# API endpoint to read GOLD_YIELD_EFFICIENCY table
@app.get("/GOLD_YIELD_EFFICIENCY")
async def read_gold_yield_efficiency():
    conn = get_snowflake_connection(os.getenv('SNOWFLAKE_SCHEMA_GOLD'))
    sf_cursor = conn.cursor()
    sf_cursor.execute("SELECT * FROM GOLD_YIELD_EFFICIENCY")
    data = sf_cursor.fetchall()
    columns = [desc[0] for desc in sf_cursor.description]
    logger.info("Connected to Snowflake successfully.")
    formatted_data = [dict(zip(columns, row)) for row in data] if data else None
    conn.close()
    return {"formatted_data": formatted_data}



# API endpoint to read GOLD_FORECAST_READY table
@app.get("/GOLD_FORECAST_READY")
async def read_gold_forecast_ready():
    conn = get_snowflake_connection(os.getenv('SNOWFLAKE_SCHEMA_GOLD'))
    sf_cursor = conn.cursor()
    sf_cursor.execute("SELECT * FROM GOLD_FORECAST_READY")
    data = sf_cursor.fetchall()
    columns = [desc[0] for desc in sf_cursor.description]
    logger.info("Connected to Snowflake successfully.")
    formatted_data = [dict(zip(columns, row)) for row in data] if data else None
    conn.close()
    return {"formatted_data": formatted_data}



# API endpoint to read GOLD_CENSUS_VALIDATION table
# Limit to 100 rows to query faster for demonstration purposes
@app.get("/GOLD_CENSUS_VALIDATION")
async def read_gold_census_validation():
    conn = get_snowflake_connection(os.getenv('SNOWFLAKE_SCHEMA_GOLD'))
    sf_cursor = conn.cursor()
    sf_cursor.execute("SELECT * FROM GOLD_CENSUS_VALIDATION LIMIT 100")
    data = sf_cursor.fetchall()
    columns = [desc[0] for desc in sf_cursor.description]
    logger.info("Connected to Snowflake successfully.")
    formatted_data = [dict(zip(columns, row)) for row in data] if data else None
    conn.close()
    return {"formatted_data": formatted_data}



# API endpoint to read GOLD_WEATHER_YIELD_CORRELATION table
@app.get("/GOLD_WEATHER_YIELD_CORRELATION")
async def read_gold_weather_yield_correlation():
    conn = get_snowflake_connection(os.getenv('SNOWFLAKE_SCHEMA_GOLD'))
    sf_cursor = conn.cursor()
    sf_cursor.execute("SELECT * FROM GOLD_WEATHER_YIELD_CORRELATION")
    data = sf_cursor.fetchall()
    columns = [desc[0] for desc in sf_cursor.description]
    logger.info("Connected to Snowflake successfully.")
    formatted_data = [dict(zip(columns, row)) for row in data] if data else None
    conn.close()
    return {"formatted_data": formatted_data}
