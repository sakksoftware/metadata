import os
from datetime import datetime, timedelta
import requests 
import json
from flask import Flask, request, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import UniqueConstraint
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from marshmallow import fields

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

mysql_db_driver_class = "com.mysql.cj.jdbc.Driver"
mysql_hostname = "localhost"
mysql_port = str(3306)
user_name = "root"
password = "root"
mysql_database_name = "temperature_db"
mysql_url = "jdbc:mysql://" + mysql_hostname + ":" + mysql_port + "/" + mysql_database_name

postgres_db_driver_class = "org.postgresql.Driver"
postgres_hostname = "localhost"
postgres_port = str(5432)
user_name = "postgres"
password = "thenewshit"
postgres_database_name = "weather_db"
posgresql_url = "jdbc:postgresql://" + postgres_hostname + ":" + postgres_port + "/" + postgres_database_name


loading_error = False
cities = ["Toronto", "Paris", "Miami", "Edmonton", "Dallas", "Vancouver", "Accra", "Madrid", "Los Angeles", "London"]
cities_coord = []
days_all_weather_list = []

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://postgres:thenewshit@localhost:5432/weather_db'
db = SQLAlchemy(app)


api_key = os.environ['API_KEY']
url = "https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={lat}&lon={lon}&dt={dt}&appid={api_key}"
url_imperial = "https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={lat}&lon={lon}&dt={dt}&appid={api_key}&units=imperial"
url_metric = "https://api.openweathermap.org/data/2.5/onecall/timemachine?lat={lat}&lon={lon}&dt={dt}&appid={api_key}&units=metric"
lat_lon_url = "https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

#units=imperial/metric

#select extract(epoch from now());

def write_table(df, table_name, schema=""):
    df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("driver", mysql_db_driver_class) \
        .option("url", "jdbc:mysql://localhost:3306/temperature_db") \
        .option("dbtable", table_name) \
        .option("user", "root") \
        .option("password", "thenewshit") \
        .save()
        

def read_table(spark, table_name, schema="public"):
    if schema:
        query = schema + "." + table_name
    else:
        query = table_name

    return spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/weather_db") \
        .option("driver", postgres_db_driver_class) \
        .option("dbtable", query) \
        .option("user", "postgres") \
        .option("password", "thenewshit") \
        .load()



class WeatherHistory (db.Model):
    id = db.Column(db.Integer, primary_key=True)
    city = db.Column(db.String(50))
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    clouds = db.Column(db.Integer)
    dew_point = db.Column(db.Float)
    dt = db.Column(db.DateTime)
    feels_like = db.Column(db.Float)
    humidity = db.Column(db.Integer)
    pressure = db.Column(db.Integer)
    temp = db.Column(db.Float)
    uvi = db.Column(db.Float)
    visibility = db.Column(db.Integer)
    weather_description: db.Column(db.Text)
    weather_icon = db.Column(db.String(50))
    weather_id = db.Column(db.Integer)
    weather_main = db.Column(db.String(50))
    wind_deg = db.Column(db.Integer)
    wind_speed = db.Column(db.Float)
    created_at = datetime.now()

    UniqueConstraint(city, dt, name='unique_weather')

    def __repr__(self):
        return '<Product %d>' % self.id

    def create(self):
        db.session.add(self)
        db.session.commit()
        return self
    

db.create_all()

class WeatherHistorySchema(SQLAlchemyAutoSchema):
  class Meta(SQLAlchemyAutoSchema.Meta):
    model = WeatherHistory
    sqla_session = db.session
  
    id = fields.Number(dump_only=True)
    city = fields.String(required=True)
    lat = fields.Float(required=True)
    lon = fields.Float(required=True)
    clouds = fields.Integer()
    dew_point = fields.Float()
    dt = fields.DateTime(required=True)
    feels_like = fields.Float()
    humidity = fields.Integer()
    pressure = fields.Integer()
    temp = fields.Float()
    uvi = fields.Float()
    visibility = fields.Integer()
    weather_description =fields.String()
    weather_icon = fields.String()
    weather_id = fields.Integer()
    weather_main = fields.String()
    wind_deg = fields.Integer()
    wind_speed = fields.Float()
    created_at = fields.DateTime()


def load_cities_coord():
    global loading_error
    for city in cities:
        try:
            response = requests.get(lat_lon_url.format(city=city, api_key=api_key))
            print(response)
            city_coord = json.loads(response.text)["coord"]
            print(city_coord)
            cities_coord.append( city_coord )
        except:
            loading_error = True
            print("Error occured getting location coordinates")


def get_prev_days_back_forcast(days_back):
    global loading_error
    day_all_city_weather_list = []
    for city_coord in cities_coord:
        dt = (datetime.today() - timedelta(days=days_back)).strftime("%s")
        try:
            response = requests.get(url_imperial.format(lat= city_coord["lat"], lon=city_coord["lon"], dt=dt, api_key=api_key))
            day_all_city_weather_list.append( json.loads(response.text) )
        except:
            loading_error = True
            print("An exception occured")
    return day_all_city_weather_list


def get_all_prev_days_forcast(days):
    load_cities_coord()
    for i in range(1, days+1):
        days_all_weather_list.append( get_prev_days_back_forcast(i) )

def parse_save():
    global loading_error
    
    get_all_prev_days_forcast(5)
    for day in days_all_weather_list:
        if day is not None:
            for city in day:
                #print(city)
                try:
                    lat = city["lat"]
                    lon = city["lon"]
                    city_name = city["timezone"].split("/")[1]
                    for hour in city["hourly"]:
                        dt=datetime.fromtimestamp( hour["dt"] )
                        rec = WeatherHistory.query.filter_by(city=city_name, dt=dt).first()
                        if rec is None:
                            # Create a new record
                            rec = WeatherHistory(city=city_name)
                            rec.lat = lat
                            rec.lon = lon
                            rec.clouds = hour["clouds"]
                            rec.dew_point = hour["dew_point"]
                            rec.dt = dt
                            rec.feels_like = hour["feels_like"]
                            rec.humidity = hour["humidity"]
                            rec.pressure = hour["pressure"]
                            rec.temp = hour["temp"]
                            rec.uvi = hour["uvi"]
                            rec.visibility = hour["visibility"]
                            rec.weather_description = hour["weather"][0]["description"]
                            rec.weather_icon = hour["weather"][0]["icon"]
                            rec.weather_id = hour["weather"][0]["id"]
                            rec.weather_main = hour["weather"][0]["main"]
                            rec.wind_deg = hour["wind_deg"]
                            rec.wind_speed = hour["wind_speed"]
                            db.session.add(rec)
                            db.session.commit()
                except:
                    print(city)
                    print("Some error in parsing city in day")
                    loading_error = True
        else:
            loading_error = True
            print("day is None")     

    if loading_error: 
        return "some errors occured ..."
    else:
        return "successfully loaded" 
                    
 
@app.route('/weather/test', methods = ['GET'])
def weather_test():
    return make_response( "testing")


@app.route('/weather/load', methods = ['GET'])
def weather_load():

    return make_response( jsonify( parse_save() ) )


@app.route("/weather/aggregate", methods = ['GET'])
def weather_aggregate():
    print("inside weather_aggregate")

    spark = SparkSession \
        .builder \
        .appName("Weather Aggregations") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.3.3,mysql:mysql-connector-java:8.0.28") \
        .getOrCreate()

    print(spark.sparkContext.appName)

    #weather_df = read_table(spark, "weather_history").cache()

    weather_query = "(select city, dt, temp from public.weather_history) as b"

    weather_df = read_table(spark, weather_query, "").cache()

    # A.join(B, ["id"], "inner")

    highest_temp_df = weather_df \
        .groupBy("city", F.month("dt").alias("month") ) \
        .agg( F.max("temp").alias("temp") ) \
        .join(weather_df.withColumn("month", F.month("dt") ), ["city", "month", "temp"], "inner") \
        .select("dt", "city", "temp") \
        .withColumn("created_at", F.current_timestamp() ) \
        .withColumn("id", F.monotonically_increasing_id() )

    agg_df = weather_df \
        .groupBy( F.to_date("dt").alias("date") ) \
        .agg( F.avg("temp").alias("avg_temp"), F.min("temp").alias("min_temp"), F.max("temp").alias("max_temp") ) \
    
    agg_with_min_temp_city_df = weather_df \
        .join( F.broadcast(agg_df), (F.to_date(weather_df.dt) == agg_df.date) & (weather_df.temp == agg_df.min_temp) ) \
        .withColumn("min_temp_city", F.col("city") ) \
        .drop("city", "dt") \
        .dropDuplicates()

    summary_temp_df = weather_df.alias("a") \
        .join( F.broadcast(agg_with_min_temp_city_df).alias("b") , (F.to_date( F.col("a.dt") ) == F.col("b.date") ) & (F.col("a.temp") == F.col("b.max_temp")) ) \
        .withColumn("max_temp_city", F.col("city") ) \
        .selectExpr("date", "avg_temp", "min_temp", "min_temp_city", "max_temp", "max_temp_city") \
        .withColumnRenamed("date", "dt") \
        .dropDuplicates() \
        .withColumn("created_at", F.current_timestamp() ) \
        .withColumn("id", F.monotonically_increasing_id() )

    write_table(highest_temp_df, "highest_temperature")
    write_table(summary_temp_df, "summary_temperature")

    return make_response( jsonify(spark.sparkContext.appName), 200)    
 

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3001, debug=True)


# posted_at=datetime.datetime.utcnow()