import os
from datetime import datetime, timedelta
import json
from flask import Flask, request, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
from marshmallow_sqlalchemy import SQLAlchemyAutoSchema
from marshmallow import fields


app = Flask(__name__)

mysql_db_driver_class = "com.mysql.cj.jdbc.Driver"
mysql_hostname = os.environ['MYSQL_HOST']
mysql_port = os.environ['MYSQL_PORT']
mysql_username = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PASSWORD']
mysql_database_name = os.environ['MYSQL_DATABASE']
mysql_url = "jdbc:mysql://" + mysql_hostname + ":" + mysql_port + "/" + mysql_database_name

# "mysql+pymysql://user:pass@some_mariadb/dbname
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://' + mysql_username + ':' + mysql_password + '@' + mysql_hostname + ':' + mysql_port + '/' + mysql_database_name


db = SQLAlchemy(app)

class HighestTemperature (db.Model):
    id = db.Column(db.Integer, db.Identity(start=1, cycle=True), primary_key=True)

    city = db.Column(db.String(50))
    dt = db.Column(db.DateTime)
    temp = db.Column(db.Float)

    created_at = db.Column(db.DateTime)

    def __repr__(self):
        return '<Product %d>' % self.id

    def create(self):
        db.session.add(self)
        db.session.commit()
        return self


class SummaryTemperature (db.Model):
    id = db.Column(db.Integer, primary_key=True)

    max_temp_city = db.Column(db.String(50))
    min_temp_city = db.Column(db.String(50))
    dt = db.Column(db.Date)
    avg_temp = db.Column(db.Float)
    min_temp = db.Column(db.Float)
    max_temp = db.Column(db.Float)

    created_at = db.Column(db.DateTime)

    def __repr__(self):
        return '<Product %d>' % self.id

    def create(self):
        db.session.add(self)
        db.session.commit()
        return self        
    

db.create_all()


class HightestTemperatureSchema(SQLAlchemyAutoSchema):
  class Meta(SQLAlchemyAutoSchema.Meta):
    model = HighestTemperature
    sqla_session = db.session
  
    id = fields.Number(dump_only=True)

    city = fields.String()
    dt = fields.DateTime(required=True)
    temp = fields.Float()

    created_at = fields.DateTime()


class SummaryTemperatureSchema(SQLAlchemyAutoSchema):
  class Meta(SQLAlchemyAutoSchema.Meta):
    model = SummaryTemperature
    sqla_session = db.session
  
    id = fields.Number(dump_only=True)

    max_temp_city = fields.String()
    min_temp_city = fields.String()
    dt = fields.DateTime(required=True)
    avg_temp = fields.String()
    min_temp = fields.Float()
    max_temp = fields.Float()

    created_at = fields.DateTime()    

 
@app.route('/temperature/highest', methods = ['GET'])
def temperature_hightest():

    get_hightest_temp = HighestTemperature.query.all()
    hightest_temp_schema = HightestTemperatureSchema()
    hightest_temperatures = hightest_temp_schema.dump(get_hightest_temp, many=True)

    return make_response(jsonify({"highest_temperatures": hightest_temperatures}))


@app.route('/temperature/summary', methods = ['GET'])
def weather_load():
    get_temp_summary = SummaryTemperature.query.all()
    summary_temp_schema = SummaryTemperatureSchema()
    summary_temp = summary_temp_schema.dump(get_temp_summary, many=True)

    return make_response(jsonify({"summary_temperatures": summary_temp}))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3002, debug=True)

