###########################################################################################
#  RUT-SOM-DATA-PT-06-2020-U-C                                               Douglas High #
#   SQL-Alchemy                                                           August 17, 2020 #
#      >climate_app                                                                       #
#   - coded in conjunction with climate_analysis.ipynb.                                   #
#   - use flask to create home page and various secondary pages returning query results   #
#     against weather observation data from Hawaii.                                       #
###########################################################################################

##############################################################
#00       I/O                                                #
#   a- import libraries.                                     #
#   b- change directory path (for VS code editor).           #
#   c- database link, reflections, and session connection.   #
#   d- associate tables to variables.                        #
#   e- flask setup.                                          #
##############################################################

#a
import os
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
import datetime as dt

#b
os.chdir(os.path.dirname(os.path.abspath(__file__)))

#c
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
Base = automap_base()
Base.prepare(engine, reflect=True)
session = Session(engine)

#d
Measurement = Base.classes.measurement
Station = Base.classes.station

#e
app = Flask(__name__)

################################################################################################################
#01        Prep Work                                                                                           #
#   - note- these are mostly re-creations of code from climate_analysis.ipynb, more detailed descriptions of   #       
#           processes can be found there (referring section numbers listed next to letter).                    #
#   a(01)- get precipitation data for past year, convert results to dictionary.                                #
#     note - results returned from all stations, dictionary date key must be unique, takes last occurance.     #
#   b- get list of stations, convert to dictionary.                                                            #
#   c(04.c)- get list of stations and count of overall activity, sort deascending.                             #
#   d(04.d-05)- get temperature readings from past year at most active station.                                #
################################################################################################################

#a
sorted = session.query(Measurement.date).order_by(Measurement.date.desc()).all()
most_recent = sorted[0][0]

year,month,day = most_recent.split('-')
year,month,day = int(year), int(month), int(day)

last_year = dt.date(year,month,day)- dt.timedelta(days=365)

past_year_rain = session.query(Measurement.date, Measurement.prcp).\
                               filter(Measurement.date >= last_year).\
                               filter(Measurement.prcp != "None").\
                               order_by(Measurement.date).all()

rain_dict = {}
for r in past_year_rain:
    rain_dict[r[0]] = r[1]

#b
stations_list = session.query(Station.station, Station.name).all()
stations_dict = {}    
for s in stations_list:
    stations_dict[s[0]] = s[1]

#c
station_activity = session.query(Measurement.station,func.count(Measurement.station)).\
                                 group_by(Measurement.station).\
                                 order_by(func.count(Measurement.station).desc()).all()

#d
pastyear_temps_mostactive_station = session.query(Measurement.date, Measurement.tobs).\
                                                  filter(Measurement.station == station_activity[0][0]).\
                                                  filter(Measurement.date >= last_year).\
                                                  order_by(Measurement.date).all()

most_active_dict = {}    
for m in pastyear_temps_mostactive_station:
    most_active_dict[m[0]] = m[1]

########################################################################
#02    Static Pages                                                    #
#   a- home page: list available routes.                               #
#   b- precipitation page: jsonified dictionary of rain data.          #
#   c- station page: jsonified dictionary of stations.                 #
#   d- tobs page: past years temp readings from most active station.   #
########################################################################

#a
@app.route("/")
def home():
    return (f"Climate Analysis Homepage<br/><br/>"
            f"   Available Routes:<br/><br/>"
            f"/api/v1.0/precipitation :  Rainfall data for past year, in inches<br/>"
            f"/api/v1.0/stations : List of all reporting stations<br/>"
            f"/api/v1.0/tobs :  Daily temperature readings for the last year from most active station: {station_activity[0][0]}<br/><br/>"
            f"  For the following two pages, replace &ltstart&gt and &ltend&gt with a date in the form yyyy-%m-%d "
            f"    to get the lowest, average, and highest temperatures<br/>"
            f"note: oldest date on table is 2010-01-01, newest date is 2017-08-23<br/><br/>"
            f"/api/v1.0/&ltstart&gt : Min, avg, max temp from start date to now (2017-08-23), inclusive<br/>"
            f"/api/v1.0/&ltstart&gt/&ltend&gt :  Min, avg, max temp from start date to end date, inclusive")

#b
@app.route("/api/v1.0/precipitation")
def precipitation():
    return jsonify(rain_dict)

#c
@app.route("/api/v1.0/stations")
def stations():
    return jsonify(stations_dict)

#d
@app.route("/api/v1.0/tobs")
def tobs():
    return jsonify(most_active_dict)

###########################################################################################
#03    Dynamic Pages                                                                      #
#   a- get min, max, and avg temperature from provided start date through end of table.   #
#   b- as above but end date is provided.                                                 #
#   - if start date before 2010-01-01 or end date beyond 2017-08-23 (table limits)        #
#     then date(s) provided are overwritten with table limits.                            #
#   - if start date is beyond eot or after end date, error page is displayed.             #
#      (note-sql queries accepted odd dates i.e 2011-44-55)                               #
###########################################################################################

#a
@app.route("/api/v1.0/<start>")
def start_date(start):
    session = Session(engine)
    if start < "2010-01-01":
        start = "2010-01-01"
    if start > "2017-08-23":
        return (f"Start Date is Beyond Last Date on Table (2017-08-23)")

    tmin, tavg, tmax = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                       filter(Measurement.date >= start).first()
    session.close()
    tavg = round(tavg, 2)

    return (f"Temperature data from {start} to {most_recent} (most recent)<br/>"
            f"Minimum temperature: {tmin} degrees fahrenheit<br/>" 
            f"Maximum Temperature: {tmax} degrees fahrenheit<br/>"
            f"Average Temperature: {tavg} degrees fahrenheit")

#b
@app.route("/api/v1.0/<start>/<end>")
def start_end_date(start,end):
    session = Session(engine)
    if start < "2010-01-01":
        start = "2010-01-01"
    if end > "2017-08-23":
        end = "2017-08-23"
    if end < start:
        return (f"Error: End Date Can Not be Before Start Date")
    if start > "2017-08-23":
        return (f"Error: Start Date is Beyond Last Date on Table (2017-08-23)")

    tmin, tavg, tmax = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                       filter(Measurement.date >= start).filter(Measurement.date <= end).first()
    session.close()
    tavg = round(tavg, 2)
    return (f"Temperature data from {start} to {end}<br/>"
            f"Minimum temperature: {tmin} degrees fahrenheit<br/>" 
            f"Maximum Temperature: {tmax} degrees fahrenheit<br/>"
            f"Average Temperature: {tavg} degrees fahrenheit")

#######################################
#99     Flask Main                    #
#   - define flask 'main' behavior.   #
#######################################
if __name__ == "__main__":
    app.run(debug=True)