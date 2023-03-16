import numpy as np,pandas as pd,datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine,reflect=True)

# Save reference to the table
Base.classes.keys()

# Save reference to the table
Measurement = Base.classes.measurement

Station = Base.classes.station
# Create our session (link) from Python to the DB
session = Session(engine)
#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        "Available Routes:<br/>/api/v1.0/precipitation:<br/>api/v1.0/stations:<br/>/api/v1.0/tobs:<br/>"
        )

#
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    recent_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first().date
    last_year = dt.datetime.strptime(recent_date, '%Y-%m-%d') - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores

    prcp_data = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= last_year).all()

    # Save the query results as a Pandas DataFrame and set the index to the date column
    prcp_df = pd.DataFrame(prcp_data,columns = ["Date","Precipitaion"]).sort_values(by="Date")
    prcp_df.set_index(prcp_df['Date'], inplace=True)
    # Sort the dataframe by date
    session.close()

    """Precipitation analysis (i.e. retrieve only the last 12 months of data)."""
    # Query precipitation data
    result = {}
    for index, row in prcp_df.iterrows():
        #result[index] = row.to_json() 
        result[index] = dict(row)
    return jsonify(result)

#Return a JSON list of stations from the dataset

@app.route('/api/v1.0/stations')
def stations():
    session = Session(engine)
    station = session.query(Station.station).all()
    
    d = {k[0]:'' for k in station}
    session.close()
    
    return jsonify(d)

#Query the dates and temperature observations of the most-active station for the previous year of data.
#Return a JSON list of temperature observations for the previous year.

@app.route('/api/v1.0/tobs')
def most_active():
    session = Session(engine)
    most_active = session.query(Measurement.station, func.count(Measurement.station)).\
              group_by(Measurement.station).\
              order_by(func.count(Measurement.station).desc()).all()
    print(most_active)
    session.close()

# Select the station with the highest number of temperature observations
    most_active_station = most_active[0][0]
    
# Query the most active station's temperature data for the previous year
    temps_last_year = session.query(Measurement.tobs).\
                      filter(Measurement.station == most_active_station).\
                      filter(Measurement.date >= '2016-08-23').all()

# Convert the query results to a list
    temps_list = list(np.ravel(temps_last_year))

    # Return the JSON representation of the list
    return jsonify(temps_list)




# 5
# /api/v1.0/<start> and /api/v1.0/<start>/<end>
# Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start or start-end range.
# For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or equal to the start date.
# For a specified start date and end date, calculate TMIN, TAVG, and TMAX for the dates from the start date to the end date, inclusive.



@app.route('/api/v1.0/<start>')
@app.route('/api/v1.0/<start>/<end>')
def min_max_avg(start, end=None):
    session = Session(engine)
    if end:
        results = session.query(func.min(tobs), func.avg(tobs), func.max(tobs)).\
                  filter(Measurement.date >= start).filter(Measurement.date <= end).all()
    else:
        results = session.query(func.min(tobs), func.avg(tobs), func.max(tobs)).\
                  filter(Measurement.date >= start).all()

    session.close()

    all_temps = []
    for min_temp, avg_temp, max_temp in results:
        temp_dict = {}
        temp_dict['min_temp'] = min_temp
        temp_dict['avg_temp'] = avg_temp
        temp_dict['max_temp'] = max_temp
        all_temps.append(temp_dict)

    return jsonify(all_temps)

if __name__ == '__main__':
    app.run(debug=True)

