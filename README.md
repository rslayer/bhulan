# bhulan
An opensource python library for GPS data processing
[Bhulan](https://en.wikipedia.org/wiki/Indus_river_dolphin) enables quick processing of raw gps data to identify properties of a vehicle's movements within the given gps trace. Using the api, you can identify a vehicle's route, stops, duration of stops, schedules, and clusters of service. 

# requirements
* [MongoDb 3.0.3](https://docs.mongodb.org/getting-started/shell/installation/)
* [Python 2.7](https://www.python.org/download/releases/2.7/)

# setup
* use the *init.py* file to setup the following initializing parameters:
    * File Directory - directory location of the GPS files to be processed
    * File Extension - extension of the file. This will determine how the system imports the file. Currently supporting excel files. Future iterations will have support for csv files
* run *setup.py* to import trucks, compute properties, and compute stops

# key properties
* getTruckPoints - returns all the truck points for a given truck (truckId). It will return the points for that day if date is proved (datenum)
* getStopsFromTruckDate – returns all stops for that truck and date
* getStopPropsFromTruckDate – returns all stop properties for that truck and date
* getStopStatistics – returns key stats for stops. Can return for specific truck and date.
* getTruckScheduleForDay – returns a truck schedule for a given day – schedule is the order of stops for that day.
* findDCs – returns the estimated location of a Distribution Center based on the stop data

# stop and stop properties
* A Stop is the 20 meter geographical radius where we have observed multiple gps points from a truck for greater than 10 minutes. 
* A StopProperty contains details of each observed point within a Stop (20 meter radius location).
* A stop will have multiple stop properties. 