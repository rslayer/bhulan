# bhulan
An opensource python library for GPS data processing
[Bhulan](https://en.wikipedia.org/wiki/Indus_river_dolphin) enables quick processing of raw gps data to identify properties of a vehicle's movements within the given gps trace. Using the api, you can identify a vehicle's route, stops, duration of stops, schedules, and clusters of service among other properties. 

# base requirements
* [MongoDb 3.0.3](https://docs.mongodb.org/getting-started/shell/installation/)
* [Python 2.7](https://www.python.org/download/releases/2.7/)

# setup
* use the *init.py* file to setup the following initializing parameters:
    * File Directory - directory location of the GPS files to be processed
    * File Extension - extension of the file. This will determine how the system imports the file. Currently supporting excel files. Future iterations will have support for csv files
* run *setup.py* to import trucks, compute properties, and compute stops

# import file format
* the gps data to be processed by bhulan must be provided in a designated format
* the input files must contain the following columns in the given format:
    * vehicle id - unique identifier of the vehicle
    * date and time - the date and time of the record. the date must be provided in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601)
    * latitude - the current latitude of the vehicle location
    * longitude - the current longitude of the vehicle location
    * direction - the direction of the vehicle
    * velocity - the current observed velocity of the vehicle
    * temperature - the current outside temperature
    
refer to the sample file in the sampledata folder. 

# internal date formats - datenum
A pseudo indicator for date called DateNum is used to make processing of dates easy. 
DateNum is calculated as below:
>Datenum = month_num * 31 + day_of_month

For June 30th, the month_num will be 6 while day_of_month will be 30. Therefore the datenum for June 30th will be 216.

# key properties
* Truck Points (getTruckPoints) - returns all the truck points for a given truck (truckId). It will return the points for that day if date is proved (datenum)
* Stops for Truck and Date (getStopsFromTruckDate) – returns all stops for that truck and date
* Stop Properties for Truck and Date (getStopPropsFromTruckDate) – returns all stop properties for that truck and date
* Truck Schedule for Date (getTruckScheduleForDay) – returns a truck schedule for a given day – schedule is the order of stops for that day.
* Find Potential DC Locations (findPotentialDCs) – returns the estimated location of a Distribution Center based on the stop data
* Total Distance Traveled for Truck and Date (getTotalDistanceTraveled) - returns the total distance traveled for a truck on a given day
* Total Time on Road for Truck and Date (getTotalTimeOnRoad) - returns the total time on road traveled for a truck on a given day
* Average Speed for Truck and Date (getAverageSpeedByDatenum) - returns the average speed on road for truck on a given day

# stop and stop properties
* A Stop is the 20 meter geographical radius where we have observed multiple gps points from a truck for greater than 10 minutes. 
* A StopProperty contains details of each observed point within a Stop (20 meter radius location).
* A stop will have multiple stop properties.