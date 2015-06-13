# bhulan
An opensource python library for GPS data processing
[Bhulan](https://en.wikipedia.org/wiki/Indus_river_dolphin) enables quick processing of raw gps data to identify properties of a vehicle's movements within the given gps trace. Using the api, you can identify a vehicle's route, stops, duration of stops, schedules, and clusters of service. 

# requirements
* [MongoDb 3.0.3](https://docs.mongodb.org/getting-started/shell/installation/)
* [Python 2.7](https://www.python.org/download/releases/2.7/)

# setup
* use the *init.py* file to setup the initial parameters for processing such as import file location, type etc.
* run *setup.py* to import trucks, compute properties, and compute stops
