## directory where the gps files are located
GPS_FILE_DIRECTORY = "chiledata/"

## file extension of the gps files
GPS_FILE_EXTENSION = "*.xlsx"


## name of the excel worksheet with GPS points
WORKSHEET_NAME = "Detalle"


# minimum time at a given location that makes it a "stop" for the vehicle
MIN_STOP_TIME = 10

# radius of 20 meters for stop location.
# if a gps point is recorded within this variable meter radius of a stop location
# it is considered part of the stop location
CONSTRAINT = .02