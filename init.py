## directory where the gps files are located
GPS_FILE_DIRECTORY = "sampledata/"

## file extension of the gps files
# for now only handling excel files
GPS_FILE_EXTENSION = "*.xlsx"


## name of the excel worksheet with GPS points
WORKSHEET_NAME = "Detalle"


# minimum time at a given location that makes it a "stop" for the vehicle
MIN_STOP_TIME = 10

# radius of 20 meters for stop location.
# if a gps point is recorded within this variable meter radius of a stop location
# it is considered part of the stop location
CONSTRAINT = .02

# hours that a vehicle has to stay at a stop for it to be considered a DC or home
DC_HOURS = 4

# radius in miles, to create a geo-fence around a city center and only
# consider points within that zone.
SANTIAGO_RADIUS = 60

# lat long for Santiago, used to calculate points within the city
# this is a hack, needs to be fixed
SANTI_LAT = '-33.469994'
SANTI_LON = '-70.642193'

# url to access cartodb
CARTO_URL = "https://<username>.cartodb.com/api/v1/imports/?api_key="

# api key for CARTO_DB. set this before sending data to cartodb
CARTO_DB_API_KEY = "<API_KEY_HERE>"
