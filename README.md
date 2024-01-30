# Taxi Trips

A Capstone project for a Springboard Data Engineering Bootcamp operated by Washington University 

### Contents

 - Initial Data Collection files
    - data_collections.py
        - A file containing extraction functions
    - data_collection_nb.ipynb
        - A python notebook that was used to pull the datasets using the extraction functions

    
### File Sizes

Green Taxi 2014 (2.37GB)

Yellow Taxi 2014 (27.1GB)

Taxi Zones (3.86MB)

CitiBike Trip Data (720MB *estimated*)
    - Some files are still compressed, so the 720MB estimate is based on the first three months data files.

# Pivoting

## New Problem Statement

An phenomenon I have heard of in NYC is that it can be faster to get somewhere by bike than by car. This is believable, but NYC is a large place; and for a visitor, or new resident, this may be difficult to determine. This project aims to allow a visitor or new resident of NYC to check if their trip is likely to be faster by bike, or by taxi. And what the weather would be like in the case that they were to bike.

## Data Sets

 - **Taxi Trip Level Data**
    - 2014 Green Taxi
        - https://data.cityofnewyork.us/resource/2np7-5jsg.csv
    - 2014 Yellow Taxi
        - https://data.cityofnewyork.us/resource/gkne-dk5s.csv
 - **Taxi Region Data**
    - https://data.cityofnewyork.us/resource/755u-8jsi.csv
 - **CitiBike Trip Data**
    - https://s3.amazonaws.com/tripdata/yyyymm-citibike-tripdata.zip
 - **Historical Weather Data (Open Weather API)**
    - Can get weather data for a lat, lon at a specified timestamp. 
 - **Geocoding start & end points**
    - Google Maps Api and Open Maps API for geocoding. Google maps to be tested at a later date, since the trial period is 90 days, and I would like to continue using free credit.