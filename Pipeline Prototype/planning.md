
## Data Collection

### Bike Data

### Taxi Data

#### Yellow Taxi
-  [x] Write extraction function, extracts the data, and adds it to a queue to be processed, and ensures that the column names match that of the green taxi data, should grab small chunks of rows
#### Green Taxi
-  [x] Write extraction function, extracts the data, and adds it to a queue to be processed, and ensures that the column names match that of the green yellow data, should grab small chunks of rows
#### Taxi Zone Data
-  [x] Write extraction function, extracts the data, and adds it to a queue to be processed, and ensures that the column names match that of the green yellow data

### Weather Data

## Cleaning & Transforming

### Bike Data
-  [x] Pulling out the bike station locations and leaving behind a primary key
-  [x] Attributing each bike station to a taxi region
### Taxi Data

#### Yellow Taxi & Green Taxi
-  [x] Attributing each ride with a taxi zone
-  [x] Loads regions from database
-  [x] Edit processing to use spatial joins instead of going row by row
    - This can be done by re-writing the attribute zones function in etl.transform, to take in a gdf, and a regions gdf, should return the joined result of these two
    - Pre-processing will require creating a dataframe with Point objects for the start and end point instead of the lat, long coords. These will be grabbed from one gdf, and joined back together using the original index

### Date Dimensions
-  [x] Create a table of all distinct dates truncated to the hour
-  [x] Add unix column in UTC time
-  [x] Add time of day, day of week etc

### Weather Data

## Writing Data

### Taxi Data

#### Taxi Zone Data
-  [ ] Write polygons to postgres