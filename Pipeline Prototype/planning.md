
## Orchestration
### Airflow

-  [ ] DAG for region data
-  [ ] DAG for bike trip data
-  [ ] DAG for yearly taxi data
-  [ ] DAG for google maps API geocode data
-  [ ] DAG for open weather API data

## Data Collection

### Bike Data

### Taxi Data

#### Yellow Taxi
-  [ ] Write extraction function, extracts the data, and adds it to a queue to be processed, and ensures that the column names match that of the green taxi data, should grab small chunks of rows
#### Green Taxi
-  [ ] Write extraction function, extracts the data, and adds it to a queue to be processed, and ensures that the column names match that of the green yellow data, should grab small chunks of rows
#### Taxi Zone Data
-  [ ] Write extraction function, extracts the data, and adds it to a queue to be processed, and ensures that the column names match that of the green yellow data

### Weather Data

## Cleaning & Transforming

### Bike Data
-  [x] Pulling out the bike station locations and leaving behind a primary key
-  [x] Attributing each bike station to a taxi region
### Taxi Data

#### Yellow Taxi & Green Taxi
-  [x] Attributing each ride with a taxi zone

### Date Dimensions
-  [x] Create a table of all distinct dates truncated to the hour
-  [x] Add unix column in UTC time
-  [x] Add time of day, day of week etc
-  [ ] Add holidays

### Weather Data

## Writing Data

### Taxi Data

#### Taxi Zone Data
-  [ ] Write polygons to postgres