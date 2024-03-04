-- Creating the new flight_delays table to be populated with the data from the DelayedFlights-updated.csv
CREATE TABLE flight_delays (
    Id INT,
    Year INT,
    Month INT,
    DayofMonth INT,
    DayOfWeek INT,
    DepTime INT,
    CRSDepTime INT,
    ArrTime INT,
    CRSArrTime INT,
    UniqueCarrier STRING,
    FlightNum INT,
    TailNum STRING,
    ActualElapsedTime INT,
    CRSElapsedTime INT,
    AirTime INT,
    ArrDelay DOUBLE,
    DepDelay DOUBLE,
    Origin STRING,
    Dest STRING,
    Distance INT,
    TaxiIn INT,
    TaxiOut INT,
    Cancelled INT,
    CancellationCode STRING,
    Diverted DOUBLE,
    CarrierDelay INT,
    WeatherDelay INT,
    NASDelay INT,
    SecurityDelay INT,
    LateAircraftDelay INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://my-uom-aws-bucket/Hive/';

-- Loading data into the newly created hive table
LOAD DATA INPATH 's3://my-uom-aws-bucket/DelayedFlights-updated.csv' OVERWRITE INTO TABLE flight_delays;

-- Year wise carrier delay from 2003-2010
SELECT Year, avg((CarrierDelay /ArrDelay)*100) from fligt_delays GROUP BY Year;

-- Year wise NAS delay from 2003-2010
SELECT Year, avg((NASDelay /ArrDelay)*100) from fligt_delays GROUP BY Year;

-- Year wise Weather delay from 2003-2010
SELECT Year, avg((WeatherDelay /ArrDelay)*100) from fligt_delays GROUP BY Year;

-- Year wise late aircraft delay from 2003-2010
SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) from fligt_delays GROUP BY Year;

-- Year wise security delay from 2003-2010
SELECT Year, avg((SecurityDelay /ArrDelay)*100) from fligt_delays GROUP BY Year;





