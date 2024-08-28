data = LOAD '/path/to/data.csv' USING PigStorage(',')
       AS (CityName:chararray, Location:chararray, Year:int, Temperature:float, State:chararray, GasPresent:chararray, QuantityofGas:float);

filtered_data = FILTER data BY Temperature IS NOT NULL;
max_temperature = FOREACH (GROUP filtered_data ALL) GENERATE MAX(filtered_data.Temperature) AS MaxTemp;
DUMP max_temperature;
