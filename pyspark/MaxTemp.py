from pyspark import SparkContext
from utils import DATA_DIR


def read_file(line: str):
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9 / 5) + 32
    return station_id, entry_type, temperature


# Create a SparkContext using every core of the local machine
sc = SparkContext("local[*]", "MaxTemp")

# Set the log level to only print errors
sc.setLogLevel("ERROR")

# Read each line of input data
input_ds = sc.textFile(DATA_DIR + "1800.csv")

# Convert to (stationID, entryType, temperature) tuples
parsed_ds = input_ds.map(read_file)

# Filter out all but TMAX entries
max_temp_ds = parsed_ds.filter(lambda x: x[1] == "TMAX")

# Convert to (stationID, temperature)
station_temps_ds = max_temp_ds.map(lambda x: (x[0], x[2]))

# Reduce by stationID retaining the minimum temperature found
max_temps_by_station_ds = station_temps_ds.reduceByKey(lambda x, y: max(x, y))

# Collect the results
results_ds = max_temps_by_station_ds.collect()

# Sort the results
results_ds.sort()

# Print the formatted results
for result in results_ds:
    station = result[0]
    formatted_temp = "{:.2f}".format(result[1])
    print(station + " max temperature: " + formatted_temp + " F")
