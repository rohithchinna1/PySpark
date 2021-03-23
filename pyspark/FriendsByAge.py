from pyspark import SparkContext
from utils import DATA_DIR


# Function that splits a line of input into (age, numFriends) tuples.
def read_file(line: str):
    # Split by commas
    fields = line.split(",")
    # Extract the age and numFriends fields, and convert to integers
    age = int(fields[2])
    num_friends = int(fields[3])
    # Return a tuple
    return age, num_friends


# Create a SparkContext using every core of the local machine
sc = SparkContext("local[*]", "FriendsByAge")

# Set the log level to only print errors
sc.setLogLevel("ERROR")

# Load each line of the source data into an RDD
input_ds = sc.textFile(DATA_DIR + "fakefriends-noheader.csv")

# Use read_file function to convert to (age, numFriends) tuples
parsed_ds = input_ds.map(read_file)

# Starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
# Use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
# Then use reduceByKey to sum up the total numFriends and total instances for each age, by
# adding together all the numFriends values and 1's respectively.
totals_by_age_ds = parsed_ds.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# Tuples of (age, (totalFriends, totalInstances))
# To compute the average divide totalFriends / totalInstances for each age.
averages_by_age_ds = totals_by_age_ds.mapValues(lambda x: x[0] // x[1])

# Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
results_ds = averages_by_age_ds.collect()

# Sort the results
results_ds.sort()

# Print the results
for result in results_ds:
    print(result)
