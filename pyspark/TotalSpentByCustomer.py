from pyspark import SparkContext
from utils import DATA_DIR


def transform_data(line: str):
    # Split by commas
    fields = line.split(",")
    # Extract the customer id and total spent fields
    customer_id = int(fields[0])
    total_spent = float(fields[2])
    # Return a tuple
    return customer_id, total_spent


# Create a SparkContext using every core of the local machine
sc = SparkContext("local[*]", "TotalSpentByCustomer")

# Set the log level to only print errors
sc.setLogLevel("ERROR")

# Read each line of input data
input_ds = sc.textFile(DATA_DIR + "customer-orders.csv")

# Split using comma and extract (customer id, total spent) tuple and reduce by customer id
# Then flip the tuple to (total spent, customer id) and sort by total spent
output_ds = input_ds\
            .map(transform_data)\
            .reduceByKey(lambda x, y: x + y)\
            .map(lambda x: (x[1], x[0]))\
            .sortByKey()\
            .collect()

# Print the results
for result in output_ds:
    customer = str(result[1])
    total = "{:.2f}".format(result[0])
    print("Customer " + customer + " spent a total of $" + total)
