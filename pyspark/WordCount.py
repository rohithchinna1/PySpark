from pyspark import SparkContext
from utils import DATA_DIR

# Create a SparkContext using every core of the local machine
sc = SparkContext("local[*]", "WordCount")

# Set the log level to only print errors
sc.setLogLevel("ERROR")

# Read each line of input data
input_ds = sc.textFile(DATA_DIR + "book.txt")

# Split into words separated by a space character
words = input_ds.flatMap(lambda x: x.split(" "))

# Count the occurrences of each word countByValue returns dictionary
wordCounts = words.countByValue()

# Print the results
for (key, value) in wordCounts.items():
    print((key, value))
