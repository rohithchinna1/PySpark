from pyspark import SparkContext
from utils import DATA_DIR
import re

# Create a SparkContext using every core of the local machine
sc = SparkContext("local[*]", "WordCount")

# Set the log level to only print errors
sc.setLogLevel("ERROR")

# Read each line of input data
input_ds = sc.textFile(DATA_DIR + "book.txt")

# Split using regular expression to extract only words
words = input_ds.flatMap(lambda x: re.split("\\W+", x))

# Normalize everything to lower case
words_lower = words.map(lambda x: x.lower())

# Count the occurrences of each word
word_counts = words_lower.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# Flip (word, count) tuples to (count, word) and then sort by key (the counts)
word_counts_sorted = word_counts.map(lambda x: (x[1], x[0])).sortByKey()

# Print the results
for result in word_counts_sorted.collect():
    print(result[1] + ": " + str(result[0]))
