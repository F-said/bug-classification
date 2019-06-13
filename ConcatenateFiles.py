import os

"""
Script to combine all buggy files into one document
"""

# Path on local machine
path = "/Users/farukhsaidmuratov/PycharmProjects/bug-classification/data/"

# Write one java file to contain all code
data = open("PigCode.java", "w")

# For each file in data
for filename in os.listdir('data'):
    # Write it to the data file
    data.writelines([l for l in open(path + filename, "r").readlines()])

data.close()
