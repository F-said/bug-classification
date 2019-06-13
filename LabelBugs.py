import pandas as pd
import os
from applyw2v import tokenizeCode, filterNonsense, joinstatement

"""
Create dataframe using pandas to classify which statements are buggy 
"""

# Create data frame to hold code line and bug classification (1 for bug, 0 for no bug)
buggy_code_df = pd.DataFrame(columns=['Statement', 'Bug'])

# Create dict of lists of buggy lines with keys being file names
filetype = "java"
buggyfiles = open("PigBuggyLines.txt", "r")
buggy_dict = {}

# Extract paths of buggy lines
for file in buggyfiles:
    # If line contains file name
    if file[-5:-1] == filetype:
        # Record its name
        begin_ind = file.rfind("/") + 1
        file_name = file[begin_ind:-1]

        # If file not already recorded then record it in dict with buggy line of code as first element in list
        if file_name not in buggy_dict.keys():
            buggy_lines = list()
            buggy_lines.append(buggyfiles.readline())
            buggy_dict[file_name] = buggy_lines
        # If already recorded, then just append buggy line to list
        else:
            buggy_dict[file_name].append(buggyfiles.readline())

# For each file in data
for filename in os.listdir('data'):
    # Write it to the data file
    data.writelines([l for l in open(path + filename, "r").readlines()])

with open("PigCode.java", 'r') as f:
    lines = f.readlines()
sents = [tokenizeCode(line) for line in lines]
filtered_sentences = [sent for sent in sents if filterNonsense(sent)]
joined_sentences = joinstatement(filtered_sentences)

print("Somebody once told me")
