import pandas as pd
import os
from applyw2v import tokenizeCode, filterNonsense, joinstatement

"""
Create dataframe using pandas to classify which statements are buggy 
"""

# Path on local machine
data_path = "/Users/farukhsaidmuratov/PycharmProjects/bug-classification/data/"

# Create data frame to hold code line and bug classification (1 for bug, 0 for no bug)
buggy_code_df = pd.DataFrame(columns=['File', 'Statement', 'Bug'])

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

# Tokenize the lists in the dict
for file in buggy_dict.keys():
    sents = [tokenizeCode(line) for line in buggy_dict[file]]
    filtered_sentences = [sent for sent in sents if filterNonsense(sent)]
    joined_sentences = joinstatement(filtered_sentences)

    buggy_dict[file] = joined_sentences

# For each file in data classify as bug if line of code appears in dictionary, with file name as the key and with line
# of code in the list
for filename in os.listdir('data'):
    # Open file to read and record statements
    with open(data_path + filename, 'r') as f:
        lines = f.readlines()
        # Tokenize
        sents = [tokenizeCode(line) for line in lines]
        filtered_sentences = [sent for sent in sents if filterNonsense(sent)]
        joined_sentences = joinstatement(filtered_sentences)

        for sentence in joined_sentences:
            if sentence in buggy_dict[filename]:
                buggy_code_df = buggy_code_df.append({'File': filename, 'Statement': sentence, 'Bug': 1}, ignore_index=True)
            else:
                buggy_code_df = buggy_code_df.append({'File': filename, 'Statement': sentence, 'Bug': 0}, ignore_index=True)

# Save as csv file
buggy_code_df.to_csv(path_or_buf="bug-classification.csv", index=False)

