import gzip
import gensim
import logging
import os
import re

"""
Train word2vec model on PigCode.java.
Idea: If end result isn't accurate, perhaps add a lot more non-buggy code?
"""

data_file = "PigCode.java"

# Consider each statement as a sentence. Therefore partition the code into a list of strings separated by semicolons
# and brackets
with open(data_file, "r") as myfile:
    data = myfile.read()

# Remove new lines
data = data.replace("\n", "")

# Split by ";" "}","{".
code_list = re.split(";|{|}", data)
