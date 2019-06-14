from Simple_rnn_and_simple_cnn import simple_cnn, simple_rnn
import pandas as pd
from numpy import random

"""
Script to train and predict on data using simple rnn and simple cnn
"""

# Import data file of matrices using
data = pd.read_csv("INSERT TRANSFORMED DATA HERE")

# Try various splits of data to see which gives highest test set F1 score
train_splits = [0.75, 0.8, 0.85, 0.9]

for split in train_splits:
    # Randomly select with uniform distribution each data sample
    rand_sample = random.rand(len(data)) <= split

    train_set = data[rand_sample]
    test_set = data[~rand_sample]

    cnn_model = simple_cnn(train_set)
    rnn_model = simple_rnn(train_set)
