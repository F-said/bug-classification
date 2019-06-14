from Simple_rnn_and_simple_cnn import simple_cnn, simple_rnn
import pandas as pd

"""
Script to train and predict on data using simple rnn and simple cnn
"""

# Import data file of matrices using
data = pd.read_csv("INSERT TRANSFORMED DATA HERE")

# Try various splits of data to see which gives highest test set F1 score 
