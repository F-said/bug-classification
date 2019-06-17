from Simple_rnn_and_simple_cnn import simple_cnn, simple_rnn
import pandas as pd
from numpy import random
from sklearn.metrics import f1_score

"""
Script to train and predict on data using simple rnn and simple cnn
"""

# Import data file of matrices using
data = pd.read_csv("INSERT TRANSFORMED DATA HERE")

# Get target values
X_data = data["..."]
y_data = data["Bug"]

# Split to consider
split = 0.9 

# Randomly select with uniform distribution each data sample
rand_sample = random.rand(len(data)) <= split

# Get training data
train = X_data[rand_sample]
y_train = y_data[rand_sample]

# Get testing data
test = X_data[~rand_sample]
y_test = y_data[~rand_sample]

# Create models from simple_rnn_and_simple_cnn file
cnn_model = simple_cnn(train)
rnn_model = simple_rnn(test)

# Get predictions from these two models
cnn_predict = 0
rnn_predict = 0

# Calc f1_score
cnn_f1 = f1_score(y_test, cnn_predict, average='binary')
rnn_f1 = f1_score(y_test, rnn_predict, average='binary')

# Print f1 scores
print("CNN f1 score:", cnn_f1)
print("RNN f1 score:", rnn_f1)
