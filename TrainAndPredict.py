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

# Try various splits of data to see which gives highest test set F1 score
train_splits = [0.75, 0.8, 0.85, 0.9]

# Record f1 score for cnn and rnn models. Index corresponds to train splits
cnn_f1 = []
rnn_f1 = []

for split in train_splits:
    # Randomly select with uniform distribution each data sample
    rand_sample = random.rand(len(data)) <= split

    # Get training data
    train = X_data[rand_sample]
    y_train = y_data[rand_sample]

    # Get testing data
    test = X_data[~rand_sample]
    y_test = y_data[~rand_sample]

    cnn_model = simple_cnn(train)
    rnn_model = simple_rnn(test)

    # Get predictions from these two models
    cnn_predict = 0
    rnn_predict = 0

    # Calc f1_score
    cnn_f1.append(f1_score(y_test, cnn_predict, average='binary'))
    rnn_f1.append(f1_score(y_test, rnn_predict, average='binary'))

# Get split with highest f1 score for both cnn and rnn
print("Split with highest cnn f1 score:", train_splits[cnn_f1.index(max(cnn_f1))])
print("Split with highest rnn f1 score:", train_splits[rnn_f1.index(max(rnn_f1))])

