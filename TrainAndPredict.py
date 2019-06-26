from Simple_rnn_and_simple_cnn import simple_cnn, simple_rnn
import pandas as pd
import numpy as np
from keras.utils.vis_utils import plot_model
import ast
import TransformData
from keras.models import load_model
from keras.utils import plot_model
from imblearn.over_sampling import RandomOverSampler

"""
Script to train and predict on data using simple rnn and simple cnn
"""

# Function to catch any division by 0
def div_catch(n, d):
    return n / d if d != 0 else "N/A"

batch_size = 32
epochs = 12

data_size = 4615

input_len = 30
input_dim = 100

# Load original data
orig_data = pd.read_csv("bug-classification.csv")

# Import data file of matrices using

orig = False
if orig:
    data = pd.read_csv("vector_data.csv")
    data.columns = ["Statement", "Bug"]

    # Get target values
    x_data = data["Statement"].as_matrix()
    x_data.reshape([-1, input_len, input_dim])

    # x_data = [ast.literal_eval(row) for row in x_data]

    y_data = data["Bug"].as_matrix()
    y_data.reshape([-1, input_len, input_dim])

    print(type(x_data))
else:
    data = TransformData.main()
    x_data = np.array([row[0] for row in data])
    y_data = np.array([row[1] for row in data])
    print(type(x_data))

# Split to consider
split = 0.9

 # Randomly select with uniform distribution each data sample
rand_sample = np.random.rand(len(data)) <= split
# rand_sample = [i for b,i in zip(rand_sample,range(len(rand_sample))) if b]

# Get training data
x_train = x_data[rand_sample]
y_train = y_data[rand_sample]

# Get testing data
x_test = x_data[~rand_sample]
y_test = y_data[~rand_sample]

oversample = True
# Over-sample to generate more positive data samples to train on
# IF OVERSAMPLE IS TRUE, CHANGE
# cnn_model = load_model("cnn_model.h5") TO cnn_model = load_model("cnn_model_res.h5")
# AND SAME FOR RNN
if oversample:
    # Set random state for consistency. Create equal number of buggy lines to non-buggy lines
    ros = RandomOverSampler(ratio='minority', random_state=42)

    # Reshape for the random sampling
    nsamples, nx, ny = x_train.shape
    x_train_reshape = x_train.reshape((nsamples, nx * ny))

    nsamples, nx, ny = x_test.shape
    x_test_reshape = x_test.reshape((nsamples, nx * ny))

    # Over sample
    x_train, y_train = ros.fit_sample(x_train_reshape, y_train)
    x_test, y_test = ros.fit_sample(x_test_reshape, y_test)

    # Reshape to original shape
    x_train = x_train.reshape([-1, input_len, input_dim])
    x_test = x_test.reshape([-1, input_len, input_dim])

    print("You've been oversampled")

saved = True
if saved:
    cnn_model = load_model("cnn_model_res.h5")
    rnn_model = load_model("rnn_model_res.h5")
else:
    # Create models from simple_rnn_and_simple_cnn file
    cnn_model = simple_cnn(input_len, input_dim, 1, 1, 1, 1, 1)
    rnn_model = simple_rnn(input_len, input_dim, 1)

    # Fit the data to the models
    cnn_model.fit(x_train, y_train, batch_size=batch_size, epochs=epochs,
                  verbose=1, validation_data=(x_test, y_test))
    rnn_model.fit(x_train, y_train, batch_size=batch_size, epochs=epochs,
                  verbose=1, validation_data=(x_test, y_test))

    # Save the models
    cnn_model.save("cnn_model_res.h5")
    rnn_model.save("rnn_model_res.h5")

    # Illustrate models
    # plot_model(cnn_model, to_file='cnn_model_vis.png')
    # plot_model(rnn_model, to_file='rnn_model_vis.png')

# Evaluate the models
cnn_score = cnn_model.evaluate(x_test, y_test, verbose=0)
rnn_score = rnn_model.evaluate(x_test, y_test, verbose=0)

# Print evaluation metrics
print('CNN Metrics:', cnn_model.metrics_names, cnn_score)
print('RNN Metrics:', rnn_model.metrics_names, rnn_score)

# Create predictions to get accuracy
# Get predictions from these two models and convert binary targets to int
tot = len(y_test)
cnn_predict = cnn_model.predict(x_test).astype(int)
rnn_predict = rnn_model.predict(x_test).astype(int)

# Convert all 0.99's to 1 in y_test for accuracy calculation
y_test[y_test > 0] = 1

# Get transpose of predictions to turn column vector into row vector
cnn_predict = cnn_predict.transpose()
rnn_predict = rnn_predict.transpose()

# Create dataframe to join predictions and create metrics
cnn_join = pd.DataFrame()
cnn_join["truth"] = y_test.tolist()
cnn_join["pred"] = cnn_predict.transpose().tolist()
cnn_join["pred"] = cnn_join["pred"][1][0]

rnn_join = pd.DataFrame()
rnn_join["truth"] = y_test.tolist()
rnn_join["pred"] = rnn_predict.transpose().tolist()
rnn_join["pred"] = rnn_join["pred"][1][0]

# Get true positives
true_pos_cnn = len(cnn_join[(cnn_join["truth"] == 1) & (cnn_join["pred"] == 1)])
true_pos_rnn = len(rnn_join[(rnn_join["truth"] == 1) & (rnn_join["pred"] == 1)])

# Get true negatives
true_neg_cnn = len(cnn_join[(cnn_join["truth"] == 0) & (cnn_join["pred"] == 0)])
true_neg_rnn = len(rnn_join[(rnn_join["truth"] == 0) & (rnn_join["pred"] == 0)])

# Compute accuraccy
print("CNN Test Accuracy: ", div_catch(true_pos_cnn + true_neg_cnn, tot))
print("RNN Test Accuracy: ", div_catch(true_pos_rnn + true_neg_rnn, tot), "\n")

# Calc false negatives and false positives
false_neg_cnn = len(cnn_join[(cnn_join["truth"] == 1) & (cnn_join["pred"] == 0)])
false_neg_rnn = len(rnn_join[(rnn_join["truth"] == 1) & (rnn_join["pred"] == 0)])

false_pos_cnn = len(cnn_join[(cnn_join["truth"] == 0) & (cnn_join["pred"] == 1)])
false_pos_rnn = len(rnn_join[(rnn_join["truth"] == 0) & (rnn_join["pred"] == 1)])

# Calculate recall
cnn_recall = div_catch(true_pos_cnn, (true_pos_cnn + false_neg_cnn))
rnn_recall = div_catch(true_pos_rnn, (true_pos_rnn + false_neg_rnn))
print("CNN Recall: ", cnn_recall)
print("RNN Recall: ", rnn_recall, "\n")

# Calculate precision
cnn_precision = div_catch(true_pos_cnn, (true_pos_cnn + false_pos_cnn))
rnn_precision = div_catch(true_pos_rnn, (true_pos_rnn + false_pos_rnn))
print("CNN Precision: ", cnn_precision)
print("RNN Precision: ", rnn_precision, "\n")

# Calculate f1 score
if cnn_precision != "N/A" and cnn_recall != "N/A":
    print("CNN f1 score: ", 2*((cnn_precision * cnn_recall)/(cnn_precision + cnn_recall)))
else:
    print("CNN f1 score: N/A")

if rnn_precision != "N/A" and rnn_recall != "N/A":
    print("RNN f1 score: ", 2*((rnn_precision * rnn_recall)/(rnn_precision + rnn_recall)), "\n")
else:
    print("RNN f1 score: N/A")

# Save predictions of both models to reveal which lines of code were considered buggy
# Index CNN predictions by positive values
cnn_result = np.where(cnn_predict == 1)
cnn_positives = list(cnn_result)

# Return and save list of positive examples according to cnn model
cnn_positives = cnn_positives[0].tolist()
predicted_buggy_cnn = orig_data.iloc[cnn_positives]

predicted_buggy_cnn.to_csv(path_or_buf="CNN_predicted_buggy.csv", index=False)

# Index RNN predictions by positive values
rnn_result = np.where(rnn_predict == 1)
rnn_positives = list(rnn_result)

# Return and save list of positive examples according to rnn model
rnn_positives = rnn_positives[0].tolist()
predicted_buggy_rnn = orig_data.iloc[rnn_positives]

predicted_buggy_rnn.to_csv(path_or_buf="RNN_predicted_buggy.csv", index=False)

print("Hello World!")
