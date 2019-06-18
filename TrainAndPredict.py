from Simple_rnn_and_simple_cnn import simple_cnn, simple_rnn
import pandas as pd
import numpy as np
from sklearn.metrics import f1_score
from keras.utils.vis_utils import plot_model
import ast
import TransformData

"""
Script to train and predict on data using simple rnn and simple cnn
"""

batch_size = 32
epochs = 12

data_size = 4615

input_len = 30
input_dim = 100

# Import data file of matrices using

orig=False
if orig:
	data = pd.read_csv("vector_data.csv")
	data.columns = ["Statement", "Bug"]

	# Get target values
	x_data = data["Statement"].as_matrix()
	x_data.reshape([-1,input_len,input_dim])

	#x_data = [ast.literal_eval(row) for row in x_data]

	y_data = data["Bug"].as_matrix()
	y_data.reshape([-1,input_len,input_dim])

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
#rand_sample = [i for b,i in zip(rand_sample,range(len(rand_sample))) if b]

# Get training data
x_train = x_data[rand_sample]
y_train = y_data[rand_sample]

# Get testing data
x_test = x_data[~rand_sample]
y_test = y_data[~rand_sample]

# Create models from simple_rnn_and_simple_cnn file
cnn_model = simple_cnn(input_len,input_dim,10,1,1,1,1)
rnn_model = simple_rnn(input_len,input_dim,10)


# Fit the data to the models
cnn_model.fit(x_train, y_train, batch_size=batch_size, epochs=epochs,
			verbose=1, validation_data=(x_test, y_test))
#rnn_model.fit(x_train, y_train, batch_size=batch_size, epochs=epochs,
#			verbose=1, validation_data=(x_test, y_test))


# Evaluate the models
cnn_score = cnn_model.evaluate(x_test, y_test, verbose=0)
#rnn_score = rnn_model.evaluate(x_test, y_test, verbose=0)

# Print evaluation metrics
print('CNN Test score:', cnn_score[0])
print('CNN Test accuracy:', cnn_score[1])

#print('RNN Test score:', rnn_score[0])
#print('RNN Test accuracy:', rnn_score[1])


# Get predictions from these two models
cnn_predict = 0
#rnn_predict = 0

# Calc f1_score
cnn_f1 = f1_score(y_test, cnn_predict, average='binary')
#rnn_f1 = f1_score(y_test, rnn_predict, average='binary')

# Print f1 scores
print("CNN f1 score:", cnn_f1)
#print("RNN f1 score:", rnn_f1)


print("Hello World!")
