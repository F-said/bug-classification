from keras.layers import Dense, Activation, Convolution1D, MaxPooling1D, Flatten
from keras.models import Sequential
from keras.layers.recurrent import GRU


# Simple CNN model: The example code here dealing with 1D convolutional problem which can be used to do the statement
# classification to detect bugs
# input_len: In bug detection problem, it is often the max statement length
# input_dim: In bug detection problem, it is often the token representation vector length (e.g. i = 1 ,
# in this statement, "i", "=", "1" all are tokens)
# output_dim: Expected model output length.  If you want to use (1,1,...,1) to represent buggy statement and
# (0,0,...,0) to represent non-buggy statement, you can set it to the length you want for (1,1,...,1) and (0,0,...,0)
# filter_num: The number of convolutional cores.You can set it to 1 to do a test
# kernel_size_num: The edge length for the convolutional core. You use one or two numbers to set the size.
# stride_num: The step length when doing the calculation.
# pool_size_num: Size of the max pooling windows.
#
# Input data format: (X, input_len, input_dim)
# Here X is the statement number, input_len is the max number of tokens in one statement,
# input_dim is the token representation vector length. For more details about layers and model, please refer:
# https://keras.io/

def simple_cnn(input_len, input_dim, output_dim, filter_num, kernel_size_num, strides_num, pool_size_num):
    model = Sequential()
    model.add(Convolution1D(input_shape=(input_len, input_dim), filters=filter_num, kernel_size=kernel_size_num,
                            strides=strides_num, padding='same'))
    model.add(Activation('relu'))
    model.add(MaxPooling1D(pool_size=pool_size_num))
    model.add(Flatten())
    model.add(Dense(input_dim))
    model.add(Activation('relu'))
    model.add(Dense(output_dim))
    model.add(Activation('softmax'))
    model.compile(loss='sparse_categorical_crossentropy', optimizer='adadelta', metrics=['accuracy'])
    return model


# Simple RNN model: The code here can regard each statement as sentences to process and find the "unnatural" statement
# input_len: In bug detection problem, it is often the max statement length
# input_dim: In bug detection problem, it is often the token representation vector length
# (e.g. i = 1 , in this statement, "i", "=", "1" all are tokens)
# output_dim: Expected model output length. If you want to use (1,1,...,1) to represent buggy statement and
# (0,0,...,0) to represent non-buggy statement, you can set it to the length you want for (1,1,...,1) and (0,0,...,0)
#
# Input data format: (X, input_len, input_dim)
# Here X is the statement number, input_len is the max number of tokens in one statement,
# input_dim is the token representation vector length
# For more details about layers and model, please refer: https://keras.io/

def simple_rnn(input_len, input_dim, output_dim):
    model = Sequential()
    model.add(GRU(input_shape=(input_len, input_dim), output_dim=output_dim))
    model.add(Dense(output_dim, activation="relu"))
    model.compile(loss="mse", optimizer='adam')
    return model
