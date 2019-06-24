# bug-classification

Supervised learning problem to classify bugs by converting java code into vectors using word2vec.

Framework

1) Collect all statements in all methods and regard each statement as a single sentence.

2) Use word2vec to get all word representation vectors. 

3) Replace all words with these vectors, and in this way, we can get a format (XXX,YYY) big vector for each statement. XXX is the max token number in all statement, YYY is the representation vector length.

4) Split the data into training data and testing data. Often we can set 90% for training and 10% for testing.

5) Put all training data into the model to train.

6) Predict on the testing data and match the results with the known results to see the performance of the model.

7) Use some metrics to evaluate the whole model, such as precision, recall, F score, AUC, and so on.

Results:


<img width="301" alt="metrics" src="https://user-images.githubusercontent.com/26397102/60028546-80eb0b80-966d-11e9-8852-d99a27ca38d5.png">


<img width="291" alt="accuracy" src="https://user-images.githubusercontent.com/26397102/60028613-a2e48e00-966d-11e9-8f3e-ca2537fb096e.png">

