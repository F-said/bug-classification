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

Original Results:


<img width="496" alt="reg_metrics" src="https://user-images.githubusercontent.com/26397102/60216166-52209100-9837-11e9-9f65-d420184fca4a.png">


Oversampled Resuts (Ratio 1:1):


<img width="498" alt="oversample_mets" src="https://user-images.githubusercontent.com/26397102/60216154-4af98300-9837-11e9-8f9e-38e690555e4c.png">
