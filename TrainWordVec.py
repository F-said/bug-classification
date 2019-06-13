import gzip
import gensim
import os
import re
import logging

"""
Train word2vec model on PigCode.java.
Idea: If end result isn't accurate, perhaps add a lot more non-buggy code?
"""

# Logging to see progress while training
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

data_file = "PigCode.java"

# Consider each statement as a sentence. Therefore partition the code into a list of strings separated by semicolons
# and brackets
with open(data_file, "r") as myfile:
    data = myfile.read()

# Remove new lines
data = data.replace("\n", "")

# Split by ";" "}","{".
code_list = re.split(";|{|}", data)

# Split as list of lists
handle = [element.split() for element in code_list]

# Empty list for word2vec training
train_set = []

# Remove empty lists that are present due to white space
for list in handle:
    if len(list) != 0:
        train_set.append(list)

# Train word2vec on dataset
'''
Word2vec Parameters
sentences (iterable of iterables, optional) – The sentences iterable can be simply a list of lists of tokens, but 
for larger corpora, consider an iterable that streams the sentences directly from disk/network. See BrownCorpus, 
Text8Corpus or LineSentence in word2vec module for such examples. See also the tutorial on data streaming in Python. 
If you don’t supply sentences, the model is left uninitialized – use if you plan to initialize it in some other way.

corpus_file (str, optional) – Path to a corpus file in LineSentence format. You may use this argument instead of 
sentences to get performance boost. Only one of sentences or corpus_file arguments need to be passed (or none of them, 
in that case, the model is left uninitialized).

size (int, optional) – Dimensionality of the word vectors.

window (int, optional) – Maximum distance between the current and predicted word within a sentence.

min_count (int, optional) – Ignores all words with total frequency lower than this.

workers (int, optional) – Use these many worker threads to train the model (=faster training with multicore machines).

sg ({0, 1}, optional) – Training algorithm: 1 for skip-gram; otherwise CBOW.

hs ({0, 1}, optional) – If 1, hierarchical softmax will be used for model training. If 0, and negative is non-zero, 
negative sampling will be used.

negative (int, optional) – If > 0, negative sampling will be used, the int for negative specifies how many 
“noise words” should be drawn (usually between 5-20). If set to 0, no negative sampling is used.

ns_exponent (float, optional) – The exponent used to shape the negative sampling distribution. A value of 1.0 samples 
exactly in proportion to the frequencies, 0.0 samples all words equally, while a negative value samples low-frequency 
words more than high-frequency words. The popular default value of 0.75 was chosen by the original Word2Vec paper. 
More recently, in https://arxiv.org/abs/1804.04212, Caselles-Dupré, Lesaint, & Royo-Letelier suggest that other values 
may perform better for recommendation applications.

cbow_mean ({0, 1}, optional) – If 0, use the sum of the context word vectors. If 1, use the mean, only applies when 
cbow is used.

alpha (float, optional) – The initial learning rate.

min_alpha (float, optional) – Learning rate will linearly drop to min_alpha as training progresses.

seed (int, optional) – Seed for the random number generator. Initial vectors for each word are seeded with a hash of 
the concatenation of word + str(seed). Note that for a fully deterministically-reproducible run, you must also limit 
the model to a single worker thread (workers=1), to eliminate ordering jitter from OS thread scheduling. (In Python 3, 
reproducibility between interpreter launches also requires use of the PYTHONHASHSEED environment variable to control 
hash randomization).

max_vocab_size (int, optional) – Limits the RAM during vocabulary building; if there are more unique words than this, 
then prune the infrequent ones. Every 10 million word types need about 1GB of RAM. Set to None for no limit.

max_final_vocab (int, optional) – Limits the vocab to a target vocab size by automatically picking a matching min_count. 
If the specified min_count is more than the calculated min_count, the specified min_count will be used. Set to None if 
not required.

sample (float, optional) – The threshold for configuring which higher-frequency words are randomly downsampled, 
useful range is (0, 1e-5).

hashfxn (function, optional) – Hash function to use to randomly initialize weights, for increased training 
reproducibility.

iter (int, optional) – Number of iterations (epochs) over the corpus.

trim_rule (function, optional) – Vocabulary trimming rule, specifies whether certain words should remain in the 
vocabulary, be trimmed away, or handled using the default

sorted_vocab ({0, 1}, optional) – If 1, sort the vocabulary by descending frequency before assigning word indexes. 
See sort_vocab().

batch_words (int, optional) – Target size (in words) for batches of examples passed to worker threads 
(and thus cython routines).(Larger batches will be passed if individual texts are longer than 10000 words, but the 
standard cython code truncates to that maximum.)

compute_loss (bool, optional) – If True, computes and stores loss value which can be retrieved using
 get_latest_training_loss().
 
callbacks (iterable of CallbackAny2Vec, optional) – Sequence of callbacks to be executed at specific stages during 
training.
'''
# Set workers to 1 and seed to 42 for reproducibility. Initial parameters set according to specs.
model = gensim.models.Word2Vec(train_set, size=200, window=8, sample=0.0001, workers=1, seed=42)
model.train(train_set, total_examples=len(train_set), epochs=10)

# View some outputs of the model
# What is "if" most associated with in these collection of programs?
w1 = "if"
print(model.wv.most_similar(positive=w1))

# Save the model. To open this model in another file:
'''
from gensim.models.keyedvectors import KeyedVectors
model = KeyedVectors.load_word2vec_format('model.bin', binary=True)
'''
model.wv.save_word2vec_format('model.bin', binary=True)
