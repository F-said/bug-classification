#from applyw2v import tokenizeCode, filterNonsense, joinstatement
import gensim.models
import csv
import ast
import numpy as np
import tensorflow as tf
#import Simple_rnn_and_simple_cnn as sm
"""
Script to transform all statements into matrix of word vectors, with padding
"""

def pad(beglist, padl, pade):
	return (beglist + padl * [pade])[:padl]

def main(debug=False,printtocsv=False):
	w2v_model = gensim.models.Word2Vec.load('w2v_model.bin')
	with open('bug-classification.csv') as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')
		lines = []
		for row in csv_reader:
			lines.append(row)
	buggy_lines = [ast.literal_eval(row[1]) for row in lines if row[2] == '1']
	fine_lines = [ast.literal_eval(row[1]) for row in lines if row[2] == '0']

	maxtokens = max([len(stm) for stm in buggy_lines])
	fine_lines = [line for line in fine_lines if len(line) <= maxtokens]

	worddim = len(w2v_model[buggy_lines[0][0]])
	buggy_vecs = [pad([w2v_model[word] for word in line],maxtokens,np.array(worddim*[0.0])) for line in buggy_lines]
	fine_vecs = [pad([w2v_model[word] for word in line],maxtokens,np.array(worddim*[0.0])) for line in fine_lines]

	if debug:
		print(str(len(buggy_lines)) + " buggy lines")
		print(str(len(fine_lines)) + " lines without bugs")
		print(str(maxtokens) + " tokens per line")
		print(str(worddim) + " dimension of token")
		#print(buggy_vecs[42])

	together = []
	for buggy_row in buggy_vecs:
		together.append([buggy_row,1])
	for clean_row in fine_vecs:
		together.append([clean_row,0])

	if printtocsv:
		with open("vector_data.csv","w+") as my_csv:
			fieldnames = ('Statement', 'Buggy_Bit')
			csvWriter = csv.writer(my_csv,delimiter=',')
			csvWriter.writerows(together)

	return together
		
if __name__ == '__main__':
	main(debug=True)
