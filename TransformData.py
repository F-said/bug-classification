#from applyw2v import tokenizeCode, filterNonsense, joinstatement
import gensim.models
import csv
import ast
import numpy as np
"""
Script to transform all statements into matrix of word vectors, with padding
"""

def main():
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

	buggy_vecs = [[w2v_model[word] for word in line] for line in buggy_lines]
	fine_vecs = [[w2v_model[word] for word in line] for line in fine_lines]

	print(str(len(buggy_lines)) + " buggy lines")
	print(str(len(fine_lines)) + " lines without bugs")
	print(buggy_lines[3])
	print(buggy_vecs[3])

if __name__ == '__main__':
	main()

### COOKING BEANS AND THEN DOING THIS BRB ###
