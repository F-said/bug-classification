from gensim.models import Word2Vec
import re

def tokenizeCode(sent):
	splitwhite = sent.split()
	splitmore = [re.split(r'(\W)',word) for word in splitwhite]
	return [item for sublist in splitmore for item in sublist if len(item) > 0]

def filterNonsense(sent):
	if len(sent) < 3:
		return False
	if sent[0] == "*":
		return False
	return True

def main():
	w2v_model = Word2Vec(min_count=20,window=2,size=400,sample=6e-5,alpha=0.03,min_alpha=0.0007,negative=20)
	with open("PigCode.java",'r') as f:
		lines = f.readlines()
	sents = [tokenizeCode(line) for line in lines]
	filtered_sentences = [sent for sent in sents if filterNonsense(sent)]
	#print(filtered_sentences[420])
	w2v_model.build_vocab(sents,progress_per=1000)
	w2v_model.train(sents, total_examples=w2v_model.corpus_count, epochs=30,report_delay=1)
	w2v_model.init_sims(replace=True)
	print_exploration(w2v_model)

def print_exploration(w2v_model):	
	print(w2v_model.wv.most_similar(positive=["int"]))
	print(w2v_model.wv.similarity("!", "="))
	print(w2v_model.wv.doesnt_match(['private', 'protected', 'if']))
	#what is to - as * is to +?
	print(w2v_model.wv.most_similar(positive=["-", "*"], negative=["+"], topn=3))

if __name__ == '__main__':
	main()