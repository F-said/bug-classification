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
	if sent[0] == "\\" and sent[1] == "\\":
		return False
	if sent[0] == "(" and sent[1] == "*":
		return False
	return True

def joinstatement(sents):
	curwordlist = []
	res = []
	for sent in sents:
		curwordlist.append(sent)
		if sent[-1] == ";" or sent[-1] == "{" or sent[-1] == "}":
			res.extend(curwordlist)
			curwordlist = []
	res.extend(curwordlist)
	return res

def dEBUG(sents):
	for sent in sents:
		for word in sent:
			if word == 'bytesToDouble':
				print("Hello")
				return
	print("no bytesToDouble found?")

def main():
	w2v_model = Word2Vec(min_count=1) #min_count=20,window=2,size=300,sample=6e-5,alpha=0.03,min_alpha=0.0007,negative=20
	with open("PigCode.java",'r') as f:
		lines = f.readlines()
	sents = [tokenizeCode(line) for line in lines]
	filtered_sentences = [sent for sent in sents if filterNonsense(sent)]
	joined_sentences = joinstatement(filtered_sentences)
	#NOTE: intuitive performance appears to be better when the raw sentences are used without filtering or joining
	sents = joined_sentences
	#dEBUG(sents)
	w2v_model.build_vocab(sents)
	w2v_model.train(sents,total_examples=w2v_model.corpus_count,epochs=w2v_model.epochs)
	w2v_model.save('w2v_model.bin')
	#print_exploration(w2v_model)

def print_exploration(w2v_model):	
	print(w2v_model.wv.most_similar(positive=["bytesToDouble"]))
	print(w2v_model.wv.similarity("!", "="))
	print(w2v_model.wv.doesnt_match(['private', 'protected', 'if']))
	#what is to - as * is to +?
	print(w2v_model.wv.most_similar(positive=["-", "*"], negative=["+"], topn=7))

if __name__ == '__main__':
	main()