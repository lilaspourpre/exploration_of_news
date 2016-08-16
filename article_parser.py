# -*- coding: utf-8 -*-
import codecs, re
from nltk.tokenize import word_tokenize
import sklearn.feature_extraction as skfe
import sklearn.cluster as sc

class ArticleParser():
    def __init__(self, lang):
        self.lang = lang
        if self.lang == 'ru':
            self.dict_names = codecs.open('Dictionaries/name_dict_ru.txt', 'r', 'utf-8')
            self.surely_not_in = [u"Бог", u"Москва", u"Глава"]
        elif self.lang == 'en': #add surnames to dict_names
            self.dict_names = open('Dictionaries/name_dict_en.txt', 'r')
            self.surely_not_in = [u'French', u'France', u'America', u'United', u'Atlantic', 'National', 'Front', u'States', u'Normandy', u'Republicans', u'Republican', u'Agence', u'Iraq', u'Paris', u'Association', u'Americans', u'Mr', u'Le', u'Presse', u'Press', u'Mrs',u'American',u"God",u"Monday",u"Don",u"Tuesday",u"Wednesday",u"Thursday",u"Friday",u"Saturday",u"Sunday",u"January",u"February",u"March",u"April",u"May",u"June",u"July",u"August",u"September",u"October",u"November",u"December",u"East",u"North",u"South",u"Western",u"Easten",u"Northen",u"Southen",u"Weste"]
        self.dict_names = self.dict_names.read().split("\n")
        self.sentences_list = []
        self.wordlist = []
        self.dict_list=[]
        self.probable_names=[]

    def useful_operations(self, text):
        for phrase in text:
            self.sentences_list.append(word_tokenize(phrase))    #создаем списки предложений
        self.sentences_list = [i for i in self.sentences_list if len(i) >= 2]
        self.wordlist = list(set([el for lst in self.sentences_list for el in lst]))
        self.dict_list = [{} for i in range(len(self.sentences_list))]
        self.emissProb = [{} for i in range(len(self.sentences_list))]
        zipList = [zip(phrase, range(len(phrase))) for phrase in self.sentences_list]
        for phrase in zipList:
            for word in phrase:
                self.dict_list[zipList.index(phrase)][word] = 0 #для каждого зипа (сл,№)есть ключ : 0, предложение в {}
        print self.dict_list
        return 0

    def character_recognition(self, text):
        if '.txt' not in text:
            text+='.txt'
        text = codecs.open('Articles/'+text, 'r', 'utf-8').read()
        phrase1, phrase2, phrase3 = re.compile("\,|\n|\'|;|\-|\—"), re.compile('\!|\"|\?|\:'), re.compile(u'\ufeff')
        text = phrase1.sub(" ", text)
        text = phrase2.sub('.', text)
        text = phrase3.sub('', text)
        text = text.split(".") #распарсили текст
        self.useful_operations(text)
        self.critere_parser()
        return self.probable_names

    def critere_parser(self):
        for index in range(len(self.sentences_list)): #имеет смысл иметь зип заглавных всех, заглавных первых (так будет быстрее)
            for word in self.dict_list[index]:
                if word[0] != word[0].upper():
                    if word[0] == word[0].capitalize():                         #если с заглавной +0.2
                        self.dict_list[index][word]+= 0.2
                        if len(word[0]) > 1:                                          #если длина >1  +0.2
                            self.dict_list[index][word]+= 0.2
                            if word[1]==0: #если в начале предложения -0.2
                                self.dict_list[index][word] = self.dict_list[index][word] - 0.2
                                for phraseW in self.sentences_list:
                                    if word[0] in phraseW and phraseW.index(word[0])!=0:         #если с большой б в тексте не в начале - то прибавить
                                        self.dict_list[index][word]+= 0.3
                            else:                                                           #если не в начале предложения +0.3
                                self.dict_list[index][word]+= 0.3
                                if word[0].lower() in self.wordlist:                      #если есть в тексте с маленькой буквы -0.3
                                    self.dict_list[index][word] = self.dict_list[index][word] - 0.3
                            if word[0] in self.dict_names:
                                self.dict_list[index][word] = 0.9                               # если есть в словаре
                            if word[0] in self.surely_not_in:
                                self.dict_list[index][word] = 0
        for sentence in self.dict_list:
            for word in sentence:
                if sentence.get(word) >= 0.7:
                    self.probable_names.append(word[0])
                    if word[1] != 0:
                        if sentence.get((self.sentences_list[self.dict_list.index(sentence)][word[1]-1],word[1]-1)) > 0.5:
                            self.probable_names.append(self.sentences_list[self.dict_list.index(sentence)][word[1]-1]+" "+word[0])
        return 0

    def cluster_recogniser(self, corpus):
        corpus_res = {}
        ngram_vectorizer = skfe.text.CountVectorizer(analyzer='char', ngram_range=(2, 4))
        counts = ngram_vectorizer.fit_transform(corpus)
        machine = sc.AffinityPropagation()
        list_num=list(machine.fit_predict(counts))
        groups=[[] for i in range(max(list_num)+1)]
        for i in range(len(corpus)):
            groups[list_num[i]].append(corpus[i])
        for i in groups:
            corpus_res[i[0]] = i
        return corpus_res