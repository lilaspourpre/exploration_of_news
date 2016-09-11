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
            self.surely_not_in = codecs.open('Dictionaries/not_in_ru.txt', 'r', 'utf-8').read().split('\r\n')
        elif self.lang == 'en': #add surnames to dict_names
            self.dict_names = open('Dictionaries/name_dict_en.txt', 'r')
            self.surely_not_in = codecs.open('Dictionaries/not_in_en.txt', 'r', 'utf-8').read().split('\r\n')
        self.dict_names = self.dict_names.read().split("\n")
        self.sentences_list = []
        self.wordlist = []
        self.list_ofDicts_with_probab = []
        self.probable_names = []
        self.list_prev_names = []
        self.NormalWordScore = 0.2
        self.FirstWordScore = 0.3
        self.PersonalNameScore = 0.9

    def preprocessing(self, text):
        for phrase in text:
            self.sentences_list.append(word_tokenize(phrase))    #создаем списки предложений
        self.sentences_list = [i for i in self.sentences_list if len(i) >= 2] #удаляем те, где <2 слов
        self.wordlist = list(set([el for lst in self.sentences_list for el in lst])) #список слов в тексте
        self.zipList = [zip(phrase, range(len(phrase))) for phrase in self.sentences_list] #создаем зипы (слово,№)

    def character_recognition(self, text):
        if '.txt' not in text:
            text+='.txt'
        text = codecs.open('Articles/'+text, 'r', 'utf-8').read()
        phrase1, phrase2, phrase3 = re.compile("[,\n';\-]"), re.compile(u'[!"?:]'), re.compile(u'\ufeff')
        text = phrase1.sub(" ", text)
        text = phrase2.sub('.', text)
        text = phrase3.sub('', text)
        text = text.split(".") #распарсили текст
        self.preprocessing(text)
        self.critere_parser()
        return self.probable_names

    def critere_parser(self):
        def estimator(cur_word, num_word):
            prob_cur_word = 0
            prob_cur_word = uppercase(prob_cur_word, cur_word, num_word) #проверяем заглявную и другие вложенные условия
            return prob_cur_word

        def uppercase(prob, word, num):
            if word != word.upper() and word == word.capitalize(): #если с заглавной
                prob+= self.NormalWordScore
                prob = wordLen(prob, word, num)
            return prob

        def wordLen(prob, word, num):
            if len(word) > 1: #если длина больше 1
                prob+= self.NormalWordScore
                prob = wordPosition(prob, word, num)
            return prob

        def wordPosition(prob, word, num):
            flag_name = False
            if num==0: #если в начале предложения -0.2
                prob -= self.FirstWordScore
            for phrase in self.sentences_list:
                if word in phrase and phrase.index(word)!=0:         #если с большой б в тексте не в начале - то прибавить
                    prob+= self.FirstWordScore
                    flag_name = True #также переходим к следующему пункту анализа
            else:
                prob+= self.FirstWordScore
                prob = dictAnalyser(prob, word)
            if flag_name == True:
                prob = dictAnalyser(prob, word)
            return prob

        def dictAnalyser(prob, word):
            if word.lower() in self.wordlist: #если встречается с мал.буквы
                prob -= self.FirstWordScore
            if word in self.dict_names:
                prob = self.PersonalNameScore   # если есть в словаре
            if word in self.surely_not_in:
                prob = 0
            return prob

        for sentence in self.zipList:
            for word in sentence: #для каждого зипа
                if estimator(word[0], word[1]) >= 0.7: #если вероятность являться именной сущностью больше 0.7
                    self.probable_names.append(word[0]) #добавляем имя в список
                    self.list_prev_names.append((self.zipList.index(sentence),sentence.index(word))) #добавляем в список кортеж (№предложения,№пп)
                    if word[1] != 0: #если слово не первое в предложении
                        if (self.zipList.index(sentence),sentence.index(word)-1) in self.list_prev_names: #проверяем, нет ли кортежа пред.слова в списке
                            self.probable_names.append(sentence[sentence.index(word)-1][0]+' '+word[0]) #если да, то объединяем оба слова

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
