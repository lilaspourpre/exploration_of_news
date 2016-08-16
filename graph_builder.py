# -*- coding: utf-8 -*-'
import networkx as nx
from matplotlib import rc
rc('font',**{'family':'verdana'})
try:
    import matplotlib.pyplot as plt
except:
    raise

class Graphs():
    """
    класс по построению графов
    def build graph: строит граф
    """
    def __init__(self, query):
        self.G=nx.Graph()
        self.query = query #для названия изображения графа

    def build_graph(self, clusttexts=None):
        """
        :param clusttexts: список текстов и героев в них
        """
        dic = buildEdges(clusttexts) #формирование кластеров в зависимости от текста
        for i in dic: #в тексте i
            for j in i[1]: #каждый персонаж
                for k in i[1]: #с каждым персонажем
                    self.G.add_edge(unicode(j), unicode(k)) #встречались, значит, они формируют ребро
        self.bc=nx.betweenness_centrality(self.G) #betweenness_centrality
        self.bc = [(self.bc[i]+0.005) * 50000 for i in self.G.nodes()] #betweenness_centrality - список для каждой вершины
        self.cc=nx.closeness_centrality(self.G) #closeness_centrality
        self.cc=[self.cc[i] for i in self.G.nodes()] #closeness_centrality - список для каждой вершины
        nx.draw(self.G,node_size=self.bc, node_color=self.cc, alpha=0.5, font_family='verdana') #построение графа
        plt.savefig('Graphs/'+self.query+'.png') #сохранение графа в папку Graphs

def buildEdges(clusttexts):
    """
    :param clusttexts: словарь, героев и список их текстов
    :return: список текстов и героев в каждом из них
    """
    texts = list(set([el for lst in clusttexts.values() for el in lst])) #создаем список текстов
    allTexts = [] #пустой список для хранения в формате [text, [pers1,pers2,pers3]]
    for text in texts:
        l = [] #список персонажей
        for cluster in clusttexts: #для каждого персонажа
            if text in clusttexts.get(cluster): #если он упоминается в тексте
                l.append(cluster.decode('utf-8')) #персонаж вносится в список
                print text
        allTexts.append([text,l]) #сохранение в формате [text, [pers1,pers2,pers3]]
    return allTexts


