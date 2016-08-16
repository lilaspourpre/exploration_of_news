# -*- coding: utf-8 -*-
import psycopg2
import graph_builder,article_parser as ap
import database_search_engine as dse

class RequestParser(dse.SQL_query):

    def select_construtor(self, requete, req):
        """
        :param requete: список [что, откуда]
        :param req: предлог
        :return: список с результатами запроса и названиями столбцов
        """
        requete[1:2] = [requete[1].split(', ')] #если критериев несколько, то они парсятся через запятую в новый список
        if 'name' in requete[0]:
            if req == 'from':#get name from article
                query = self.persons(textname=requete[1])
            elif req == 'of':#get name of alias
                query = self.persons(alias=requete[1])
            else:
                return [[['Verify the preposition and the value']],['Error']]
        elif 'person' in requete[0]:
            if req == 'from': #get persons from article
                query = self.clusters(textname = requete[1])
            elif req == 'by': #get person by one of his names
                query = self.clusters(persons = requete[1])
            elif req == 'of': #get persons of another person
                relations, cluttexts = {}, {}
                for qu in requete[1]:
                    pre_query = self.texts(alias = [qu]) #для каждого персонажа мы находим список текстов
                    self.cur.execute(pre_query) #выполняем запрос
                    texts = [i[1] for i in self.cur.fetchall()]  #список текстов
                    self.cur.execute(self.clusters(textname=texts))
                    relations[qu] = self.cur.fetchall() #для каждого персонажа добавляем кластеры из каждого текста
                print relations
                clusters = [el for lst in relations.values() for el in lst] #объединяем кластеры в один список
                colnames = [desc[0] for desc in self.cur.description] #получение имен столбцов
                for cluster in clusters: #для каждого кластера получить список текстов, в которых он встречается (для построения графа)
                    self.cur.execute(self.texts(alias=[cluster[1]]))
                    cluttexts[cluster[1]] = self.cur.fetchall() #кластер:[текст1, текст2]
                return [clusters,colnames,cluttexts]

            else:
                return [[['Verify the preposition and the value']],['Error']]
        elif 'info' in requete[0]: #получить информацию по графу
            if requete[1][0] == 'persons': #get info about persons
                query = self.clusters()
            elif requete[1][0] == 'articles': #get info about articles
                query = self.texts()
            elif requete[1][0] == 'all names': #get info about names
                query = self.persons()
            else:
                return [[['Verify the query']],['Error']]
        elif 'article' in requete[0]: #если речь идет о статье
            if req == 'about':
                print requete[1]
                query = self.texts(alias=requete[1]) #статья о пресонаже
                print query
            else:
                return [[['Verify the query']],['Error']]
        self.cur.execute(query) #выполнить запрос
        res = self.cur.fetchall()
        colnames = [desc[0] for desc in self.cur.description]
        return [res, colnames]

    def request_parser(self, requete):
        """
        :param requete: текстовый запрос
        :return: список с значениями по запросу, названиями столбцов
        """
        if "get" in requete: #если в запросе - получить что-либо
            try:
                for i in ['from', 'of', 'about', 'by']: #проверить наличие нужных предлогов запроса
                    if i in requete:
                        requete = requete.replace('get ', '').split(" "+i+" ")
                        return self.select_construtor(requete, i) #дальнейший парсинг и формирование SQL запроса
            except:
                return [[['Problems with "get" query']],['Error']]
        elif "drop" in requete: #если происходит удаление статьи
            try:
                requete = requete.replace('drop ', '')
                text_pers_ids = self.drop_ptrelations(requete) #удаление начинается с таблицы ptrelations, т.к. политика restrict
                self.drop_article(text_pers_ids[0]) #затем удаляется статья
                clustids = self.drop_persons(text_pers_ids[1]) #удаляются персонажи и функция возвращает кластеры
                self.drop_clusters(clustids) #в последнюю очередь удаляются кластеры
                return [[['dropped']],[requete]] #вернуть сообщение о том, что удаление прошло успешно
            except:
                return [[['Verify the name of article']],['Error']] #вернуть сообщение об ошибке
        else:
            return [[['Verify syntax']],['Error']] #вернуть сообщение об ошибке
        return 0

    def drop_ptrelations(self, textname):
        query = self.texts(textname=[textname])
        self.cur.execute(query) #выполнить запрос
        textids = [i[0] for i in self.cur.fetchall()]
        p_query = self.ptrelations(textid=textids)
        self.cur.execute(p_query)
        personids = [i[0] for i in self.cur.fetchall()]
        query = self.delete('ptrelations',textid=textids)
        self.cur.execute(query)
        self.con.commit()
        return [textids,personids]

    def drop_article(self, textids=None):
        query = self.delete('texts',textids)
        self.cur.execute(query)
        self.con.commit()
        print '0'

    def drop_persons(self, personids=None):
        print personids
        query = self.persons(personid=personids)
        self.cur.execute(query)
        clusters = [i[2] for i in self.cur.fetchall()]
        query = self.delete('persons', personid=personids)
        self.cur.execute(query)
        self.con.commit()
        print 'cll', clusters
        return clusters

    def drop_clusters(self, clustids=None):
        print set(clustids)
        query = self.delete('clusters', clustid=list(set(clustids)))
        print query
        self.cur.execute(query)
        self.con.commit()
        print 'yesssss'

class UploadParser(dse.SQL_query):

    def return_IEnames(self, information):
        try:
            self.app = ap.ArticleParser(information[3])
            self.text_index = self.insert('texts', column_names=['textname','source','datepublic'], values=information[:3])
            if information[3] == 'ru' or information[3] == 'en':
                return self.app.character_recognition(information[0])
        except Exception,e:
            return e

    def return_clusters(self, names):
            return self.app.cluster_recogniser(names)

    def verify_clusters(self,clusters):
        list_of_copies = []
        for name in clusters:
            query = self.clusters(alias=[name])
            self.cur.execute(query) #выполнить запрос
            res = self.cur.fetchall()
            if res!=['','']:
                list_of_copies.append(res)
        return list_of_copies

    def return_result(self, clusters, copies):
        print copies
        for alias in clusters: #dict with [,,,,]
            if alias not in copies:
                cluster_id = self.insert('clusters', column_names=['alias'], values=[alias])
            else:
                cluster_id = copies[alias]
            for name in clusters.get(alias):
                print name
                person_id = self.insert('persons', column_names=['persname','clustid'], values=[name,cluster_id])
                self.insert('ptrelations', column_names=['personid','textid'], values=[person_id,self.text_index])
        return u"en: done successfully\nru: успешно загружено\nfr: succes de chargement"
