# -*- coding: utf-8 -*-
import psycopg2
import xml.etree.ElementTree as ET
from psycopg2.extensions import adapt

class SQL_query():
    """
    класс, который осуществляет поиск в базе данных
    поддерживает возможность поиска информации и добаления новых данных (insert)
    """
    def __init__(self):
        self.con = self.connect()
        self.cur = self.con.cursor()

    def open_config(self, config): #находит в файле xml код и строит дерево, возвращает строку для коннекта
        tree = ET.parse(config)
        root = tree.getroot()
        string = ""
        for child in root: #соединяем данные в строку user=postgres и тд
            string+=child.tag+"='"+child.text+"' "
        return string

    def connect(self): #подключение к БД
        try:
            return psycopg2.connect(self.open_config("config.xml"))
        except psycopg2.DatabaseError, e:
            if self.con:
                self.con.rollback()

    def close(self): #отключение от БД
        if self.con:
            self.con.close()

    def select_condition(self, columnname, data):
        """
        вспомогательная функция для Texts, Persons, Clusters (поиска по БД)
        :param columnname: название столбца указывается автоматически, пользователю его набирать не нужно
        :param data: указанное значение
        :return: соединяет условие для запроса в вид persid = '1' OR peris = '2'
        """
        condition = []
        for i in data:
            condition.append(columnname + '=' + psycopg2.extensions.adapt(i).getquoted())
        return ' OR '.join(condition)

    def Texts(self, all=None, persons=None, clustid=None, alias=None):
        """
        :param all: при all!=None выдает содержание всей таблицы texts
        :param persons: заданные параметры persons означают, что тексты будут выдаваться относительно персонажей в них
        :param clustid: заданные параметры clustid означают, что тексты будут выдаваться относительно персонажей в них (более общее чем persons)
        :param alias: заданные параметры alias означают, что тексты будут выдаваться относительно персонажей в них
        (то же, что и clustid, только задается общее имя для кластера, а не его номер)
        :return: возвращает номер и название текста (textid, textname)
        """
        if self.con:
            if all!=None:
                query = 'SELECT * FROM texts;'
            if alias!=None:
                query = 'SELECT textid, textname FROM texts NATURAL JOIN ptrelations NATURAL JOIN persons NATURAL JOIN clusters WHERE ( '+self.select_condition('alias',alias)+' );'
            elif persons!=None:
                query = 'SELECT textid, textname FROM texts NATURAL JOIN ptrelations NATURAL JOIN persons WHERE ( '+self.select_condition('persname',persons)+' );'
            elif clustid!=None:
                query = 'SELECT textid, textname FROM texts NATURAL JOIN ptrelations NATURAL JOIN persons WHERE ( '+self.select_condition('clustid',clustid)+' );'
            print query
            self.cur.execute(query)
            return self.cur.fetchall()
        else:
            self.connect()
            self.Texts(all, persons, clustid, alias)

    def Persons(self, all=None, clustid=None, textid=None, alias=None):
        """
        :param all: при all!=None выдает содержание всей таблицы persons
        :param clustid: заданные параметры clustid означают, что персонажи будут выдаваться в соответствии с номером кластера
        :param textid: заданные параметры textid означают, что персонажи будут выдаваться в соответствии с номером текста
        :param alias: заданные параметры alias означают, что персонажи будут выдаваться в соответствии с кластером
        (то же, что и clustid, только задается общее имя для кластера, а не его номер)
        :return: возвращает номер и имя персонажа (personid, persname)
        """
        if self.con:
            if all!=None:
                query = 'SELECT * FROM persons;'
            if alias!=None:
                query = 'SELECT personid, persname FROM persons NATURAL JOIN clusters WHERE ('+self.select_condition('alias',alias)+');'
            elif textid!=None:
                query = 'SELECT personid, persname FROM texts NATURAL JOIN ptrelations NATURAL JOIN persons WHERE ('+self.select_condition('textid',textid)+');'
            elif clustid!=None:
                query = 'SELECT personid, persname FROM persons WHERE ('+self.select_condition('clustid',clustid)+');'
            print query
            self.cur.execute(query)
            return self.cur.fetchall()
        else:
            self.connect()
            self.Persons(all, clustid, textid, alias)

    def Clusters(self, all=None, textid=None, persons=None):
        """
        :param all: при all!=None выдает содержание всей таблицы clusters
        :param text: заданные параметры textid означают, что кластеры будут выдаваться в соответствии с номером текста
        :param persons: заданные параметры persons означают, что кластеры будут выдаваться относительно персонажей в них
        :return: возвращает номер и имя кластера персонажа
        """
        if self.con:
            if all!=None:
                query = 'SELECT * FROM clusters;'
            elif textid!=None:
                query = 'SELECT * FROM texts NATURAL JOIN ptrelations NATURAL JOIN persons NATURAL JOIN clusters WHERE ( '+self.select_condition('textid',textid)+' );'
            elif persons!=None:
                query = 'SELECT * FROM persons WHERE ('+self.select_condition('persname',persons)+');'
            print query
            self.cur.execute(query)
            return self.cur.fetchall()
        else:
            self.connect()
            self.Clusters(all, textid, persons)

    def insert(self, table_name, column_names=None, values=[]):
        """
        :param table_name: мы ведь должны указывать название таблицы, иначе куда делать insert
        :param column_names: и column_names тоже, по желанию
        :param values: и то, что добавить хотим
        :return: просто commit it
        """
        columns = ''
        if column_names != None: #если указываются, добавляем в раздел со столбцами
                assert len(column_names) == len(values)
                columns+='('+', '.join(column_names)+')'
        self.cur.execute('INSERT INTO '+ table_name+' '+columns+' '+'VALUES ('+', '.join(psycopg2.extensions.adapt(i).getquoted() for i in values)+');')
        self.con.commit()
        return

def data_retrieval():
    """
    до сих пор точно не понимаю, что ты от меня хочешь здесь, как я буду сразу прописывать на вход несколько запросов
    :return: result
    """
    dr = SQL_query()
    dr.connect()
    #dr.Texts(persons=['Bob','Tom']) )
    #dr.Clusters(text=[2])
    result = dr.Persons(alias=['Bob'])
    dr.close()
    return result

print data_retrieval()

