# -*- coding: utf-8 -*-
import psycopg2
import xml.etree.ElementTree as ET
from psycopg2.extensions import adapt

class SQL_query():
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
        condition = []
        for i in data:
            condition.append(columnname + '=' + psycopg2.extensions.adapt(i).getquoted())
        return ' OR '.join(condition)

    def Texts(self, all=None, persons=None, clustid=None, alias=None):
        if self.con:
            if all!=None:
                query = 'SELECT * FROM texts;'
            if alias!=None:
                query = 'SELECT texid, textname FROM texts NATURAL JOIN ptrelations NATURAL JOIN persons NATURAL JOIN clusters WHERE ( '+self.select_condition('alias',alias)+' );'
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

    def Clusters(self, all=None, text=None, persons=None):
        if self.con:
            if all!=None:
                query = 'SELECT * FROM clusters;'
            elif text!=None:
                query = 'SELECT personid, persname FROM texts NATURAL JOIN ptrelations NATURAL JOIN persons NATURAL JOIN clusters WHERE ( '+self.select_condition('textid',text)+' );'
            elif persons!=None:
                query = 'SELECT personid, persname FROM persons WHERE ('+self.select_condition('persname',persons)+');'
            print query
            self.cur.execute(query)
            return self.cur.fetchall()
        else:
            self.connect()
            self.Clusters(all, text, persons)

    def insert(self, table_name, column_names=None, values=[]): #добавление строк столбцы могут как указываться, так и нет
        columns = ''
        if column_names != None: #если указываются, добавляем в раздел со столбцами
                assert len(column_names) == len(values)
                columns+='('+', '.join(column_names)+')'
        self.cur.execute('INSERT INTO '+ table_name+' '+columns+' '+'VALUES ('+', '.join(psycopg2.extensions.adapt(i).getquoted() for i in values)+');')
        self.con.commit()
        return

def data_retrieval():
    dr = SQL_query()
    dr.connect()
    #dr.Texts(persons=['Bob','Tom']) )
    #dr.Clusters(text=[2])
    result = dr.Persons(alias=['Bob'])
    dr.close()
    return result

for i in data_retrieval():
    print i

