# -*- coding: utf-8 -*-
import psycopg2
import sys
import xml.etree.ElementTree as ET
class SQL_functions():
    def __init__(self):
        self.con = None
        self.cur = None

    def open_config(self, config):
        tree = ET.parse(config)
        root = tree.getroot()
        string = ""
        for child in root:
            string+=child.tag+"='"+child.text+"' "
        return string

    def connection_on(self):
        try:
            self.con = psycopg2.connect(self.open_config("config.xml"))
            self.cur = self.con.cursor()
            return 'success'
        except psycopg2.DatabaseError, e:
            if self.con:
                self.con.rollback()
            return "fail"

    def connection_off(self):
        if self.con:
            self.con.close()
        return "success"

    def execution(self, first, second, third):
        self.cur.execute(first+" "+second+" ("+third+");")

    def create_table(self, table_name, dict_columns, prim_key=""):
        column_string = ''
        for i in dict_columns:
            column_string+=i+" "+dict_columns[i]+" "
        column_string+="PRIMARY KEY ("+prim_key+")"
        self.execution("CREATE TABLE IF NOT EXISTS", table_name, column_string)

    def insert(self, table_name, column_names="", values=[]):
        if column_names=="":
            second_part = table_name+" ("+column_names+") "+"VALUES"
        else:
            second_part = table_name+" "+"VALUES"
        for group_value in values:
            third=""
            for value in group_value:
                if group_value.index(value)!=len(group_value)-1:
                    third+=value+", "
                else:
                    third+=value
            self.execution("INSERT INTO", second_part, third)

    def select(self, choice, *args, **kwargs):
        choix = ""
        if type(choice)==list:
            for i in choice:
                if choice.index(i)!=len(choice)-1:
                    choix+=i+", "
                else:
                    choix+=i+" "
        else:
            choix=choice
        first = "SELECT "+choix+" FROM "+self.where(choice,args,kwargs)
        second = " WHERE "
        third = self.condition_for_select(list(args),kwargs)+" 1=1"
        return first + second + third

    def arguments(self, arg):
        list_args = []
        for i in range(len(arg)): #находим знаки препинания (учитывая варианты <> != >= <=) и захватываем строку
                if arg[i]=='<' or arg[i]=='>' or arg[i]=='=':
                    if '=' in arg[:i-1] or '>' in arg[:i-1] or '<' in arg[:i-1]: #если условий много, как например (clustid<>0 AND textid=1)
                        spaces = []
                        for smth in range(len(arg[:i-1])):
                            if arg[:i-1][smth] == " ":
                                spaces.append(smth)
                        if arg[i-1]!='<' and arg[i-1]!='>' and arg[i-1]!='=':
                            list_args.append(arg[max(spaces):i])

                    else: #если оно от начала и в одной строке не много условий
                        if arg[i-1]!='<' and arg[i-1]!='>' and arg[i-1]!='=':
                            list_args.append(arg[:i])
        return list_args

    def where(self, choice, args, kwargs): #таблицы, где мы будем искать заданные памаметры, чтобы узнать, какие таблицы нужны, составим список всех колонок
        intersections = []
        choice2 = {}
        for i in kwargs: #добавили из словаря названия переменных
            choice2[i]=[]
        for arg in args: #из "специального условия" тоже вытаскиваем колонки
            choice.extend(self.arguments(arg)) #функция по вытягиванию отденльно
        tables = self.where_search() #получаем словарь {таблица : колонки}
        for i in choice:
            choice2[i]=[]
        for table in tables:
            for ch in choice2:
                if ch in tables[table]:
                    choice2[ch].append(table)
        print choice2
        flag = 0
        for i in choice2:
            for j in choice2:
                if i!=j:
                    lisst = list(set(choice2[i]).intersection(choice2[j]))
                    if lisst!=[]:
                        intersections.extend(lisst)
                    else:
                        for a in choice2[i]:
                            for b in choice2[j]:
                                if len(set(tables[a]).intersection(tables[b]))!=0:
                                    intersections.extend([a,b])
                                else:
                                    flag+=1
        f = len(choice2)
        for se in choice2:
            f = f * len(choice2[se])
        if flag == f:
            for m in choice2:
                intersections.extend(choice2[m])
        intersections = list(set(intersections))
        print intersections
        if len(intersections) == 1:
            return intersections[0]
        what_to_use = self.what_to_join(intersections, tables)
        join_string=''
        for i in what_to_use:
            if what_to_use.index(i)!=len(what_to_use)-1:
                join_string+=i+" NATURAL JOIN "
            else:
                join_string+=i
        return join_string

    def what_to_join(self, intersections, tables): #ищем точки соприкосновения
        probable = []
        for i in intersections: #для каждой таблицы ищем общие столбцы в другой таблице
            for j in intersections:
                if i!=j:
                    if len(set(tables[i]).intersection(tables[j]))!=0: #если у них есть что-то общее, то мы их вернем
                        probable.extend([i,j])
        probable=list(set(probable))
        if len(probable)==len(intersections):
            return probable
        else:
            for table in tables:
                if table not in intersections:
                    length = 0
                    for s in intersections:
                        if len(set(tables[table]).intersection(tables[s]))!=0:
                            length+=1
                    intersections.append(table)
                    if length==len(intersections):
                        return intersections
                    else:
                        self.what_to_join(intersections,tables)

    def condition_for_select(self, *args):
        condition = ""
        for arg in args:
            if type(arg)==list:
                for a in arg:
                    condition+=" "+a+" AND"
            else:
                for ar in arg:
                    if type(arg[ar])==str:
                        condition+=" "+ar+"='"+arg[ar]+"' AND"
                    elif type(arg[ar])==list:
                        for i in arg[ar]:
                            if type(i)==str:
                                om="'"
                            else:
                                om=''
                            if arg[ar].index(i)!=len(arg[ar])-1:
                                condition+=" "+ar+"="+om+str(i)+om+" OR"
                            else:
                                condition+=" "+ar+"="+om+str(i)+om+" AND"
                    else:
                        condition+=" "+ar+"="+str(arg[ar])+" AND"
        return condition

    def where_search(self):
        self.connection_on()
        tables = {}
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        for row in self.cur.fetchall():
            tab = []
            self.cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='"+row[0]+"';")
            for r in self.cur.fetchall():
                    tab.append(r[0])
            tables[row[0]]=tab
        self.connection_off()
        return tables

    def combine_all(self):
        self.connection_on()
        self.con.commit()

a= SQL_functions()
print a.select(["textname"], persname='Bob')
