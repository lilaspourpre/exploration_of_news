# -*- coding: utf-8 -*-
import psycopg2
import xml.etree.ElementTree as ET

class SQL_search(): #здесь располагаются функции для открытия/закртыия коннекта
    def __init__(self):
        self.con = None
        self.cur = None

    def open_config(self, config): #находит в файле xml код и строит дерево, возвращает строку для коннекта
        tree = ET.parse(config)
        root = tree.getroot()
        string = ""
        for child in root: #соединяем данные в строку user=postgres и тд
            string+=child.tag+"='"+child.text+"' "
        return string

    def connection_on(self): #подключение к БД
        try:
            self.con = psycopg2.connect(self.open_config("config.xml"))
            self.cur = self.con.cursor()
            return 'success on'
        except psycopg2.DatabaseError, e:
            if self.con:
                self.con.rollback()
            return "fail"

    def connection_off(self): #отключение от БД
        if self.con:
            self.con.close()
        return "success off"

class SQL_functions(SQL_search): #здесь хранятся функции, необходимые для запросов

    def execution(self, first, second, third): #общий сбор запроса
        self.cur.execute(first+" "+second+" ("+third+");")

    def create_table(self, table_name, dict_columns, prim_key=""): #создание таблицы
        column_string = ''
        for i in dict_columns:
            column_string+=i+" "+dict_columns[i]+", "
        column_string+="PRIMARY KEY ("+prim_key+")"
        self.execution("CREATE TABLE IF NOT EXISTS", table_name, column_string)
        self.con.commit()
        return 'done'

    def insert(self, table_name, column_names="", values=[]): #добавление строк столбцы могут как указываться, так и нет
        columns = ''
        if type(column_names)==list: #если указываются, добавляем в раздел со столбцами
            for i in column_names:
                if column_names.index(i)!=len(column_names)-1:
                    columns+=i+", "
                else:
                    columns+=i
            second_part = table_name+" ("+columns+") "+"VALUES"
        else: #если столбцы не указываются
            second_part = table_name+" "+"VALUES"
        third=""
        for value in values: #нужно учитывать тип данных(строка/не строка) и не ставить запятую елли это последнее значение
            if values.index(value)!=len(values)-1:
                if type(value)==str:
                    third+="'"+value+"', "
                else:
                    third+=str(value)+", "
            else:
                if type(value)==str:
                    third+="'"+value+"'"
                else:
                    third+=str(value)
        self.execution("INSERT INTO", second_part, third)
        self.con.commit()
        return 'commit accepted'

    def drop_table(self,table_name): #удаление таблицы
        self.cur.execute("DROP TABLE IF EXISTS "+table_name)
        return "done"

    def select(self, choice, *args, **kwargs): #адское месиво
        choix = ""
        if type(choice)==list: #первый пункт - выбор, либо *, либо одно значение, либо список
            for i in choice: #если список, то записываем все в строку
                if choice.index(i)!=len(choice)-1:
                    choix+=i+", "
                else:
                    choix+=i+" "
        else:
            choix=choice #если * или одно значение - просто заносим
        fromm = ''
        if args: #тут на самом деле раньше были *args, потому тут и проверка. но когда я начала собирать все в одно, то поняла, что *args и **kwargs совсем не подходят, нужно срочно все менять
            if type(args[0]) == list: #в *args перым подавался список где искать, если его нет - мы сами находим, где искать, а вот без *args просто в списке args[0] совсем стремно
                for i in args[0]: #если есть список, то мы записываем их через join или просто
                    if args[0].index(i)!=len(args[0])-1:
                        fromm+=i+" NATURAL JOIN "
                    else:
                        fromm+=i
                third = self.condition_for_select([x for x in args if args.index(x) > 0],kwargs)+" 1=1" #из оставшихся параметров формируем услвие в отдельной фунции
            else:
                fromm = self.where(choice,args,kwargs)  #если нет списка, где искать, идем в функцию where
                third = self.condition_for_select(list(args),kwargs)+" 1=1" #и формируем услвие в отдельной фунции
        else:
            fromm = self.where(choice,args,kwargs) #это было нужно, когда не было *args, но были *kwargs, это очень удобно было...
            third = self.condition_for_select(list(args),kwargs)+" 1=1"
        first = "SELECT "+choix+" FROM "+fromm #что и откуда
        second = " WHERE "
        print first + second + third
        self.execution(first, second, third)
        rows = self.cur.fetchall()
        return rows

    def arguments(self, arg): #парсер аргументов
        list_args = []
        for i in range(len(arg)): #находим знаки препинания (учитывая варианты <> != >= <=) и захватываем строку
                if arg[i]=='<' or arg[i]=='>' or arg[i]=='=':
                    if '=' in arg[:i-1] or '>' in arg[:i-1] or '<' in arg[:i-1]: #если условий много, как например (clustid<>0 AND textid=1)
                        spaces = []
                        for smth in range(len(arg[:i-1])): #мы находим все предыдущие пробелы и выбираем ближайший к нам (макс порядковое значение)
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
                    choice2[ch].append(table) #проходимся по всем колонкам и записываем, где что встречается
        flag = 0
        for i in choice2:
            for j in choice2:
                if i!=j:
                    lisst = list(set(choice2[i]).intersection(choice2[j])) #проходимся по всем колонкам и смотрим на пересечения в таблицах
                    if lisst!=[]: #если есть пересечения - добавляем
                        intersections.extend(lisst)
                    else: #если нет пересечений, то возможно просто таблицы имеют общие ключи(столбцы), проверяем это
                        for a in choice2[i]:
                            for b in choice2[j]:
                                if len(set(tables[a]).intersection(tables[b]))!=0:
                                    intersections.extend([a,b])
                                else: #если нет - ведем подсчет
                                    flag+=1
        f = len(choice2)
        for se in choice2:
            f = f * len(choice2[se])
        if flag == f: #если вышло так, что вообще ничего ни с чем не совпало, то мы добавляем все и отправляемся на поиски связующих компонентов
            for m in choice2:
                intersections.extend(choice2[m])
        intersections = list(set(intersections)) #получаем список всех таблиц
        if len(intersections) == 1: #если одна, значит все в одной
            return intersections[0]
        what_to_use = self.what_to_join(intersections, tables) #в других случаях надо смотреть, что там общего имеется
        join_string=''
        for i in what_to_use: #нашли все, что нужно
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
            for table in tables: #ищем таблицу соприкосновения
                if table not in intersections: #если таблица не в списке
                    length = 0
                    for s in intersections: #проходим по имеющемуся списку
                        if len(set(tables[table]).intersection(tables[s]))!=0: #если есть что-то общее у таблицы с другой таблицей
                            length+=1
                    intersections.append(table)
                    if length==len(intersections): #если совпадают эти параматры, то мы нашли таблицу, которая со всеми связана
                        return intersections
                    else:
                        return self.what_to_join(intersections,tables) #если нет - ищем дальше

    def condition_for_select(self, *args): #условия для выбора
        condition = ""
        for arg in args:
            print arg
            if type(arg)==list:
                for a in arg:
                    condition+=" "+a+" AND" #если OR то прописывать это вручную одной строкой
            else:
                for ar in arg: #для каждого элемента
                    if type(arg[ar])==str: #если значение элемента в словаре = строка
                        condition+=" "+ar+"='"+arg[ar]+"' AND" #тогда с кавычками
                    elif type(arg[ar])==list: #если это вида столбец:[несколько вариантов в списке]
                        for i in arg[ar]:
                            if type(i)==str:
                                om="'" #для тогда, чтобы записать значение как строку
                            else:
                                om='' #чтобы записать значение как цифру
                            if arg[ar].index(i)!=len(arg[ar])-1:
                                condition+=" "+ar+"="+om+str(i)+om+" OR" #если это неск вариантов, то нужно же OR
                            else:
                                condition+=" "+ar+"="+om+str(i)+om+" AND" #с последним элементом, который есть везде для селекта 1=1, нужен AND
                    else:
                        condition+=" "+ar+"="+str(arg[ar])+" AND" #если просто, одно, не строка, не список, число
        return condition

    def where_search(self): #это универсально, а не только для нашей таблицы (может зря), но поэтому есть штука, которая делает словарь {таблица:все столбцы}
        tables = {}
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        for row in self.cur.fetchall():
            tab = []
            self.cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='"+row[0]+"';")
            for r in self.cur.fetchall():
                    tab.append(r[0])
            tables[row[0]]=tab
        return tables



class SQL_perform(): #это просто ужас, потому что не особенно получилось запихнуть все, что написала в реализацию

    def __init__(self, ):
        self.classs = None

    def using_sql(self):
        self.classs = SQL_functions()
        self.classs.connection_on()
        for i in range(5):
            r = raw_input('What do you want to do? (select, insert, create table, drop table or exit) ')
            if r == 'exit':
                self.classs.connection_off()
                return 'end'
            else:
                result = self.choosing_what_to_do(r)
                if result!='done':
                    file_name = raw_input("Enter a filename where to save results ")
                    title = [desc[0] for desc in self.classs.cur.description]
                    print result
                    with open(file_name+'.txt','w') as f:
                        for i in title:
                            f.write(i+'\t')
                        f.write('\n')
                        for s in result:
                            for m in s:
                                f.write(str(m)+'\t')
                            f.write('\n')
        self.classs.connection_off()
        return 'succes'

    def choosing_what_to_do(self, *args):
        for i in args:
            if i == 'select':
                return self.classs.select(["*"],['ptrelations','texts','persons'], 'personid>0', 'personid>1', persname='Bob')
            elif i == 'insert':
                return self.classs.insert()
            elif i == 'create table':
                table_name=raw_input(" ")
                return self.classs.create_table('tab', {"tabid":'int','tabname':'text'}, 'tabid')
            elif i == 'drop table':
                return self.classs.drop_table(raw_input('enter tablename '))
            else:
                return 'Incorrect query'

ok = SQL_perform()
print ok.using_sql()