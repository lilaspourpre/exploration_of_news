import psycopg2
import sys
import xml.etree.ElementTree as ET
class sql_functions():
    def __init__(self):
        self.con = None
        self.cur = None

    def open_config(config):
        tree = ET.parse(config)
        root = tree.getroot()
        string = ""
        for child in root:
            string+=child.tag+"='"+child.text+"' "
        return string

    def connection_on(self):
        try:
            self.con = psycopg2.connect(self.open_config("config.xml"))
            self.cur = self.con.curr()
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

    def select(self, choice, condition):
        first = "SELECT "+choice+" FROM "+ where
        second = "WHERE "
        third = condition
        self.execution(first, second, third)

    def combine_all(self):
        self.connection_on()
        column_string = ''
        self.con.commit()

    def where_search(self, choice, condition):
        tables = {}
        self.connection_on()
        self.cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        for row in self.cur.fetchall():
            tab = []
            self.cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='"+row[0]+"';")
            for r in self.cur.fetchall():
                    tab.append(r[0])
            tables[row[0]]=tab
        
        return tables



