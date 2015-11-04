import psycopg2
import sys
import xml.etree.ElementTree as ET

def open_config(config):
    tree = ET.parse(config)
    root = tree.getroot()
    string = ""
    for child in root:
        string+=child.tag+"='"+child.text+"' "
    return string

def connection_database():
    con = psycopg2.connect(open_config("config.xml"))
    cur = con.cursor()
    return

def create_table():
    