# -*- coding: utf-8 -*-
import database_search_engine as dse, psycopg2

RP = dse.SQL_query()
RP.connect()
#RP.cur.execute('DROP TABLE IF EXISTS articles')
#RP.cur.execute('CREATE TABLE IF NOT EXISTS texts (textid serial8 NOT NULL, textname varchar NOT NULL, source varchar NOT NULL, datepublic DATE NOT NULL CHECK (datepublic < CURRENT_DATE), CONSTRAINT Pk_articles PRIMARY KEY (textid))')
#RP.cur.execute('CREATE TABLE IF NOT EXISTS clusters (clustid serial8 NOT NULL, alias varchar NOT NULL, CONSTRAINT Pk_clusters PRIMARY KEY (clustid))')
#RP.cur.execute('CREATE TABLE IF NOT EXISTS persons (personid serial8 NOT NULL, persname varchar NOT NULL, clustid INTEGER NOT NULL, CONSTRAINT Pk_persons PRIMARY KEY (personid), CONSTRAINT Fk_persons FOREIGN KEY (clustid) REFERENCES clusters(clustid) ON DELETE RESTRICT ON UPDATE RESTRICT)')
#RP.cur.execute('CREATE TABLE IF NOT EXISTS ptrelations (personid integer NOT NULL, textid integer NOT NULL, CONSTRAINT Pk_ptrel PRIMARY KEY (personid, textid), CONSTRAINT Fk_ptrel FOREIGN KEY (personid) REFERENCES persons(personid) ON DELETE RESTRICT ON UPDATE RESTRICT, CONSTRAINT Fk_ptrel2 FOREIGN KEY (textid) REFERENCES texts(textid) ON DELETE RESTRICT ON UPDATE RESTRICT)')
RP.con.commit()
try:
    RP.close()
except psycopg2.DatabaseError, e:
    print e