CREATE TABLE IF NOT EXISTS clusters (clustid serial PRIMARY KEY, alias char(200));
CREATE TABLE IF NOT EXISTS PTrelations(PersonID INTEGER REFERENCES persons, TextID INTEGER REFERENCES texts);
CREATE TABLE IF NOT EXISTS persons (personid serial PRIMARY KEY, persname char(200), clustid int);
CREATE TABLE IF NOT EXISTS texts (textid serial PRIMARY KEY, textname char, source char, datepublic date, reviewed boolean);