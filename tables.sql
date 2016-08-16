CREATE TABLE IF NOT EXISTS texts (
	textid serial8 NOT NULL,
	textname varchar NOT NULL,
	source varchar NOT NULL,
	datepublic DATE NOT NULL CHECK (datepublic < CURRENT_DATE),
	CONSTRAINT Pk_articles PRIMARY KEY (textid)
);

CREATE TABLE IF NOT EXISTS clusters (
	clustid serial8 NOT NULL,
	alias varchar NOT NULL,
	CONSTRAINT Pk_clusters PRIMARY KEY (clustid)
);

CREATE TABLE IF NOT EXISTS persons (
	personid serial8 NOT NULL,
	persname varchar NOT NULL,
	clustid INTEGER NOT NULL,
	CONSTRAINT Pk_persons PRIMARY KEY (personid),
	CONSTRAINT Fk_persons FOREIGN KEY (clustid) REFERENCES clusters(clustid) ON DELETE RESTRICT ON UPDATE RESTRICT
);

CREATE TABLE IF NOT EXISTS ptrelations (
 	integer NOT NULL,
 	textid integer NOT NULL,
 	CONSTRAINT Pk_ptrel PRIMARY KEY (personid, textid),
 	CONSTRAINT Fk_ptrel FOREIGN KEY (personid) REFERENCES persons(personid) ON DELETE RESTRICT ON UPDATE RESTRICT,
 	CONSTRAINT Fk_ptrel2 FOREIGN KEY (textid) REFERENCES texts(textid) ON DELETE RESTRICT ON UPDATE RESTRICT
 );
