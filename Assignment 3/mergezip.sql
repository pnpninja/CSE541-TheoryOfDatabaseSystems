CREATE TABLE CSE532.ZIP_INFO(
	ZIP INTEGER NOT NULL,
	SHAPE DB2GSE.ST_GEOMETRY,
	ZPOP DECFLOAT(16) DEFAULT 0,
	ANALYZED BOOLEAN DEFAULT FALSE,
	MERGED_WITH INTEGER
)COMPRESS YES;@
DROP INDEX CSE532.ZIP_INFO_ZIP_IDX;@
DROP INDEX CSE532.SHAPETEMPIDX;@
CREATE INDEX CSE532.ZIP_INFO_ZIP_IDX ON CSE532.ZIP_INFO (ZIP);@
CREATE INDEX CSE532.SHAPETEMPIDX on CSE532.ZIP_INFO(SHAPE) extend using db2gse.spatial_index(0.85, 2, 5);@
CREATE OR REPLACE PROCEDURE CSE532.LOAD_DATA_FOR_ANALYSIS()
LANGUAGE SQL
BEGIN 
	DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
	DECLARE TEMPZIP INTEGER;
	DECLARE cursor1 CURSOR FOR SELECT DISTINCT CAST(substr(GEOID10,1,5) AS INTEGER) FROM CSE532.USZIP;
	OPEN cursor1;
    FETCH FROM cursor1 INTO TEMPZIP;
    WHILE(SQLSTATE = '00000')
    DO
    	INSERT INTO CSE532.ZIP_INFO (ZIP, SHAPE, ZPOP) SELECT a.ZIP, b.SHAPE, a.ZPOP FROM CSE532.ZIPPOP a, CSE532.USZIP b WHERE a.ZIP = CAST(substr(b.GEOID10,1,5) AS INTEGER) AND a.ZIP = TEMPZIP LIMIT 1;
    	FETCH FROM cursor1 INTO TEMPZIP;
    END WHILE;
    CLOSE cursor1;
END@
CALL CSE532.LOAD_DATA_FOR_ANALYSIS();@
CREATE OR REPLACE PROCEDURE CSE532.AVERAGE_ZIP_POPULATION(OUT AVERAGE_POP DECFLOAT)
LANGUAGE SQL
BEGIN 
	DECLARE cursor2 CURSOR FOR SELECT AVG(ZPOP) FROM CSE532.ZIP_INFO;
	OPEN cursor2;
	FETCH FROM cursor2 INTO AVERAGE_POP;
	CLOSE cursor2;
END@
CREATE OR REPLACE PROCEDURE CSE532.GET_POP(IN IN_ZIP INTEGER, OUT OUT_POP DECFLOAT)
LANGUAGE SQL
BEGIN
	DECLARE cursor1 CURSOR FOR SELECT ZPOP FROM CSE532.ZIP_INFO WHERE ZIP = IN_ZIP;
	OPEN cursor1;
	FETCH FROM cursor1 INTO OUT_POP;
	CLOSE cursor1;
END@
CREATE OR REPLACE PROCEDURE CSE532.TO_ANALYZE_ZIP(IN IN_ZIP INTEGER, OUT TO_ANALYZE_FLAG BOOLEAN)
LANGUAGE SQL
BEGIN
	DECLARE cursor1 CURSOR FOR SELECT ANALYZED FROM CSE532.ZIP_INFO WHERE ZIP = IN_ZIP;
	OPEN cursor1;
	FETCH FROM cursor1 INTO TO_ANALYZE_FLAG;
    SET TO_ANALYZE_FLAG = NOT TO_ANALYZE_FLAG;
	CLOSE cursor1;
END@
CREATE OR REPLACE PROCEDURE CSE532.GET_UNANALYZED_NEIGHBORS_AND_CHECK_FOR_MERGE(IN IN_ZIP INTEGER,IN IN_POP DECFLOAT, IN AVGPOP DECFLOAT)
LANGUAGE SQL 
BEGIN
	DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
	DECLARE NEIGHBOR_ZIP INTEGER;
	DECLARE NEIGHBOR_POP DECFLOAT;
	DECLARE cursor1 CURSOR FOR SELECT ZIP,ZPOP FROM CSE532.ZIP_INFO WHERE db2gse.st_touches((SELECT SHAPE FROM CSE532.ZIP_INFO WHERE ZIP = IN_ZIP),SHAPE) = 1 AND ANALYZED = FALSE;
	OPEN cursor1;
	FETCH FROM cursor1 INTO NEIGHBOR_ZIP,NEIGHBOR_POP;
	WHILE(SQLSTATE = '00000')
	DO
		IF(NEIGHBOR_POP >= AVGPOP) THEN
			UPDATE CSE532.ZIP_INFO SET ANALYZED = TRUE, MERGED_WITH = IN_ZIP WHERE ZIP = NEIGHBOR_ZIP;
			UPDATE CSE532.ZIP_INFO SET ZPOP = ZPOP + NEIGHBOR_POP WHERE ZIP = IN_ZIP;
			UPDATE CSE532.ZIP_INFO SET SHAPE = (SELECT db2gse.st_union(a.SHAPE,b.SHAPE)UNION FROM CSE532.ZIP_INFO a, CSE532.ZIP_INFO b WHERE a.ZIP = IN_ZIP AND b.ZIP = NEIGHBOR_ZIP) WHERE ZIP = IN_ZIP;
		END IF;
		FETCH FROM cursor1 INTO NEIGHBOR_ZIP,NEIGHBOR_POP;
	END WHILE;
	CLOSE cursor1;
	UPDATE CSE532.ZIP_INFO SET ANALYZED = TRUE, MERGED_WITH = IN_ZIP WHERE ZIP = IN_ZIP;
END@
CREATE OR REPLACE PROCEDURE CSE532.START_ZIP_MERGE()
LANGUAGE SQL
BEGIN 
	DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
	DECLARE AVGPOP DECFLOAT;
	DECLARE ZIP_FOR_ANALYSIS INTEGER;
	DECLARE ZIP_FOR_ANALYSIS_POP DECFLOAT;
	DECLARE TO_ANALYZE BOOLEAN;
	DECLARE cursor3 CURSOR FOR SELECT ZIP FROM CSE532.ZIP_INFO ORDER BY ZPOP;
	CALL CSE532.AVERAGE_ZIP_POPULATION(AVGPOP);
	OPEN cursor3;
	FETCH FROM cursor3 INTO ZIP_FOR_ANALYSIS;
	WHILE(SQLSTATE = '00000')
	DO
		CALL CSE532.TO_ANALYZE_ZIP(ZIP_FOR_ANALYSIS,TO_ANALYZE);
		IF(TO_ANALYZE = TRUE) THEN
			CALL CSE532.GET_POP(ZIP_FOR_ANALYSIS, ZIP_FOR_ANALYSIS_POP);
			CALL CSE532.GET_UNANALYZED_NEIGHBORS_AND_CHECK_FOR_MERGE(ZIP_FOR_ANALYSIS,ZIP_FOR_ANALYSIS_POP,AVGPOP);
		END IF;
		FETCH FROM cursor3 INTO ZIP_FOR_ANALYSIS;
	END WHILE;
	CLOSE cursor3;
END@
CALL CSE532.START_ZIP_MERGE();@
DELETE FROM CSE532.ZIP_INFO WHERE ZIP <> MERGED_WITH;@
SELECT * FROM CSE532.ZIP_INFO;@ 