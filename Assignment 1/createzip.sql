CONNECT TO SAMPLE;
CREATE TABLE CSE532.ZIPPOP (
	ZIP INTEGER NOT NULL,
	COUNTY INTEGER DEFAULT NULL,
	GEOID INTEGER DEFAULT NULL,
	ZPOP DECFLOAT(16) DEFAULT 0
)COMPRESS YES;