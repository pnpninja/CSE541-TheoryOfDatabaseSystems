CONNECT TO SAMPLE;
CREATE OR REPLACE VIEW TEMP AS SELECT DISTINCT ZIP, ZPOP FROM CSE532.ZIPPOP WHERE ZPOP > 0;
CREATE OR REPLACE VIEW TEMP2 AS SELECT SUM(MME) AS TOTAL_MME, BUYER_ZIP FROM CSE532.DEA_NY GROUP BY BUYER_ZIP;
SELECT A.ZIP, B.TOTAL_MME/A.ZPOP AS NORM_MME, RANK () OVER (ORDER BY B.TOTAL_MME/A.ZPOP DESC) RANK FROM TEMP A INNER JOIN TEMP2 B ON A.ZIP = B.BUYER_ZIP ORDER BY NORM_MME DESC LIMIT 5;