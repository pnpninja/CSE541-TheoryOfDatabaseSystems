CONNECT TO SAMPLE;
CREATE INDEX DEA_NY_BUYER_ZIP_IDX ON CSE532.DEA_NY (BUYER_ZIP);
CREATE INDEX DEA_NY_REPORTER_ZIP_IDX ON CSE532.DEA_NY (REPORTER_ZIP);
CREATE INDEX ZIPPOP_ZIP_IDX ON CSE532.ZIPPOP (ZIP);
CREATE INDEX DEA_NY_TRANSACTION_DATE_IDX ON CSE532.DEA_NY (TRANSACTION_DATE);
COMMIT;