CONNECT TO SAMPLE;
load from arcos-ny-statewide-itemized.csv of del modified by dateformat="MMDDYYYY" insert into CSE532.DEA_NY;