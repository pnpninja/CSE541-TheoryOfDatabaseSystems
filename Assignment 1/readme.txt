# README

The following scripts make the assumption that the tables will be created and reside in the **SAMPLE**  database. If not, please change the scripts by running the following Bash Command

    sed -i '.sqlbackup''s/SAMPLE/<Database Name>/g' *

The following files do the following

 - **createdeatable.sql** - Creates DEA_NY Table
 - **load.sql** - Loads data into DEA_NY (please ensure the data is in ***arcos-ny-statewide-itemized.csv*** located on current folder. Otherwise, please change the script to point to the appropriate path)
 - **createdeaindexes.sql** - Creates necessary indices on DEA_NY table
 - **createzip.sql** - Creates ZIPPOP table
 - **zipload.sql** - Loads data into ZIPPOP table (please ensure that the data is in ***nyzip.csv***
 - **q3a.sql** - SQL script for 3A.
 - **q3a.jpg** - Image showing the monthly pill counts (smoothed and unsmoothed)
 - **q3a.csv** - Result of 3A in CSV
 - **q3b.sql** - SQL script for 3B.
 - **q3b.csv** -  Result of 3B in CSV

