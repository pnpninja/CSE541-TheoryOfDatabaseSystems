# README

The following scripts make the assumption that the tables reside in the **SAMPLE**  database. 

The following files do the following

 1. `facilityinsert.sql` - Script to insert data into **CSE532.FACILITY** by selecting data from **CSE532.FACILITYORIGINAL** table and converting (_Latitude, Longitude_) attributes into **DB2GSE.ST_POINT type with srs_id 1** for **_geolocation_** attribute in **CSE532.FACILITY**
 2. `createfacilititycertificationtable.sql` - Creates **CSE532.FACILITYCERTIFICATION** table
 3. `createindexes.sql` - Creates indexes over the tables for faster query
 4. `nearester.sql` - Find closest healthcare facility with an ER room  *(AttributeValue = 'Emergency Department')* and its distance from **"2799 Horseblock Road Medford, NY 11763"** (Coordinates - 40.824369, -72.993983) 
 5. `noerzips.sql` - Script to find zip codes without any "Emergency Department", neither in their neighboring zip codes.
 6. `mergezip.sql` - Script to merge zip code areas into large ones with neighboring zip code areas, so that the new population in each merged region (combined zipcodes) is large than the current average population, using the zip code population table in Homework 1.

### Query Performance Statistics

| Script Name   | Runtime before Indexing | Runtime after Indexing |
|---------------|-------------------------|------------------------|
| nearester.sql | 0.050399 seconds        | 0.049363 seconds       |
| noerzips.sql  | 13.049493 seconds       | 1.144129 seconds       |

### Instruction for Running Scripts

For `facilityinsert.sql`, issue the following command - 

    db2 -tf facilityinsert.sql

For `createfacilititycertificationtable.sql`, issue the following command - 

    db2 -tf createfacilititycertificationtable.sql
For `createindexes.sql`, issue the following command - 

    db2 -tf createindexes.sql
 For `nearester.sql`, issue the following command - 

    db2 -tf nearester.sql
 For `noerzips.sql`, issue the following command - 

    db2 -tf noerzips.sql
   For `mergezip.sql`, issue the following command - 

    db2 connect to SAMPLE; db2 -td@ -f mergezip.sql



    






