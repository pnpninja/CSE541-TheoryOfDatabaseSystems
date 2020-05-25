drop index cse532.facilityidx;
drop index cse532.zipidx;
drop index cse532.USZIP_GEOID10_IDX;
drop index cse532.FACILITY_ZIPCODE_IDX;
drop index cse532.FACILITYCERTIFICATION_ATTRIBUTEVALUE_IDX;

create index cse532.USZIP_GEOID10_IDX on CSE532.USZIP (GEOID10);

create index cse532.FACILITY_ZIPCODE_IDX on CSE532.FACILITY (ZIPCODE);

create index cse532.FACILITYCERTIFICATION_ATTRIBUTEVALUE_IDX on CSE532.FACILITYCERTIFICATION (ATTRIBUTEVALUE);

create index cse532.facilityidx on cse532.facility(geolocation) extend using db2gse.spatial_index(0.85, 2, 5);

create index cse532.zipidx on cse532.uszip(shape) extend using db2gse.spatial_index(0.85, 2, 5);

runstats on table cse532.facility and indexes all;

runstats on table cse532.uszip and indexes all;