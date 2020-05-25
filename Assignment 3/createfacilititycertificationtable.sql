CONNECT TO SAMPLE;
DROP TABLE CSE532.facilitycertification;
CREATE TABLE CSE532.facilitycertification(
	FacilityID VARCHAR(16),
	FacilityName VARCHAR(128),
	Description VARCHAR(128),
	AttributeType VARCHAR(128),
	AttributeValue VARCHAR(128),
	MeasureValue VARCHAR(32),
	County VARCHAR(32)
);