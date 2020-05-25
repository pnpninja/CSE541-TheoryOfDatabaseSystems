
# README

The following files do the following job - 

 1. `Covid19.jar` - JAR File that runs Hadoop Jobs
 2. `SparkCovid19.jar` - JAR File that runs Spark Jobs
 3. `todbs_assignment` - Maven Project with all the code for running Hadoop Jobs
 4. `todbs_assignment_spark` - Maven Project with all the code for running Spark Jobs
                                                              |

### Requirements
1. Hadoop 2.8.0 and above (but not 3.0 and above)
2. Spark 2.4.0
3. JDK 8

### Instruction for Running Scripts for Hadoop Jobs

1. For finding the **total number of reported cases for every country/location till April 8th, 2020 with "World" data included** , the command is 
`hadoop jar Covid19.jar Covid19_1 <Path to Covid19 CSV File> true <Path to Output folder>` 

2. For finding the **total number of reported cases for every country/location till April 8th, 2020 without "World" data included**, the command is 
`hadoop jar Covid19.jar Covid19_1 <Path to Covid19 CSV File> false <Path to Output folder>` 

3. For finding the **total number of deaths for every country/location in a date range**, the command is 
`hadoop jar Covid19.jar Covid19_2 <Path to Covid19 CSV File> <Start Date> <End Date> <Path to Output folder>` 

   The format for `Start Date` and `End Date` is **yyyy-MM-dd** (eg - 2020-04-08 is April 8, 2020)

4. For finding the total number of cases per 1 million population for every country, the command is 
`hadoop jar Covid19.jar Covid19_3 <Path to Covid19 CSV File> <Path to Populations CSV File> <Path to Output folder>` 

The format for `Start Date` and `End Date` is **yyyy-MM-dd** (eg - 2020-04-08 is April 8, 2020)

### Instruction for Running Scripts for Spark Jobs

1. For finding the **total number of deaths for every country/location in a date range**, the command is 
`spark-submit --class Covid19_2 SparkCovid19.jar <Path to Covid19 CSV File> <Start Date> <End Date> <Path to Output folder>` 

   The format for `Start Date` and `End Date` is **yyyy-MM-dd** (eg - 2020-04-08 is April 8, 2020)

2. For finding the total number of cases per 1 million population for every country, the command is 
`spark-submit --class Covid19_3 SparkCovid19.jar <Path to Covid19 CSV File> <Path to Populations CSV File> <Path to Output folder>` 

The format for `Start Date` and `End Date` is **yyyy-MM-dd** (eg - 2020-04-08 is April 8, 2020)

### Instruction for Building Jar

1. Go into each of the project folder
2. Run the dollowing command - `mvn clean compile package`
3. Go to `target` folder to find `Covid19.jar` in `todbs_assignment` (for Hadoop Job) and `SparkCovid19.jar` in `todbs_assignment_spark` (for Spark Job)





    






