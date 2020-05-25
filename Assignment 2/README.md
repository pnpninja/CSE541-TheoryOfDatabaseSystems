# README

The following scripts make the assumption that the tables reside in the **SAMPLE**  database. 

The following files do the following

 - **SalaryStdDev.sql** - Creates Stored Procedure to calculate Standard Deviation of Employees' Salary using SQL PL.
 -  **StandardStdDev.jar** - A JAR File that connects to SAMPLE database and calculates the standard deviation programatically using JDBC (Requires JDK 8+ to run)
 - **StandardStdDev.java** - JAVA File that connects to SAMPLE database and calculates the standard deviation programatically using JDBC (Requires JDK 8+ to run). Note - You need to compile it to run it. To compile, run the command - `javac SalaryStdDev.java`. This will generate a class file.
 - **StandardStdDev.class** - JAVA Classs File that can be run to calculate the Standard Deviation. To run the file, type this in terminal and press enter - `java -cp ".:jcc-11.5.0.0.jar:" SalaryStdDev <DATABASE> <TABLE> <USERNAME> <PASSWORD>`
 - **jcc-11.5.0.0.jar** - DB2 JDBC Driver
 - **todbs-assignment** -  Maven Project to connect to SAMPLE database and calculate the standard deviation programatically. This code will help generate the completely runnable  **StandardStdDev.jar** by issuing the command inside that folder - `mvn assembly:single`
 - **output-sp.txt** - Text output after running the SQL script having the Stored Procedure.
 - **output-jdbc.jpg** - Screenshot of the Java Program using JDBC calculating the standard deviation
 
To run the JAR to programatically, make sure you have JDK 8+ installed (cannot guarantee to execute on JDK 7). Run the following command - 

    java -jar SalaryStdDev.jar <DATABASE> <TABLE> <USERNAME> <PASSWORD>

Otherwise, you can first compile just the JAVA file and create the CLASS file that can be executed. To compile, run - 

	javac SalaryStdDev.java

This generates the class file that can be executed by running the following command in terminal - 

    java -cp ".:jcc-11.5.0.0.jar:" SalaryStdDev <DATABASE> <TABLE> <USERNAME> <PASSWORD>
    
Note - the program works only if the database runs on **localhost:50000**.

To run the SQL script that loads the stored procedure and executes it, run the following command - 

    db2 -td@ -f SalaryStdDev.sql



