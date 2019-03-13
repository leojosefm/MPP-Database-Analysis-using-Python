### Netezza Database Analysis using Python
This is a generic script to analyze Data distribution in a Netezza Database. 
This script can be modified (System table names are different in different Databases) and can be used with other MPP databases including Teradata, Retshift etc

The script generates an excel file that shows the data distribution of tables that are not properly distributed and other metrics as shown in the excel file.
The Chart shows that data is not evenly distributed across all SPUs for table SAMPLE_TABLE_DATA. Only 13/480 SPUs are used thereby reducing performance.

THe second sheet "Distribution" has other informations total record count; average record count per SPU, Distribute Key, Column with highest cardinality etc.

#### Script
All required information like hostname, user credentials, Database to connect, list of Databases to analyze are givn as input during the execution.

pyodbc:- Module used to connect to Netezza Database

Only those tables that have a record count of greater than 10,000 are considered in the script.



