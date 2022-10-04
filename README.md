# Prog_Assign_Rajkumar

The entire Assignment is carried out with Java Programming Language

Main Class: Assignment.java,
			Execute Assignment.java class will trigger the data process.

Unit Test Class: DataExecutionTest.java

Output File Path: \src\main\output

Output Files:1.customer.csv
			2.transactions.csv
			3.dayOfWeek_Spendings.csv
			4.linkage_Customer.csv
	
	1.customer.csv and transactions.csv files are generated after processed based on the given objectives
	2.dayOfWeek_Spendings.csv - has the detailed summary of total spendings by days of week- how much customers spend on Mondays till Sundays
	3.linkage_Customer.csv - customer.csv is processed as below in the end to avoid the Linkage Attack. 
To avoid the linkage attack, 'person_id', 'postcode', 'state', 'sex' Column data are replaced with Unique Ids. 
The reference of the Unique Ids are present in the 'data.csv' file under path: \src\main\resources.
Due to the data have only Unique Id values, it will prevent the customer data from linkage attack. 
I found this as one of the effective way to avoid linkage attacks.
