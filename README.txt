A web dashboard interface is made to analyze the performance of Grand River Transit.

sample raw data.csv file is a sample data showing number of boardings, alightings, loadings,
actual arrival time, actual departure time at each stop for one of the bus trips made on Sept 22, 2015.

Database Design Schema.png shows the final database design and 
Relational Table file shows the relational table created using the final database design
(done in MySQL Workbench).

SQLDatabaseTest.java program shows the program connecting to the Microsoft Azure cloud server
and creates the database. Data are grouped by route number, route pattern, time of day, day of week.
Max, min, mean, and variance are calculated for boardings and alightings.

Final UI_summarypg, Final UI_RE_summarypg, and Final UI_DetailRouteInfopg show what the final product looks like as a web dashboard page.