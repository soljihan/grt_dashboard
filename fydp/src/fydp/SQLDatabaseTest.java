package fydp;

import java.sql.*;
import com.microsoft.sqlserver.jdbc.*;

public class SQLDatabaseTest {

    public static void main(String[] args) {
        String connectionString =
            "jdbc:sqlserver://bentobox.database.windows.net:1433;"
            + "database=FYDP;"
            + "user=fydp2016@bentobox;"
            + "password=stacey1o1!;"
            + "encrypt=true;"
            + "trustServerCertificate=false;"
            + "hostNameInCertificate=*.database.windows.net;"
            + "loginTimeout=30;";
        
        // Declare the JDBC objects.
        Connection connection = null;
        Statement statement = null;

        try {
            connection = DriverManager.getConnection(connectionString);
            
            //drop tables
            Statement drop = connection.createStatement();
            String d1 = "DROP TABLE stopinstance";
            String d2 = "DROP TABLE stop";
            String d3 = "DROP TABLE bustrip";
            String d4 = "DROP TABLE routedirection";
            String d5 = "DROP TABLE busroute";
            String d6 = "DROP TABLE bus";
            drop.addBatch(d1);
            drop.addBatch(d2); 
            drop.addBatch(d3);
            drop.addBatch(d4);
            drop.addBatch(d5);
            drop.addBatch(d6);
    //        drop.executeBatch();
            
            //create tables
            Statement create = connection.createStatement();

            String c1 = "CREATE TABLE Bus " +
                    "(Id integer NOT NULL identity(1,1), " +
                    "PRIMARY KEY (Id))";
            
            String c2 = "CREATE TABLE BusRoute " +
            		"(routeName varchar(300) NOT NULL, " +
                    "PRIMARY KEY (routeName))";

            String c3 = "CREATE TABLE Stop " +
                    "(name varchar(300) not NULL, " +
            		"longitude decimal(8,6) not NULL, " +
            		"latitude decimal(8,6) not NULL, " +
                    "PRIMARY KEY (name))";            
  
            String c4 = "CREATE TABLE RouteDirection " +
                    "(Id integer NOT NULL identity(1,1), " +
            		"patternName varchar(300) NOT NULL, " +
                    "busroute varchar(300) NOT NULL, " +
                    "PRIMARY KEY (Id), " +
                    "FOREIGN KEY (busroute) REFERENCES BusRoute (routeName))";
            
            String c5 = "CREATE TABLE BusTrip " +
                    "(Id integer NOT NULL identity(1,1), " +
            		"date date NOT NULL, " +
                    "dayofweek varchar(300), " +
                    "timeofday time NOT NULL, " +
            		"time varchar(300), " +
            		"pattern varchar(300), " +
            		"tripType varchar(300) NOT NULL, " +
                    "busId integer NOT NULL, " +
                    "busRouteId varchar(300) NOT NULL, " +
                    "RouteDirectionId1 integer NOT NULL, " +
            		"PRIMARY KEY (Id), " +
                    "FOREIGN KEY (busId) REFERENCES Bus (Id), " +
            		"FOREIGN KEY (busRouteId) REFERENCES BusRoute (routeName), " +
    				"FOREIGN KEY (RouteDirectionId1) REFERENCES RouteDirection (Id))";    
           
            String c6 = "CREATE TABLE StopInstance " +
                    "(Id integer NOT NULL identity(1,1), " +
            		"load integer NOT NULL, " +
            		"boardings integer NOT NULL, " +
                    "alightings integer NOT NULL, " +
                    "stopId varchar(300) NOT NULL, " +
                    "tripId integer NOT NULL, " +
                    "type varchar(300) NOT NULL, " +
            		"PRIMARY KEY (Id), " +
                    "FOREIGN KEY (stopId) REFERENCES Stop (name), " +
            		"FOREIGN KEY (tripId) REFERENCES BusTrip (Id))";  
            
            create.addBatch(c1);
            create.addBatch(c2); 
            create.addBatch(c3);
            create.addBatch(c4);
            create.addBatch(c5);
            create.addBatch(c6);
     //       create.executeBatch();
            
            
            //update bustrip table
            Statement update = connection.createStatement();
            
            String u1 = "UPDATE bustrip "
            		+ "SET time = (CASE "
            		+ 	"WHEN timeofday >= '04:00:00' AND timeofday <= '07:00:00' "
            		+ 		"THEN 'Morning' "
            		+ 	"WHEN timeofday > '07:00:00' AND timeofday <= '10:00:00' "
            		+ 		"THEN 'Morning Peak' "
            		+ 	"WHEN timeofday > '10:00:00' AND timeofday <= '16:00:00' "
            		+ 		"THEN 'Afternoon' "
            		+ 	"WHEN timeofday > '16:00:00' AND timeofday <= '19:00:00' "
            		+ 		"THEN 'Evening Peak'"
            		+ 	"WHEN timeofday > '19:00:00' OR timeofday < '04:00:00' "
            		+ 		"THEN 'Evening' "
            		+ 	"ELSE 'Error' "
            		+ "END)";
            
            String u2 = "UPDATE bustrip "
            		+ "SET dayofweek = (CASE "
            		+ 	"WHEN datename(weekday,date) = 'Monday' "
            		+ 		"THEN 'Weekday' "
            		+ 	"WHEN datename(weekday,date) = 'Tuesday' "
            		+ 		"THEN 'Weekday' "
            		+ 	"WHEN datename(weekday,date) = 'Wednesday' "
            		+ 		"THEN 'Weekday' "
            		+ 	"WHEN datename(weekday,date) = 'Thursday' "
            		+ 		"THEN 'Weekday' "
            		+ 	"WHEN datename(weekday,date) = 'Friday' "
            		+ 		"THEN 'Weekday' "
            		+ 	"WHEN datename(weekday,date) = 'Saturday' "
            		+ 		"THEN 'Weekend' "
            		+ 	"WHEN datename(weekday,date) = 'Sunday' "
            		+ 		"THEN 'Weekend' "
            		+ "END)";
            
            update.addBatch(u1);
            update.addBatch(u2);

      //      update.executeBatch();
            
            //aggregate sequel
            Statement aggregated = connection.createStatement();
            
            String a1 = "CREATE VIEW a1 "
            		+ "WITH SCHEMABINDING "
            		+ "AS "
            		+ "SELECT b.Id, b.dayofweek, b.time, b.pattern, b.tripType, b.busRouteId, r.patternName "
            		+ "FROM dbo.BusTrip b, dbo.RouteDirection r "
            		+ "WHERE b.RouteDirectionId1 = r.Id; ";
            
            String a2 = "CREATE VIEW a2 "
            		+ 	"WITH SCHEMABINDING "
            		+ 	"AS "
            		+ 	"SELECT s.Id, a.busRouteId, a.patternName, s.load, s.boardings, s.alightings, s.stopId, s.tripId, s.type, "
            		+ 	"a.time, a.dayofweek, a.pattern, a.tripType "
            		+ 	"FROM dbo.StopInstance s "
            		+ 	"LEFT JOIN dbo.[a1] a "
            		+ 	"ON s.tripId = a.Id ";
            
            //percentiles
            String loadtwfv = "CREATE VIEW loadtwfv "
            		+ "WITH SCHEMABINDING AS "
            		+ "SELECT TOP 25 PERCENT load "
            		+ "FROM dbo.[a2] "
            		+ "ORDER BY load ASC ";
            
            String loadsvfv = "CREATE VIEW loadsvfv "
            		+ "WITH SCHEMABINDING AS "
            		+ "SELECT TOP 75 PERCENT load "
            		+ "FROM dbo.[a2] "
            		+ "ORDER BY load ASC ";
            
            String boardtwfv = "CREATE VIEW boardtwfv "
            		+ "WITH SCHEMABINDING AS "
            		+ "SELECT TOP 25 PERCENT boardings "
            		+ "FROM dbo.[a2] "
            		+ "ORDER BY boardings ASC ";
            
            String boardsvfv = "CREATE VIEW boardsvfv "
            		+ "WITH SCHEMABINDING AS "
            		+ "SELECT TOP 75 PERCENT boardings "
            		+ "FROM dbo.[a2] "
            		+ "ORDER BY boardings ASC ";
            
            String alighttwfv = "CREATE VIEW alighttwfv "
            		+ "WITH SCHEMABINDING AS "
            		+ "SELECT TOP 25 PERCENT alightings "
            		+ "FROM dbo.[a2] "
            		+ "ORDER BY alightings ASC ";
            
            String alightsvfv = "CREATE VIEW alightsvfv "
            		+ "WITH SCHEMABINDING AS "
            		+ "SELECT TOP 75 PERCENT alightings "
            		+ "FROM dbo.[a2] "
            		+ "ORDER BY alightings ASC ";
                      
            
            
            String agg = "CREATE VIEW aggregated "
            		+ "WITH SCHEMABINDING "
            		+ "AS "
            		+ "SELECT min(a.Id) as stopInstanceId, a.stopId, a.busRouteId, a.patternName, a.time, a.dayofweek, "
            		+ "count(*) as numOfTrips, "
            		+ "min(a.load) as minload, avg(cast(a.load as float)) as meanload, "
            		+ "max(a.load) as maxload, var(cast(a.load as float)) as varload, "            		
            		+ "min(a.boardings) as minboarding, "
            		+ "avg(cast(a.boardings as float)) as meanboarding, "
            		+ "max(a.boardings) as maxboarding, var(cast(a.boardings as float)) as varboarding, "            		
            		+ "min(a.alightings) as minalighting, "
            		+ "avg(cast(a.alightings as float)) as meanalighting, "
            		+ "max(a.alightings) as maxalighting, var(cast(a.alightings as float)) as varalighting "            		
            		+ "FROM dbo.[a2] a "
            		+ "GROUP BY a.stopId, a.busRouteId, a.patternName, a.time, a.dayofweek "
            		+ "HAVING count(*) > 3 ";
            
            String loadPercentiles = "CREATE VIEW loadPercentiles "
            		+ "WITH SCHEMABINDING "
            		+ "AS "
            		+ "SELECT min(a.Id) as stopInstanceId, a.stopId, a.busRouteId, a.patternName, a.time, a.dayofweek, "
            		+ "count(*) as numOfTrips, "
            		+ "max(lt.load) as q1load, "
            		+ "max(ls.load) as q3load "            		           		
            		+ "FROM dbo.[a2] a, dbo.[loadtwfv] lt, dbo.[loadsvfv] ls "
            		+ "GROUP BY a.stopId, a.busRouteId, a.patternName, a.time, a.dayofweek "
            		+ "HAVING count(*) > 3 ";
            
            String dropView = "drop view aggregated ";
            
            //route eff, passenger act, peak load factor
            String perfMetric = "CREATE VIEW perfMetric AS "
            		+ "SELECT ISNULL(ROW_NUMBER() OVER (ORDER BY busRouteId, patternName, time, dayofweek),0) AS row, "
            		+ "busRouteId, patternName, time, dayofweek, "
            		+ "sum(meanload) as totalLoad, count(*) as numOfStops, count(*)*35 as availVol, "
            		+ "(sum(cast(meanload as float))*100)/(count(*)*35) as routeEff, (max(cast(meanload as float))/35)*100 as peakLoadFactor,"
            		+ "((sum(cast(meanboarding as float))+sum(cast(meanalighting as float)))/count(*)) as passengerAct "
            		+ "FROM dbo.[aggregated] "
            		+ "GROUP BY busRouteId, patternName, time, dayofweek "
            		+ "HAVING count(*) > 2";

            
            aggregated.addBatch(loadPercentiles);
       //     aggregated.addBatch(dropView);
            aggregated.executeBatch();
            
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // Close the connections after the data has been handled.
            if (statement != null) try { statement.close(); } catch(Exception e) {}
            if (connection != null) try { connection.close(); } catch(Exception e) {}
        }
    }
}