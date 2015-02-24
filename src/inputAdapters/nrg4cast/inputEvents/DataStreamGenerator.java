package inputAdapters.nrg4cast.inputEvents;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import eventTypes.nrg4cast.Measurement;

public class DataStreamGenerator implements Runnable{

	@Override
	public void run() {
		//connect to database
		Connection dbCon = connectPsql("postgres", "pgAdmin3");
		// get measurements month by month starting with 2011-06-01
		// until 2015-02-18
		Calendar c = Calendar.getInstance();
		c.set(2011, 5, 1, 0, 0);
		Calendar end = Calendar.getInstance();
		end.set(2015, 1, 18, 0, 0);
		while(c.getTime().before(end.getTime())){
			ArrayList<Measurement> mes = getMeasurements(dbCon, c);
			//sendMeasurements to esper
			
			//increment calendar with one month
			c.add(Calendar.MONTH, 1);
			//System.out.println(mes.size());
		}
		
		
		try {
			dbCon.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private ArrayList<Measurement> getMeasurements(Connection con, Calendar time) {
		ArrayList<Measurement> result = new ArrayList<Measurement>();
		Calendar end = Calendar.getInstance();
		end.setTime(time.getTime());
		end.add(Calendar.MONTH, 1);
		
		String sql = "SELECT *FROM \"Measurements\"  WHERE "
				+ "timestamp>='"+time.getTime()+"' and "
				+ "timestamp<'"+end.getTime()+"'  "
				+ "ORDER BY timestamp ASC ";
	//	System.out.println(sql);
		try {
			Statement stmt = con.createStatement();
			stmt.execute(sql);
			ResultSet res = stmt.getResultSet();
			while(res.next()){
				
				/*"nodeId" text NOT NULL,
				  "nodeName" text,
				  "subjectId" text,
				  lat double precision,
				  lng double precision,
				  "sensorId" text NOT NULL,
				  value real,
				  "timestamp" timestamp without time zone NOT NULL,
				  "typeId" text,
				  "sensorType" text,
				  phenomenon text,
				  uom text,*/
				String nodeId = res.getString("nodeId");
				String nodeName = res.getString("nodeName");
				String subjectId= res.getString("subjectId");
				double lat = res.getDouble("lat");
				double lng = res.getDouble("lng");
				String sensorId = res.getString("sensorId");
				double value = res.getDouble("value");
				Date timestamp = res.getDate("timestamp");
				String typeId = res.getString("typeId");
				String sensorType = res.getString("sensorType");
				String phenomenon = res.getString("phenomenon");
				String uom = res.getString("uom");
				Measurement m = new Measurement(nodeId, nodeName, subjectId, 
						lat, lng, sensorId, value, timestamp, 
						typeId, sensorType, phenomenon, uom);
				result.add(m);
			}
			res.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return result;
	}


	private Connection connectPsql(String user, String password){
		System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");
 
		try {
 
			Class.forName("org.postgresql.Driver");
 
		} catch (ClassNotFoundException e) {
 
			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return null;
 
		}
 
		System.out.println("PostgreSQL JDBC Driver Registered!");
 
		Connection connection = null;
 
		try {
 
			connection = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/nrg4cast", user,
					password);
 
		} catch (SQLException e) {
 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
 
		}
 
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
			return connection;
		} else {
			System.out.println("Failed to make connection!");
			return null;
		}
	}

}
