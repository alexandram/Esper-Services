package inputAdapters.nrg4cast.inputEvents;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.espertech.esper.client.EPRuntime;

import eventTypes.nrg4cast.Measurement;

public class HistoricalDataProcessing implements Runnable {
	private static final Logger log = Logger.getLogger
			(HistoricalDataProcessing.class.getName());
	private String path;
	private EPRuntime cepRT;



	public HistoricalDataProcessing(String path, EPRuntime cepRT) {
		super();
		this.path = path;
		this.cepRT = cepRT;

	}


	public void run() {
		// configure logger
		try {
			FileHandler fh = new FileHandler("dataProcessing2.log");
			Formatter newFormatter = new SimpleFormatter();
			fh.setFormatter(newFormatter );
			log.addHandler(fh);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//read file by file from path directory
		File dirPath = new File(this.path);
		String[] files;
		Date d = new Date(0);
		ArrayList<Measurement> measurements = new ArrayList<Measurement>();
		Connection dbCon = connectPsql("postgres", "pgAdmin3");

		if(dirPath.isDirectory()){
			files = dirPath.list();
			Charset charset = Charset.forName("US-ASCII");
			for(String filename: files){
				if(filename.matches("log-\\d{8}.txt")){
					System.out.println(filename);
					Path file = Paths.get(dirPath +"/"+filename);
					d = parseFileDate(filename.substring(4, 12));
					try (BufferedReader reader = Files.newBufferedReader(file, charset)) {
						String line = null;
						int nline=0;

						while ((line = reader.readLine()) != null) {
							List<Measurement> result = processLine(line);
							measurements.addAll(result);
							nline++;						
						}
						//insert in database

						String sql="";

												
						for(Measurement m : measurements){
							try {
								Statement stmt = dbCon.createStatement();	
								sql = "INSERT INTO \"Measurements\" " + 
										"VALUES ('"+m.getNodeId()+"','"+m.getNodeName()+"','"
										+ m.getSubjectId()+"',"+m.getLat()+","
										+ m.getLng()+ ",'"+m.getSensorId()+"',"
										+ m.getValue()+ ",'"+m.getTimestamp()+ "','"
										+ m.getTypeId() + "','" + m.getSensorType()
										+ "','" +m.getPhenomenon()+"','"+ m.getUom()+ "')";
								stmt.executeUpdate(sql);							


							} catch (SQLException e) {							
								log.info("error inserting in database \n" +
										"for query:" + sql + "\n"+
										"error message: " + e.getMessage() +"\n");
							}
						}

						measurements.clear();						


					} catch (IOException x) {
						System.err.format("IOException: %s%n", x);
					}
				}
			}
		} else {
			System.out.println("invalid path, path must be directory");
			return;
		}

		System.out.println("right before Thread.yield();	");
		Thread.yield();		
	}




	private List<Measurement> processLine(String line) {
		ArrayList<Measurement> meass = new ArrayList<Measurement>();
		if(line.length()>2){
			Object obj = JSONValue.parse(line);
			if(obj!=null){
				//parse array of nodes
				JSONArray arr = (JSONArray)obj;
				Date refTime = new Date(0);
				for(int i=0;i<arr.size();i++){
					JSONObject node = (JSONObject)
							((JSONObject)arr.get(i)).get("node");
					String id = node.get("id").toString();
					String name = node.get("name").toString();
					String subjectId = node.get("subjectid").toString();
					double lat = Double.parseDouble(node.get("lat").toString());
					double lng = Double.parseDouble(node.get("lng").toString());
					JSONArray ms = (JSONArray)node.get("measurements");
					//parse array of measurements
					for(int j=0;j<ms.size();j++){
						JSONObject m = (JSONObject)ms.get(j);
						double val;
						try{
							val = Double.parseDouble(m.get("value").toString());
						}catch (NumberFormatException e){
							//val = Double.POSITIVE_INFINITY;
							continue;
						}
						String sensorId = m.get("sensorid").toString();
						String timeStr = m.get("timestamp").toString();
						Date time = parseTimestamp(timeStr);
						if(refTime.after(time)){
							System.out.println();
						}
						JSONObject type = (JSONObject) m.get("type");
						String typeId = type.get("id").toString();
						String typeName = type.get("name").toString();
						String phen = type.get("phenomenon").toString();
						String uom = type.get("UoM").toString();
						Measurement mes = new Measurement(id, name,subjectId, 
								lat, lng, sensorId, val, time, typeId, typeName, 
								phen, uom);
						meass.add(mes);


					}

				}

			}else {
				System.out.println("incorrect format in line: " + line);
			}

		}
		return  meass;
	}

	public Date parseTimestamp(String timeStr) {
		//"2013-02-02T10:11:12.984",

		String timestamp = timeStr.replace("T", "-");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS", Locale.ENGLISH);
		Date result= null;

		try {
			result = df.parse(timestamp);		
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();			
		}  		
		return result;
	}

	private Date parseFileDate(String dateStr) {
		// 20140929
		DateFormat df = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
		Date result= null;
		try {
			result = df.parse(dateStr);
			Calendar cal = Calendar.getInstance();
			cal.setTime(result);
			cal.add(Calendar.HOUR_OF_DAY, 23);
			cal.add(Calendar.MINUTE, 59);
			cal.add(Calendar.SECOND, 59);
			result = cal.getTime();
		} catch (ParseException e) {
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
