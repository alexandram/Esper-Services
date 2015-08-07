package esperCore;



import inputAdapters.nrg4cast.inputEvents.PredictionDelay;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashMap;

import javax.servlet.ServletContext;

import listeners.nrg4cast.Nrg4castAlarmListener;


import org.apache.log4j.Logger;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;


import eventTypes.nrg4cast.*;



public class EsperNrg4Cast implements EsperInstance{


	static final Logger log = Logger.getLogger(EsperNrg4Cast.class);
	static String engineName = "nrg4cast";
	//static String configFile = "D:\\server\\TomcatNRG4Cast\\conf\\nrg4castApp\\esper.nrg4cast.cfg.xml"; //uncomment this for tomcat deployment
	static String configFile = "D:\\Users\\alexandram\\nrg4cast\\Esper-Services\\WebContent\\WEB-INF\\conf\\esper.nrg4cast.cfg.xml";
	static String queriesFile = "D:\\server\\TomcatNRG4Cast\\conf\\nrg4castApp\\queries.epl";
	Configuration cepConfig = null;
	EPServiceProvider cep = null;
	ServletContext context;
	PredictionDelay dataSource;

	public EsperNrg4Cast(ServletContext context){

		// The Configuration is meant only as an initialization-time object.
		cepConfig = new Configuration();
		// use File as a hack for fixing the issue with classpath run configuration, which seems that are not exported in .war
		// otherwise change configfile to the string name of the file and pass it directly as parameter	
		File f = new File(configFile);
		cepConfig.configure(f);
		// for offline analysis disable the internal timer	else comment the 2 lines below	
		// cepConfig.getEngineDefaults().getThreading().
		// setInternalTimerEnabled(false);
		addEventTypes(cepConfig);

		cep = EPServiceProviderManager.getProvider(
				engineName, cepConfig);		
		EPRuntime cepRT = cep.getEPRuntime();
		this.context = context;
		

		
		//tests
//		EPAdministrator cepAdm = cep.getEPAdministrator();
//		EPStatement cepStatement = cepAdm.createEPL("@Name(\"Phenomena\") select phenomenon from Measurement.win:time_batch(10 min).std:unique(phenomenon)");

	//	cepStatement.addListener(new CEPListener());



	}
	@Override
	public void addListeners(){
		// add listener for all queries added from saving modules
		EPAdministrator cepAdm = cep.getEPAdministrator();
		String[] names = cepAdm.getStatementNames();
		HashMap<String,String> returnMap = new HashMap<String,String>();
		for(String key:names){
			cepAdm.getStatement(key).addListener(new Nrg4castAlarmListener(this.context, key));

		}	

	}
	
	public void addDataSources(){
		//add data sources
		dataSource = new PredictionDelay();
		Thread tSync = new Thread(dataSource);
		tSync.setName("PredictionDelay");
		tSync.start();

	}

	@Override
	public void addEventTypes(Configuration config){
		config.addEventType("Measurement", Measurement.class.getName());
		config.addEventType("WeatherMeasurement", WeatherMeasurement.class.getName());
		config.addEventType("Prediction", Prediction.class.getName());
		config.addEventType("Alert", Alert.class.getName());

	}

	@Override
	public void shutdown(){
		dataSource.terminate();
		cep.destroy();
	}

	public static class CEPListener implements UpdateListener {

		public void update(EventBean[] newData, EventBean[] oldData) {
			log.info(newData[0].getUnderlying());
			System.out.println(newData[0].getUnderlying());
			if(newData != null){
				for(EventBean e: newData){

					EventType t= e.getEventType();
					log.info(e.getUnderlying().toString());
				}
			}


		}
	}

	@Override
	public String addQuery(String query,String queryName) {
		EPAdministrator cepAdm = cep.getEPAdministrator();
		try{
			EPStatement cepStatement = cepAdm.createEPL(query);

			cepStatement.addListener(new Nrg4castAlarmListener(this.context, cepStatement.getName()));
			//cepStatement.addListener(new CEPListener());
			log.info(queryName + " " + cepStatement.getName() + " added succesfully");
		}catch (Exception e){
			return "Error: " + e.getMessage();
		}
		updateQueryFile();
		return "Query added succesfully!";


	}
	//keep running queries in a file to be loaded when services start
	private void updateQueryFile() {
		EPAdministrator cepAdm = cep.getEPAdministrator();
		String[] names = cepAdm.getStatementNames();
		//create a new file if already exists
		Path newFile = Paths.get(queriesFile);
		try {
			Files.deleteIfExists(newFile);
			newFile = Files.createFile(newFile);
		} catch (IOException ex) {
			System.out.println("Error creating file");
		}
		for(String key:names){
			writeLineToFile(newFile, cepAdm.getStatement(key).getText());
		}

	}


	private void writeLineToFile(Path newFile, String text) {
		try(BufferedWriter writer = Files.newBufferedWriter(
				newFile, Charset.defaultCharset(), StandardOpenOption.APPEND)){
			writer.append(text + ";");
			System.out.println("line added: " + text);
			writer.newLine();
			writer.flush();
			writer.close();
		}catch(IOException exception){
			System.out.println("Error writing to file");
		}

	}
	@Override
	public String deleteQuery(String queryName) {
		String response = null;
		EPAdministrator cepAdm = cep.getEPAdministrator();
		String[] names = cepAdm.getStatementNames();
		if(Arrays.asList(names).contains(queryName)){
			EPStatement query = cepAdm.getStatement(queryName);
			query.removeAllListeners();
			query.stop();
			query.destroy();
			response = "Query " + queryName + " successfully removed!";
		} else {
			response = "Query " + queryName + " NOT FOUND!";
		}
		updateQueryFile();
		return response;
	}
	@Override
	public HashMap<String, String> getRegisteredQueries() {
		EPAdministrator cepAdm = cep.getEPAdministrator();
		String[] names = cepAdm.getStatementNames();
		HashMap<String,String> returnMap = new HashMap<String,String>();
		for(String key:names){
			returnMap.put(key, cepAdm.getStatement(key).getText());

		}
		return returnMap;
	}
	@Override
	public boolean isValidQueryName(String name) {
		EPAdministrator cepAdm = cep.getEPAdministrator();
		String[] names = cepAdm.getStatementNames();
		if(Arrays.asList(names).contains(name)){
			return false;
		}

		return true;
	}


}
