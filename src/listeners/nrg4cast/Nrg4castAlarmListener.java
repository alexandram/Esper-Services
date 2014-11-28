package listeners.nrg4cast;

import inputAdapters.nrg4cast.inputEvents.QMinerJSONInputService;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;

import javax.servlet.ServletContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.client.UpdateListener;

import eventTypes.nrg4cast.Alert;
import eventTypes.nrg4cast.AlertHeader;
import eventTypes.nrg4cast.PatternParameter;

public class Nrg4castAlarmListener implements UpdateListener {

	static final Logger log = Logger.getLogger(Nrg4castAlarmListener.class);
	private final String USER_AGENT = "Mozilla/5.0";
	private ServletContext context = null;
	private String queryName = "";

	public Nrg4castAlarmListener(ServletContext context, String queryName) {
		super();
		this.context = context;
		this.queryName = queryName;
	}

	@Override
	public void update(EventBean[] inEvents, EventBean[] outEvents) {
		HashMap<String,AlertHeader> headers = (HashMap<String,AlertHeader>) context.getAttribute("eventHeaders");
		AlertHeader head = headers.get(queryName);
		for(int i=0;i<inEvents.length;i++){
			Alert alert = new Alert();
					
			EventBean eb = inEvents[i];
			EventType et = eb.getEventType();
			String[] propNames = et.getPropertyNames();
			alert.setTimestamp(new Date(System.currentTimeMillis()));
			PatternParameter p = new PatternParameter();
			boolean addParam = false;
			for (int j=0;j<propNames.length;j++){				
				//	obj.put(propNames[j], eb.get(propNames[j]));	

				String propName = propNames[j];
				if(propName.equals("latitude")){
					alert.setLatitude(Double.parseDouble(eb.get(propNames[j]).toString()));
				} else if(propName.equals("longitude")){
					alert.setLongitude(Double.parseDouble(eb.get(propNames[j]).toString()));
				} else if(propName.equals("subjectId")){
					alert.setLocation(eb.get(propNames[j]).toString());
				} else if(propName.equals("phenomenon")){
					p.setPhenomenon(eb.get(propNames[j]).toString());
					addParam=true;
				} 
				else if(propName.equals("value")){
					p.setValue(eb.get(propNames[j]).toString());
					addParam=true;
				} 
				else if(propName.equals("uom")){
					p.setUom(eb.get(propNames[j]).toString());
				} else if(propName.equals("sensorId")){
					alert.setSensorId(eb.get(propNames[j]).toString());
				}
				

			}
			if(addParam){
				alert.addParameter(p);
			}
			alert.buildMessage();
			log.info("Alert sent: " + alert.toJsonString());
			try {
				sendToAlertBus("event="+alert.toJsonString());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


		}

	}

	private void sendToAlertBus(String url) {
		try {
			URI myURI = null;
			try {
				myURI = new URI("http", "demo3.nrg4cast.org:8081","/",url,null);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			URL myURL = myURI.toURL();	    
			HttpURLConnection con = (HttpURLConnection) myURL.openConnection();

			// optional default is GET
			con.setRequestMethod("GET");

			//add request header
			con.setRequestProperty("User-Agent", USER_AGENT);
			int responseCode = con.getResponseCode();
			//   con.connect();
		} 
		catch (MalformedURLException e) { 
			System.out.println("malformed URL: " + url);
		} 
		catch (IOException e) {   
			System.out.println(e.getMessage());

		}

	}

}
