package inputAdapters.nrg4cast.inputEvents;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Date;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class PredictionDelay implements Runnable{

	private volatile boolean running = true;
	static final Logger log = Logger.getLogger(QMinerJSONInputService.class);
	private static int SLEEP_INTERVAL = 5;
	private static String getPredictionsURL = 
			"http://mustang.ijs.si:9080/Esper-Services/api/Prediction";
	private static String deletePredictionsURL = 
			"http://mustang.ijs.si:9080/Esper-Services/api/DeletePrediction?timestamp=";
	private final String USER_AGENT = "Mozilla/5.0";
	@Override
	public void run() {
		try {
			while(running){
				//get current time
				long time  = System.currentTimeMillis();
				//get prediction buffer from JSON INPUT Service

				URL obj = new URL(getPredictionsURL);
				HttpURLConnection con = (HttpURLConnection) obj.openConnection();

				// optional default is GET
				con.setRequestMethod("GET");

				//add request header
				con.setRequestProperty("User-Agent", USER_AGENT);		 
				int responseCode = con.getResponseCode();
				if(responseCode == 200) { //OK


					BufferedReader in = new BufferedReader(
							new InputStreamReader(con.getInputStream()));
					String inputLine;
					StringBuffer response = new StringBuffer();

					while ((inputLine = in.readLine()) != null) {
						response.append(inputLine);
					}
					in.close();
					//parse result 
					JSONArray array= (JSONArray)JSONValue.parse(response.toString());
					for(int i=0;i<array.size();i++){
						JSONObject jobj= (JSONObject)array.get(i);
						String me = jobj.get("me").toString();
						String mae = jobj.get("mae").toString();
						String mse = jobj.get("mse").toString();
						String rmse = jobj.get("rmse").toString();
						String rsquared = jobj.get("rsquared").toString();
						String value = jobj.get("value").toString();
						String sensorId = jobj.get("sensorId").toString();
						String modelId = jobj.get("modelId").toString();
						String timestampStr = jobj.get("timestampStr").toString();
						long timestamp = Long.parseLong(jobj.get("timestamp").toString());
						String parameters = "stream=Prediction&"+ 
								"&me=" + me+
								"&mae=" + mae +
								"&mse=" + mse + 
								"&rmse=" + rmse +
								"&rsquared=" + rsquared + 
								"&value=" + value +
								"&sensorId=" + sensorId + 
								"&modelId=" + modelId +
								"&timestampStr=" + timestampStr;

						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} 
						Date d = new Date(timestamp);
						//send predictions to CEP that are in current time window.
						//every 5 minutes
						if(d.before(new Date(time))){
							callURL(parameters);
						}
					}	
				}

				//delete the predictions that have been sent form buffer
				obj = new URL(deletePredictionsURL+time);
				con = (HttpURLConnection) obj.openConnection();
				con.setRequestMethod("GET");
				con.setRequestProperty("User-Agent", USER_AGENT);		 
				responseCode = con.getResponseCode();
				if(responseCode == 200) { //OK
					System.out.println("deleted");
				}
				//sleep for 5 minutes

				TimeUnit.MINUTES.sleep(SLEEP_INTERVAL);

			}
		} catch (InterruptedException  e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}

	}

	private void callURL(String url) {
		try {
			URI myURI = null;
			try {
				myURI = new URI("http", "localhost:9082","/sendData2Esper",
						url,null);
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
			log.error("malformed URL: " + url);
		} 
		catch (IOException e) {   
			log.error(e.getMessage());
		}

	}

	public void terminate (){
		running = false;
	}

}
