package inputAdapters.nrg4cast.inputEvents;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;




@Path("/")
public class QMinerJSONInputService {

	static final Logger log = Logger.getLogger(QMinerJSONInputService.class);
	static String esperService = "http://localhost:9079/sendData2Esper?stream=Measurement&";
	private final String USER_AGENT = "Mozilla/5.0";
	@POST
	@Path("/QMinerJSONInputService")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getQMinerJSONData(InputStream incomingData) {
		StringBuilder jsonBuilder = new StringBuilder();
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(incomingData));
			String line = null;
			while ((line = in.readLine()) != null) {
				jsonBuilder.append(line);
			}
		} catch (Exception e) {
			System.out.println("Error Parsing: - ");
		}
		//System.out.println("Data Received: " + jsonBuilder.toString());

		// return HTTP response 200 in case of success
		if(JSONValue.parse(jsonBuilder.toString())!=null){
			//log.info(jsonBuilder.toString() + "\n");
			callEsperService(jsonBuilder.toString());
			return Response.status(200).entity(jsonBuilder.toString()).build();			
		} else{
			return Response.status(400).entity(jsonBuilder.toString()).build();
		}
	}

	private void callEsperService(String string) {
		JSONArray array= (JSONArray)JSONValue.parse(string);
		for(int i=0;i<array.size();i++){
			JSONObject obj= (JSONObject)array.get(i);
			JSONObject node = (JSONObject) obj.get("node");
			String nodeId = node.get("id").toString();
			String nodeName = node.get("name").toString();
			String subjectid ="";
			if(node.containsKey("subjectId")){
				subjectid = node.get("subjectid").toString();
			}
			
			String lat = node.get("lat").toString();
			String lng = node.get("lng").toString();
			JSONArray measurements = (JSONArray) node.get("measurements");
			for(int j=0;j<measurements.size();j++){
				JSONObject msm = (JSONObject) measurements.get(j);
				String sensorId = msm.get("sensorid").toString();
				String value = msm.get("value").toString();

				String timestampStr = msm.get("timestamp").toString();
				JSONObject type = (JSONObject) msm.get("type");
				String typeId = type.get("id").toString();
				String typeName = type.get("name").toString();
				String phenomenon = type.get("phenomenon").toString();
				String uom = type.get("UoM").toString();

				String parameters = "stream=Measurement&"+ 
						"&nodeId=" + nodeId +
						"&value=" + value +
						"&nodeName=" + nodeName + 
						"&lat=" + lat +
						"&lng=" + lng + 
						"&phenomenon=" + phenomenon +
						"&sensorId=" + sensorId + 
						"&sensorType=" + typeName +
						"&subjectId=" + subjectid +
						"&timestampStr=" + timestampStr+
						"&typeId=" + typeId + 
						"&uom=" + uom;
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  
				callURL(parameters);
			}



		}


	}



	private void callURL(String url) {
		try {
		    URI myURI = null;
			try {
				myURI = new URI("http", "localhost:9079","/sendData2Esper",url,null);
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
}
