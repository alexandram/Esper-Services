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



import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.servlet.ServletContext;





import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import eventTypes.nrg4cast.Prediction;




@Path("/")
public class QMinerJSONInputService {

	static final Logger log = Logger.getLogger(QMinerJSONInputService.class);
	static String esperService = "http://localhost:9082/sendData2Esper?";
	private final String USER_AGENT = "Mozilla/5.0";
	@Context
	private ServletContext context;


	@POST
	@Path("/QMinerJSONInputService")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getQMinerJSONData(InputStream incomingData) {
		StringBuilder jsonBuilder = new StringBuilder();
		try {
			BufferedReader in = new BufferedReader(
					new InputStreamReader(incomingData));
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
	@SuppressWarnings("unchecked")
	@GET
	@Path("/Prediction")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getPredictionBuffer() {

		ArrayList<Prediction> predBuffer = (ArrayList<Prediction>) context.getAttribute("predBuffer");

		//predBuffer.add(new Prediction(0,0,0,0,0,0,"id","model","2015-07-01T13:11:12.984"));
		//	predBuffer.add(new Prediction(1,1,1,1,1,1,"id1","model1","2015-07-01T14:11:12.984"));
		//	predBuffer.add(new Prediction(2,2,2,2,2,2,"id2","model2","2015-07-01T15:11:12.984"));
		JSONArray json = new JSONArray();
		for(Prediction p: predBuffer){
			json.add(p.toJson());
		}

		return Response.ok(json.toJSONString(), MediaType.APPLICATION_JSON).build();
	}

	@GET
	@Path("/DeletePrediction")
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteFromPredictionBuffer(@QueryParam("timestamp") long timestamp) {
		// remove from predBuffer all records before timestamp
		ArrayList<Prediction> predBuffer = (ArrayList<Prediction>) context.getAttribute("predBuffer");
		Iterator<Prediction> it = predBuffer.iterator();
		Date d = new Date(timestamp);
		System.out.println(d.toString());
		JSONArray json = new JSONArray();
		while(it.hasNext()){
			Prediction pred = it.next();
			System.out.println("prediction: " + pred.getTimestamp().toString());
			if(pred.getTimestamp().before(d)){
				System.out.println("before");
				it.remove();
				json.add(pred.toJson());
			} else {
				System.out.println("after");
			}
		}		
		return Response.ok(json.toJSONString(), MediaType.APPLICATION_JSON).build();
	}

	@POST
	@Path("/QMinerPredictionInputService")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response getQMinerPredictionData(InputStream incomingData) {
		StringBuilder jsonBuilder = new StringBuilder();
		try {
			BufferedReader in = new BufferedReader(
					new InputStreamReader(incomingData));
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
			callEsperService4Predictions(jsonBuilder.toString());
			return Response.status(200).entity(jsonBuilder.toString()).build();			
		} else{
			return Response.status(400).entity(jsonBuilder.toString()).build();
		}
	}

	private void callEsperService4Predictions(String string) {
		//	System.out.println("predictions received: - " + string);
		
		ArrayList<Prediction> predBuffer = (ArrayList<Prediction>) context.getAttribute("predBuffer");
		JSONArray array= (JSONArray)JSONValue.parse(string);
		for(int i=0;i<array.size();i++){
			JSONObject obj= (JSONObject)array.get(i);
			double me = Double.parseDouble(obj.get("me").toString());
			double mae = Double.parseDouble(obj.get("mae").toString());
			double mse = Double.parseDouble(obj.get("mse").toString());
			double rmse = Double.parseDouble(obj.get("rmse").toString());
			double rsquared = Double.parseDouble(obj.get("rsquared").toString());
			double value = Double.parseDouble(obj.get("value").toString());
			String sensorId = obj.get("sensorId").toString();
			String modelId = obj.get("modelId").toString();
			String timestamp = obj.get("timestamp").toString();
			/*String parameters = "stream=Prediction&"+ 
					"&me=" + me+
					"&mae=" + mae +
					"&mse=" + mse + 
					"&rmse=" + rmse +
					"&rsquared=" + rsquared + 
					"&value=" + value +
					"&sensorId=" + sensorId + 
					"&modelId=" + modelId +
					"&timestampStr=" + timestamp;*/

			
			//callURL(parameters);
			predBuffer.add(new Prediction(me, mae, mse, rmse, rsquared, value,
					sensorId, modelId, timestamp));
		}




	}



	private void callEsperService(String string) {

		//	System.out.println(string);
		JSONArray array= (JSONArray)JSONValue.parse(string);
		for(int i=0;i<array.size();i++){
			JSONObject obj= (JSONObject)array.get(i);
			JSONObject node = (JSONObject) obj.get("node");
			String nodeId = node.get("id").toString();
			String nodeName = node.get("name").toString();
			String subjectid ="";
			if(node.containsKey("subjectid")){
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
				log.debug(parameters);
				callURL(parameters);
			}



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
}
