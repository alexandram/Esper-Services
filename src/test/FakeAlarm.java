package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;

import eventTypes.nrg4cast.Alert;

public class FakeAlarm {
	private static String USER_AGENT = "Mozilla/5.0";
	
	public static void main(String args[]){
		Alert alerts[] = new Alert[10];
		Alert a = new Alert();
		a.setLocation("turin-building-CSI_BUILDING");				
		a.setLatitude(45.038536);
		a.setLongitude(7.651824);
		a.setMessage("Too fast use of energy by building (15 min consumption > 260 Kw)");
		a.setType("energy");
		a.setLevel("warning");
		a.setPilotId("turin");
		a.setTimeWindow("15 min");
		a.setName("High energy use");
		a.setTimestamp(new Date(System.currentTimeMillis() - 1000*60*60*24-1000*60*60));
		a.setSensorId("turin-building_part-CSI_BUILDING_Office113-computersOffice113");		
		System.out.println(a.toJsonString());
		sendToAlertBus(a.toJsonString());
		
		a.setTimestamp(new Date(System.currentTimeMillis() - 1000*60*60));
		System.out.println(a.toJsonString());
		sendToAlertBus(a.toJsonString());
		
		a.setMessage("Very high level of energy use for office computers (hourly > 2,7 kW)");
		a.setType("energy");
		a.setLevel("alarm");
		a.setTimeWindow("1 hour");
		a.setName("Very high energy use");
		a.setTimestamp(new Date(System.currentTimeMillis() - 1000*60*60*24));
		a.setSensorId("turin-building_part-CSI_BUILDING_Office113-computersOffice113");
		System.out.println(a.toJsonString());
		sendToAlertBus(a.toJsonString());
		
		a.setTimestamp(new Date(System.currentTimeMillis()));
		System.out.println(a.toJsonString());
		sendToAlertBus(a.toJsonString());
		
		a.setPilotId("ntua");
		a.setLocation("ntua-building-HYDROLICS");	
		a.setLatitude(37.978164);
		a.setLongitude(23.775518);
		a.setMessage("No sensor data received for 1 day");
		a.setType("technical");
		a.setLevel("notice");
		a.setTimeWindow("1 day");
		a.setName("Missing data");
		a.setTimestamp(new Date(System.currentTimeMillis()));
		a.setSensorId("ntua-building-HYDROLICS-current_l1");
		System.out.println(a.toJsonString());
		sendToAlertBus(a.toJsonString());
		
		a.setTimestamp(new Date(System.currentTimeMillis()- 1000*60*60*24));
		System.out.println(a.toJsonString());
		sendToAlertBus(a.toJsonString());
		
		a.setPilotId("ntua");
		a.setLocation("ntua-building-HYDROLICS");	
		a.setLatitude(37.978164);
		a.setLongitude(23.775518);
		a.setMessage("Very high electricity consumption of a building");
		a.setType("energy");
		a.setLevel("notice");
		a.setTimeWindow("1 day");
		a.setName("Very high electricity consumption");
		a.setTimestamp(new Date(System.currentTimeMillis()-3*24*60*60*1000));
		a.setSensorId("ntua-building-HYDROLICS-energy_a");
		System.out.println(a.toJsonString());
		sendToAlertBus(a.toJsonString());
	}
	
	private static void sendToAlertBus(String url) {
		try {
			URI myURI = null;
			try {
				myURI = new URI("http", "demo3.nrg4cast.org:8088","/","event="+url,null);
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
			System.out.println("\nSending 'GET' request to URL : " + myURL);
			System.out.println("Response Code : " + responseCode);
			//   con.connect();
			
			
			//get response
			BufferedReader in = new BufferedReader(
			        new InputStreamReader(con.getInputStream()));
			String inputLine;
			StringBuffer response = new StringBuffer();
	 
			while ((inputLine = in.readLine()) != null) {
				response.append(inputLine);
			}
			in.close();
	 
			//print result
			System.out.println(response.toString());
	 
		} 
		catch (MalformedURLException e) { 
			System.out.println("malformed URL: " + url);
		} 
		catch (IOException e) {   
			System.out.println(e.getMessage());

		}

	}

	
}
