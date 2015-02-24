package eventTypes.nrg4cast;

import java.util.ArrayList;
import java.util.Date;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Alert {


	private String location;
	private double latitude;
	private double longitude;
	private String message;
	private Date timestamp;
	
	private String name;
	private String pilotId;
	private String type;
	private String level;	
	private String timeWindow;
//	private AlertHeader header;
	private ArrayList<PatternParameter> parameters;
	private boolean isLatLngSet;
	
	private String sensorId;


	public Alert(String location, double latitude, double longitude,
			String message, Date timestamp, String name, String pilotId,
			String type, String level, String timeWindow,
			ArrayList<PatternParameter> parameters, boolean isLatLngSet,
			String sensorId) {
		super();
		this.location = location;
		this.latitude = latitude;
		this.longitude = longitude;
		this.message = message;
		this.timestamp = timestamp;
		this.name = name;
		this.pilotId = pilotId;
		this.type = type;
		this.level = level;
		this.timeWindow = timeWindow;
		this.parameters = parameters;
		this.isLatLngSet = isLatLngSet;
		this.sensorId = sensorId;
	}

	public void addParameter(PatternParameter p){
		parameters.add(p);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPilotId() {
		return pilotId;
	}

	public void setPilotId(String pilotId) {
		this.pilotId = pilotId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getLevel() {
		return level;
	}

	public void setLevel(String level) {
		this.level = level;
	}

	public String getTimeWindow() {
		return timeWindow;
	}

	public void setTimeWindow(String timeWindow) {
		this.timeWindow = timeWindow;
	}

	public double getLatitude() {
		return latitude;
	}
	public void setLatitude(double latitude) {
		this.latitude = latitude;
		this.isLatLngSet = true;
	}
	public double getLongitude() {
		return longitude;

	}
	public void setLongitude(double longitude) {
		this.longitude = longitude;
		this.isLatLngSet = true;
	}


	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}



	public String getSensorId() {
		return sensorId;
	}

	public void setSensorId(String sensorId) {
		this.sensorId = sensorId;
	}

	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public Alert() {
		super();
		this.parameters = new ArrayList<PatternParameter>();
		// TODO Auto-generated constructor stub
	}
	
	

	public String toJsonString(){

		JSONObject jsonObj = new JSONObject();
		jsonObj.put("Name", this.getName());
		jsonObj.put("Type", this.getType());
		jsonObj.put("Level", this.getLevel());
		jsonObj.put("Timestamp", this.timestamp.getTime()/1000L);
		jsonObj.put("Msg", this.message);
		jsonObj.put("PilotName", this.getPilotId());
		jsonObj.put("SensorId", this.sensorId);

		if(this.getLocation()!=null){
			jsonObj.put("Location", this.location);
		}
		if(isLatLngSet){
			jsonObj.put("Latitude", this.latitude);
			jsonObj.put("Longitude", this.longitude);
		}
		if(this.getTimeWindow()!=null){
			jsonObj.put("Timewindow", this.getTimeWindow());
		}
		JSONArray paramArray = new JSONArray();
		for(int i = 0; i< parameters.size(); i++){
			PatternParameter p =parameters.get(i);
			JSONObject jsonParam = new JSONObject();
			jsonParam.put("Value", p.getValue());
			jsonParam.put("Relation", p.getRelation());
			jsonParam.put("Threshold", p.getThreshold());
			jsonParam.put("Phenomenon", p.getPhenomenon());
			jsonParam.put("Uom", p.getUom());
			paramArray.add(jsonParam);
		}
		if(paramArray.size()>0){
			jsonObj.put("Parameters", paramArray);
		}


		return jsonObj.toJSONString();
	}



}
