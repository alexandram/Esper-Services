package eventTypes.nrg4cast;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Measurement implements Comparable<Measurement> {
	private String nodeId;
	private String nodeName;
	private String subjectId;
	private double lat;
	private double lng;
	private String sensorId;
	private double value;
	private String timestampStr;
	private Date timestamp;
	private String typeId;
	private String sensorType;
	private String phenomenon;
	private String uom;
	public Measurement(String nodeId, String nodeName, String subjectId,
			double lat, double lng, String sensorId, double value,
			Date timestamp, String typeId, String sensorType,
			String phenomenon, String uom) {
		this.nodeId = nodeId;
		this.nodeName = nodeName;
		this.subjectId = subjectId;
		this.lat = lat;
		this.lng = lng;
		this.sensorId = sensorId;
		this.value = value;
		this.timestamp = timestamp;
		this.typeId = typeId;
		this.sensorType = sensorType;
		this.phenomenon = phenomenon;
		this.uom = uom;
	}

	public Measurement() {
	}


	
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public Measurement(String nodeId) {
		this.nodeId = nodeId;
	}

	public String getNodeId() {
		return nodeId;
	}
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}
	public String getNodeName() {
		return nodeName;
	}
	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	public String getSubjectId() {
		return subjectId;
	}
	public void setSubjectId(String subjectId) {
		this.subjectId = subjectId;
	}
	public double getLat() {
		return lat;
	}
	public void setLat(double lat) {
		this.lat = lat;
	}
	public double getLng() {
		return lng;
	}
	public void setLng(double lng) {
		this.lng = lng;
	}
	public String getSensorId() {
		return sensorId;
	}
	public void setSensorId(String sensorId) {
		this.sensorId = sensorId;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	public Date getTimestamp() {
		return timestamp;
	}


	public String getTimestampStr() {
		return timestampStr;
	}
	public void setTimestampStr(String timestampStr) {
		this.timestampStr = timestampStr;
		if(timestampStr!=null){
			this.parseTimestamp();
		}
	}

	public void parseTimestamp() {
		//"2013-02-02T10:11:12.984",

		String timestamp = this.getTimestampStr().replace("T", "-");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS", 
				Locale.ENGLISH);
		Date result;

		try {
			result = df.parse(timestamp);
			this.timestamp = result;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  		

	}
	public String getTypeId() {
		return typeId;
	}
	public void setTypeId(String typeId) {
		this.typeId = typeId;
	}
	public String getSensorType() {
		return sensorType;
	}
	public void setSensorType(String sensorType) {
		this.sensorType = sensorType;
	}
	public String getPhenomenon() {
		return phenomenon;
	}
	public void setPhenomenon(String phenomenon) {
		this.phenomenon = phenomenon;
	}
	public String getUom() {
		return uom;
	}
	public void setUom(String uom) {
		this.uom = uom;
	}

	public String toString(){
		return nodeId + " " + phenomenon + " " + value + 
				" " + uom + " " +timestamp.toString();
	}

	@Override
	public int compareTo(Measurement m) {
		if(this.timestamp.after(m.timestamp)){
			return 1;
		} else if (this.timestamp.before(m.timestamp)){
			return -1;
		}
		return 0;
	}


}
