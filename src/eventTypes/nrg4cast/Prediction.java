package eventTypes.nrg4cast;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.json.simple.JSONObject;

public class Prediction {
	private double me;
	private double mae;
	private double mse;
	private double rmse;
	private double rsquared;
	private double value;
	private String sensorId;
	private String modelId;
	private Date timestamp;	
	private String timestampStr;
	
	public Prediction() {
		
	}
	
	
	public Prediction(double me, double mae, double mse, double rmse,
			double rsquared, double value, String sensorId, String modelId,
			String timestampStr) {
		super();
		this.me = me;
		this.mae = mae;
		this.mse = mse;
		this.rmse = rmse;
		this.rsquared = rsquared;
		this.value = value;
		this.sensorId = sensorId;
		this.modelId = modelId;
		this.timestampStr = timestampStr;
		this.parseTimestamp();
	}


	public double getMe() {
		return me;
	}
	public void setMe(double me) {
		this.me = me;
	}
	public double getMae() {
		return mae;
	}
	public void setMae(double mae) {
		this.mae = mae;
	}
	public double getMse() {
		return mse;
	}
	public void setMse(double mse) {
		this.mse = mse;
	}
	public double getRmse() {
		return rmse;
	}
	public void setRmse(double rmse) {
		this.rmse = rmse;
	}
	public double getRsquared() {
		return rsquared;
	}
	public void setRsquared(double rsquared) {
		this.rsquared = rsquared;
	}
	public double getValue() {
		return value;
	}
	public void setValue(double value) {
		this.value = value;
	}
	public String getSensorId() {
		return sensorId;
	}
	public void setSensorId(String sensorId) {
		this.sensorId = sensorId;
	}
	public String getModelId() {
		return modelId;
	}
	public void setModelId(String modelId) {
		this.modelId = modelId;
	}
	public Date getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
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
	
	@SuppressWarnings("unchecked")
	public JSONObject toJson(){
		JSONObject obj = new JSONObject();
		obj.put("me", me);
		obj.put("mae", mae);
		obj.put("mse", mse);
		obj.put("rmse", rmse);
		obj.put("rsquared", rsquared);
		obj.put("value", value);
		obj.put("sensorId", sensorId);
		obj.put("modelId", modelId);
		obj.put("timestamp", timestamp.getTime());
		obj.put("timestampStr", timestampStr);
	
		return obj;
	}
	
	

}
