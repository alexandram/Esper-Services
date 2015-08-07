/**
 * 
 */
package eventTypes.nrg4cast;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

/**
 * @author alexandram
 *
 */
public class WeatherMeasurement extends Measurement {
	
	private int predictionHour;

	public WeatherMeasurement() {
		super();
		// TODO Auto-generated constructor stub
	}

	public WeatherMeasurement(String nodeId, String nodeName, String subjectId,
			double lat, double lng, String sensorId, double value,
			Date timestamp, String typeId, String sensorType,
			String phenomenon, String uom, int predictionHour) {
		super(nodeId, nodeName, subjectId, lat, lng, sensorId, value, timestamp,
				typeId, sensorType, phenomenon, uom);
		this.predictionHour = predictionHour;
	}

	public WeatherMeasurement(String nodeId) {
		super(nodeId);
		// TODO Auto-generated constructor stub
	}

	public int getPredictionHour() {
		return predictionHour;
	}
	

	public void setPredictionHour(int predictionHour) {
		this.predictionHour = predictionHour;
	}
	
	@Override
	public void parseTimestamp() {
		//"2013-02-02T10:11:12.984",

		String timestamp = this.getTimestampStr().replace("T", "-");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss.SSS", 
				Locale.ENGLISH);
		Date result;

		try {
			result = df.parse(timestamp);
			super.setTimestamp(result);
			Calendar c = Calendar.getInstance();
			c.setTime(result);
			this.predictionHour = c.HOUR_OF_DAY;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  		

	}
	
	
}
