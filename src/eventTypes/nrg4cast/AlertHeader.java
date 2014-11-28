package eventTypes.nrg4cast;

import java.util.ArrayList;

public class AlertHeader {
	
	private String name;
	private String pilotId;
	private String type;
	private String level;
	
	private String timeWindow;

	
	
	
	
	public AlertHeader(String name, String pilotId, String type, String level) {
		super();
		this.name = name;
		this.pilotId = pilotId;
		this.type = type;
		this.level = level;
	}
	
	public AlertHeader(String name, String pilotId, String type, String level, String timeWindow) {
		super();
		this.name = name;
		this.pilotId = pilotId;
		this.type = type;
		this.level = level;
		this.timeWindow = timeWindow;
	}
	
	public String getTimeWindow() {
		return timeWindow;
	}

	public void setTimeWindow(String timeWindow) {
		this.timeWindow = timeWindow;
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

	@Override
	public String toString() {
		return "EventHeader [name=" + name + ", pilotName=" + pilotId + ", type="
				+ type + ", level=" + level + ", timeWindow=" + timeWindow
				+ "]";
	}
	

}
