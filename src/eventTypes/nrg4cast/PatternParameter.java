package eventTypes.nrg4cast;

public class PatternParameter {
	
	
	private String value;
	private String threshold;
	private String relation; //less , equal or higher between value and threshold
	private String uom;
	private String phenomenon;
	


	public PatternParameter(String value, String threshold, String relation) {
		super();
		this.value = value;
		this.threshold = threshold;
		this.relation = relation;
	}
	
	public PatternParameter() {
		super();
		
	}
	
	


	public PatternParameter(String value, String threshold, String relation,
			String uom, String phenomenon) {
		super();
		this.value = value;
		this.threshold = threshold;
		this.relation = relation;
		this.uom = uom;
		this.phenomenon = phenomenon;
	}


	public String getPhenomenon() {
		return phenomenon;
	}


	public void setPhenomenon(String phenomenon) {
		this.phenomenon = phenomenon;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public String getThreshold() {
		return threshold;
	}
	public void setThreshold(String threshold) {
		this.threshold = threshold;
	}
	public String getRelation() {
		return relation;
	}
	public void setRelation(String relation) {
		this.relation = relation;
	}
	public String getUom() {
		return uom;
	}
	public void setUom(String uom) {
		this.uom = uom;
	}
	
	


}
