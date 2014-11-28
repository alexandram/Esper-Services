package esperCore;

import java.util.HashMap;

import com.espertech.esper.client.Configuration;

public interface EsperInstance {

	public void shutdown();

	public void addEventTypes(Configuration config);

	public void addListeners();
	
	public String addQuery(String query, String queryName);
	
	public String deleteQuery(String queryName);
	
	public HashMap<String, String> getRegisteredQueries();
	
	public boolean isValidQueryName(String name);
	
	
}
