package servlets;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;























import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import esperCore.EsperEngine;
import esperCore.EsperInstance;
import eventTypes.nrg4cast.AlertHeader;
import eventTypes.nrg4cast.PatternParameter;

/**
 * Servlet implementation class EPLQueryParser
 */
@WebServlet("/RegisterEventPattern")
public class EPLQueryParser extends HttpServlet {
	private static final long serialVersionUID = 1L;
	static final Logger log = Logger.getLogger(EPLQueryParser.class);
	private static final ArrayList<String> quotedFields = new ArrayList<String>( 
			Arrays.asList("nodeId","subjectId","sensorId","typeId",
					"sensorType", "phenomenon", "uom"));

	static String nrg4castEngineName = "nrg4cast"; //should be a parameter send by client   
	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public EPLQueryParser() {
		super();


	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/plain");		
		PrintWriter out = response.getWriter();

		//get the engine name
		ServletContext context = request.getSession().getServletContext();
		Map<String, EsperInstance> contextAttr = (Map<String, EsperInstance>) context.getAttribute("instances");
		EsperInstance esper = contextAttr.get(nrg4castEngineName);
		
		if(request.getParameter("rule")!=null){
			String ruleString = request.getParameter("rule"); 
			String callString = addQueryToEsper(esper, ruleString,context);
			log.info(callString);
			out.write(callString);
		}else {
			response.setStatus(400);
			out.println("Unsupported content type");
		}
		out.close();
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		response.setContentType("text/plain");		
		PrintWriter out = response.getWriter();

		//get the epl query from the post content as json encoded data or epl directly
		StringBuilder msgBuilder = new StringBuilder();
		try {
			BufferedReader in = request.getReader();
			String line = null;
			while ((line = in.readLine()) != null) {
				msgBuilder.append(line);
			}
		} catch (Exception e) {
			System.out.println("Error Parsing: - " + e.getMessage());
		}

		//get the engine name
		ServletContext context = request.getSession().getServletContext();
		Map<String, EsperInstance> contextAttr = (Map<String, EsperInstance>) context.getAttribute("instances");
		EsperInstance esper = contextAttr.get(nrg4castEngineName);

		if(request.getContentType().equals("application/json")){
			//parse json message
			// return HTTP response 200 in case of success and 400 in case of wrong format
			if(JSONValue.parse(msgBuilder.toString())!=null){			
				response.setStatus(200);
			} else{
				response.setStatus(400);
			}
			//add query to engine			
			String callString = addQueryToEsper(esper, msgBuilder.toString(),context);
			log.info(callString);
			out.println(callString);
		}  else {
			response.setStatus(400);
			out.println("Unsupported content type");
		}




		out.close();
	}

	private String addQueryToEsper(EsperInstance esper, String jsonString, ServletContext context) {

		String queryString = "";
		String name = "";
		try{
			JSONObject queryObj = (JSONObject) JSONValue.parse(jsonString);
			if(!isValidQueryString(jsonString)){
				return "invalid query, you must specify name, pilotName, type, and level parameters";
			} else{
				name = queryObj.get("name").toString();
				if(!esper.isValidQueryName(name)){
					return "invalid query name " + name + " .A query with this name is already registered.";
				}
				String pilotName = queryObj.get("pilotName").toString();
				String type = queryObj.get("type").toString();
				String level = queryObj.get("level").toString();
				String message = queryObj.get("message").toString();

				if (queryObj.containsKey("epl")){
					queryString = queryObj.get("epl").toString();
					String [] qss = queryString.split("\\*");
					if(qss.length!=2){
						return "unkown error, select statement seems to be incomplete";
					} else {
						queryString = qss[0] + "*,'" + 
								pilotName + "' as pilotName, '" + 
								type + "' as type, '" + 
								level + "' as level, '" +
								message +"' as message, '" +
								name + "' as name " +qss[1];
					}
					
				} 
			}
		}catch(Exception e){
			e.printStackTrace();
			return "Exception: " + e.getMessage();
		}
	//	System.out.println(queryString);
		return esper.addQuery(queryString,name) + "   " + queryString;

	}



	private boolean validTimeWindow(String string) {

		return true;
	}

	private String parseParameters(JSONObject parametersObj) {
		if(parametersObj.containsKey("or") || parametersObj.containsKey("and")){
			return parseMultipleParameters(parametersObj);
		}else{
			return parseParameter(parametersObj);			
		}		
	}

	private String parseMultipleParameters(JSONObject parametersObj) {
		String parameters = "(";
		String operator = "";
		if(parametersObj.containsKey("or")){
			operator = "or";
		} else if(parametersObj.containsKey("and")){
			operator = "and";
		}
		JSONArray paramArr = (JSONArray) parametersObj.get(operator);
		if (paramArr.size()<2){
			return "";
		} else {
			for(int i=0; i<paramArr.size(); i++){
				if(i>0){
					parameters += " " + operator + " ";
				}
				parameters += parseParameters((JSONObject) paramArr.get(i));

			}
		}

		return parameters+")";
	}

	private String parseParameter(JSONObject parameterObj) {
		String parameter = "";
		if(isValidParameter(parameterObj)){
			String field = parameterObj.get("field").toString();
			String relation = parameterObj.get("relation").toString();
			String threshold = parameterObj.get("threshold").toString();
			parameter =  mapField(field) + relation + properlyQuoted( mapField(field),threshold);			
		}else{
			return "invalid parameters";
		}
		return parameter;
	}

	private String mapField(String field) {

		HashMap<String, String> myMap = new HashMap<String,String>();
		myMap.put("id", "nodeId");
		myMap.put("name", "nodeName");
		myMap.put("subjectid", "subjectId");
		myMap.put("lat", "lat");
		myMap.put("lng", "lng");
		myMap.put("sensorid", "sensorId");
		myMap.put("timestamp", "timestampStr");
		myMap.put("type", "sensorType");
		myMap.put("phenomenon", "phenomenon");
		myMap.put("UoM", "uom");
		myMap.put("value", "value");

		return myMap.get(field);
	}

	private String properlyQuoted(String field, String threshold) {
		if(quotedFields.contains(field)){
			return "\"" + threshold + "\"";
		} else {
			return threshold;
		}

	}

	private boolean isValidParameter(JSONObject filterObj) {
		if(filterObj.containsKey("field") && filterObj.containsKey("relation") && filterObj.containsKey("threshold")){
			if(isValidRelation(filterObj.get("relation").toString())){
				return true;
			} else {
				return false;
			}
		}
		return false;
	}

	private boolean isValidRelation(String relation) {
		if (relation.equals("=") || relation.equals("!=") || relation.equals(">")
				|| relation.equals(">=") || relation.equals("<") || relation.equals("<=")){
			return true;
		}
		return false;
	}

	private boolean isValidQueryString(String jsonString) {
		try{
			JSONObject queryObj = (JSONObject) JSONValue.parse(jsonString);
			if(!queryObj.containsKey("name") || !queryObj.containsKey("pilotName") || !queryObj.containsKey("type") || !queryObj.containsKey("level")){
				return false;
			}else {
				if(queryObj.get("name").toString().length()<1 ||
						queryObj.get("pilotName").toString().length()<1 ||
						queryObj.get("type").toString().length()<1 ||
						queryObj.get("level").toString().length()<1){
					return false;
				}
			}
		}catch(Exception e){
			return false;
		}
		return true;
	}

}
