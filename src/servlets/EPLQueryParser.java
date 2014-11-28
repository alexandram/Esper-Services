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
				String pilotId = queryObj.get("pilotName").toString();
				String type = queryObj.get("type").toString();
				String level = queryObj.get("level").toString();

				if (queryObj.containsKey("epl")){
					queryString = queryObj.get("epl").toString();
					//extract window or interval
					String interval = null;
					if(queryString.contains("interval")){
						String str1=queryString.split("interval")[1];
					//	System.out.println(str1);
						interval =str1.substring(1,str1.indexOf(")"));
					}
					//add event header to context
					Map<String, AlertHeader> eventHeaders= (Map<String, AlertHeader>) context.getAttribute("eventHeaders");
					AlertHeader eh = new AlertHeader(name, pilotId, type, level,interval);
					eventHeaders.put(name, eh);

				} else {
					//parse select
					String selectStr = "";
					if(queryObj.containsKey("select")){
						JSONArray selectArr = (JSONArray) queryObj.get("select");
						if(selectArr.size()>0){
							for(int i=0;i<selectArr.size();i++){
								selectStr += selectArr.get(i).toString();
								if(i<selectArr.size()-1){
									selectStr += ",";
								}
							}
						} else {
							return "invalid select";
						}
					} else {
						selectStr = "*";
					}

					//parse streams

					String streamStr = "";				
					if(queryObj.containsKey("stream")){
						JSONArray streamArr = (JSONArray) queryObj.get("stream");
						if(streamArr.size()>0){
							for(int i=0;i<streamArr.size();i++){
								selectStr += streamArr.get(i).toString();
								if(i<streamArr.size()-1){
									streamStr += ",";
								}
							}
						} else {
							return "invalid stream";
						}
					} else {
						streamStr = "Measurement";
					}

					//parse window
					String timeInterval = null;
					if(queryObj.containsKey("window")){
						if(validTimeWindow(queryObj.get("window").toString())){
							timeInterval = queryObj.get("window").toString();

						}
						//add event header to context
						Map<String, AlertHeader> eventHeaders= (Map<String, AlertHeader>) context.getAttribute("eventHeaders");
						eventHeaders.put(name, new AlertHeader(name, pilotId, type, level,timeInterval));
					} else{
						//add event header to context
						Map<String, AlertHeader> eventHeaders= (Map<String, AlertHeader>) context.getAttribute("eventHeaders");
						eventHeaders.put(name, new AlertHeader(name, pilotId, type, level));
					}

					//parse parameters
					String parameters = "";
					ArrayList<PatternParameter> paramArray = new ArrayList<PatternParameter>();
					if(queryObj.containsKey("parameters")){
						parameters = parseParameters((JSONObject)queryObj.get("parameters"));

					}

					queryString = "@Name(\"" + name + "\") select " + selectStr + " from " + streamStr;
					if(timeInterval !=null){
						queryString += ".win:" + timeInterval;
					}
					if(parameters.length()>1){
						queryString += " where " + parameters;
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
