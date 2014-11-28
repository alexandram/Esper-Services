package servlets;


import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import esperCore.EsperInstance;

/**
 * Servlet implementation class EPLQueryRemoval
 */
@WebServlet("/EventPatternRemoval")
public class EPLQueryRemoval extends HttpServlet {
	private static final long serialVersionUID = 1L;
	static String nrg4castEngineName = "nrg4cast"; //should be a parameter send by client   
       
    /**
     * @see HttpServlet#HttpServlet()
     */
    public EPLQueryRemoval() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			
		PrintWriter out = response.getWriter();

		//get the engine name
		ServletContext context = request.getSession().getServletContext();
		Map<String, EsperInstance> contextAttr = (Map<String, EsperInstance>) context.getAttribute("instances");
		EsperInstance esper = contextAttr.get(nrg4castEngineName);
		
		//if a name attribute is found than delete the query with that name, else return all existing queries
		if(request.getParameter("name") !=null){
			response.setContentType("text/plain");
			String queryName = request.getParameter("name"); 
			out.write(esper.deleteQuery(queryName));
		} else {
			response.setContentType("application/json");	
			HashMap<String, String> queries = esper.getRegisteredQueries();
			
			JSONObject obj = new JSONObject();
			JSONArray queriesJSON = new JSONArray();
			for(String key: queries.keySet()){
				JSONObject queryObj = new JSONObject();
				queryObj.put("name", key);
				queryObj.put("epl", queries.get(key));
				queriesJSON.add(queryObj);
			}
			obj.put("queries", queriesJSON);
			
			out.write(obj.toJSONString());
			out.close();
		}
	
		
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

}
