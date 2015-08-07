package esperCore;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.deploy.DeploymentOptions;
import com.espertech.esper.client.deploy.DeploymentOrder;
import com.espertech.esper.client.deploy.DeploymentResult;
import com.espertech.esper.client.deploy.Module;
import eventTypes.nrg4cast.Prediction;

/**
 * @author alexandram
 * 
 */
public class EsperEngine implements ServletContextListener {

	static final Logger log = Logger.getLogger(EsperEngine.class);
	static String nrg4castEngineName = "nrg4cast";
	static String nrg4castHistEngineName = "nrg4castHist";
	private ServletContext context = null;
	private static Map<String, EsperInstance> instance;
	private static ArrayList<Prediction> predBuffer;
	static {
		instance = new HashMap<String, EsperInstance>();
		predBuffer =  new ArrayList<Prediction>();
	}

	
	private List<String> deploymentIds = new ArrayList<String>();

	@Override
	public void contextInitialized(ServletContextEvent servletContextEvent) {
			PropertyConfigurator.configure(new File("D:\\server\\"
					+ "TomcatNRG4Cast\\conf\\nrg4castApp\\log4j.properties").
					toString()); //uncoment for .war deployment
		this.context = servletContextEvent.getServletContext();


		this.context.setAttribute("instances", instance);
		this.context.setAttribute("predBuffer", predBuffer);

		// add engine instances
		instance.put(nrg4castEngineName, new EsperNrg4Cast(this.context));
		log.info(nrg4castEngineName + " engine started");
	//	instance.put(nrg4castHistEngineName, new EsperNrg4CastHist(this.context));
	//	log.info(nrg4castHistEngineName + " engine started");

		for(String s: EPServiceProviderManager.getProviderURIs()){
			System.out.println("known provider uri: " + s);
		}
		//add modules to the corresponding instance
		try {

			String engineModulesList = servletContextEvent.getServletContext()
					.getInitParameter("eplEngineModules");

			String[] split = engineModulesList.split(",");
			for (int i = 0; i < split.length; i++){
				String engineName = split[i].trim().replace("Modules", "");
				String modulesList = servletContextEvent.getServletContext()
						.getInitParameter(split[i].trim());
				if( Arrays.asList(EPServiceProviderManager.getProviderURIs())
						.contains(engineName)){
					addModules(EPServiceProviderManager.getProvider(engineName),
							modulesList, servletContextEvent);
				}else {
					addModules(EPServiceProviderManager.getDefaultProvider(),
							modulesList,servletContextEvent);
				}

			}

		}catch (Exception ex) {
			ex.printStackTrace();
		}

		//add listeners for each engine instance
		for(String engine: instance.keySet()){
			EsperInstance ei = instance.get(engine);
			ei.addListeners();
		}
		
		
		
	}


	@Override
	public void contextDestroyed(ServletContextEvent servletContextEvent) {

		//shut down esper engines
		for (String key : instance.keySet()) {
			if(instance.get(key) !=null){
				instance.get(key).shutdown();
				log.info(String.format("Instace %s succesfully shutdown",key));
			}

		}

	}

	private void addModules(EPServiceProvider cep, String modulesList,
			ServletContextEvent servletContextEvent) {
		try{
			List<Module> modules = new ArrayList<Module>();
			if (modulesList != null) {
				String[] split = modulesList.split(",");
				for (int i = 0; i < split.length; i++) {
					String resourceName = split[i].trim();
					if (resourceName.length() == 0) {
						continue;
					}
					//String realPath = servletContextEvent.getServletContext()
					//.getRealPath(resourceName);
					Module module = cep.getEPAdministrator()
							.getDeploymentAdmin().read(new File(resourceName));
					modules.add(module);
					log.info(String.format("Module %s added to engine %s",
							resourceName, cep.getURI()));
				}


				// Determine deployment order
				DeploymentOrder order = cep.getEPAdministrator()
						.getDeploymentAdmin().getDeploymentOrder(modules, null);

				// Deploy
				for (Module module : order.getOrdered()) {
					DeploymentResult result = cep.getEPAdministrator()
							.getDeploymentAdmin()
							.deploy(module, new DeploymentOptions());
					deploymentIds.add(result.getDeploymentId());
				}
			}
		}
		catch (Exception ex) {
			ex.printStackTrace();
		}

	}
	public static EsperInstance getInstance(String key) {
		return instance.get(key);
	}

}
