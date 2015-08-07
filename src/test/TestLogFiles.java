package test;

import com.espertech.esper.client.EPRuntime;

import inputAdapters.nrg4cast.inputEvents.HistoricalDataProcessing;

 
public class TestLogFiles {


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String path= "E:/nrg4cast/www/logs";
		
		EPRuntime cepRT = null;
		Thread t = new Thread(new HistoricalDataProcessing(path, cepRT));
		t.start();
		System.out.println("Thread started");
		//connectPsql("postgres", "pgAdmin3");

	}
	
	

}
