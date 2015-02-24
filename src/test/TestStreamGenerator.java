package test;

import inputAdapters.nrg4cast.inputEvents.DataStreamGenerator;

public class TestStreamGenerator {
	
	public static void main(String[] args){
		Thread t = new Thread(new DataStreamGenerator());
		t.start();
	}
	

}
