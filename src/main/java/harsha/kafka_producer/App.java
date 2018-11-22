package harsha.kafka_producer;

import java.util.Properties;


/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		
		
		int userWaitSec = Integer.parseInt(args[0]);
		int userMaxCount = Integer.parseInt(args[1]);
		int driverWaitSec = Integer.parseInt(args[2]);
		int driverMaxCount = Integer.parseInt(args[3]);
		int weatherWaitSec = Integer.parseInt(args[4]);
		int tripdataWaitSec = Integer.parseInt(args[5]);
		
		 KProducer threadguru1 = new KProducer("user",userWaitSec,userMaxCount,"userRequest");
		  threadguru1.start();
		  KProducer threadguru2 = new KProducer("driver",driverWaitSec,driverMaxCount,"DriverAvailable");
		  threadguru2.start();
		  
		  WeatherProducer wproducer = new WeatherProducer("weather", weatherWaitSec, "weathermetadata");
		  wproducer.start();
		  
		  TripDataProducer tproducer = new TripDataProducer("tripdata", weatherWaitSec, "triprmetadata");
		  tproducer.start();
	}

}
