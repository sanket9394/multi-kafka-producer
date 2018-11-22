package harsha.kafka_producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TripDataProducer implements Runnable {

	String topicName;
	int waitCycle; // in seconds
	String threadName;

	ArrayList<String> geohashes = new ArrayList<String>(Arrays.asList("dr5r81", "dr5r8q", "dr5r82", "dr5r8r", "dr5r83",
			"dr5r84", "dr5r8m", "dr5r8n", "dr5r80", "dr5r8p", "dr5r85", "dr5r2p", "dr5r86", "dr5r87", "dr5r2r",
			"dr5r8j", "dr5r8k", "dr5rb0", "dr5r8h", "dr5rb2"));

	// dr5r81,dr5r8q,dr5r82,dr5r8r,dr5r83,dr5r84,dr5r8m,dr5r8n,dr5r80,dr5r8p,dr5r85,dr5r2p,dr5r86,dr5r87,dr5r2r,dr5r8j,dr5r8k,dr5rb0,dr5r8h,dr5rb2
	private Thread thread;

	public TripDataProducer(String topicName, int waitCycleSec, String threadName) {

		this.topicName = topicName;
		this.waitCycle = waitCycleSec * 1000;
		this.threadName = threadName;

	}

	public void start() {
		System.out.println("Starting " + threadName);
		if (thread == null) {
			thread = new Thread(this, threadName);
			thread.start();
		}
	}

	@Override
	public void run() {
		System.out.println("Thread running : " + threadName);
		push();
	}

	private void push() {
		String key = "Key1";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		while (true) {

			try {

				int countPerSec = new Random().nextInt(10);
//				System.out.println("count : "+countPerSec + " name: "+threadName);

				for (int i = 0; i < countPerSec; i++) {

					Double picklat = 40.68786621 + Double.valueOf(Math.random() * (40.74279785 - 40.68786621));
					Double pickLong = -74.1796875 + Double.valueOf(Math.random() * (-74.15771484 +74.1796875));
					Double dropLat = 40.68786621 + Double.valueOf(Math.random() * (40.74279785 - 40.68786621));
					Double dropLong = -74.1796875 + Double.valueOf(Math.random() * (-74.15771484 +74.1796875));
					Double speed = 15 + Double.valueOf(Math.random() * (50 - 20));

					String value = picklat + "," + pickLong + "," + dropLat +","+dropLong+","+speed;
					ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
					producer.send(record);

				}

				Thread.sleep(waitCycle);

			} catch (Exception ex) {
				ex.printStackTrace(System.out);
				producer.close();
			}

		}
	}

}
