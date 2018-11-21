package harsha.kafka_producer;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KProducer implements Runnable {

	String topicName;
	int waitCycle; // in seconds
	String threadName;
	int maxCount;

	private Thread thread;

	public KProducer(String topicName, int waitCycleSec, int maxCount, String threadName) {

		this.topicName = topicName;
		this.waitCycle = waitCycleSec * 1000;
		this.threadName = threadName;
		this.maxCount = maxCount;

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
		// TODO Auto-generated method stub
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

				int countPerSec = new Random().nextInt(maxCount);
//				System.out.println("count : "+countPerSec + " name: "+threadName);

				for (int i = 0; i < countPerSec; i++) {
					Double lat1 = 40.68786621 + Double.valueOf(Math.random() * (40.74279785 - 40.68786621));
					Double long1 = -74.1796875 + Double.valueOf(Math.random() * (-74.15771484 +74.1796875));

					Timestamp ts = new Timestamp(new Date().getTime());

					String value = lat1 + "," + long1 + "," + ts.toString() ;
					ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key,value);
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
