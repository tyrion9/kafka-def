package kafkadef.chapter3;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Hello world!
 *
 */
public class KafkaProducerTest {

	public static void main(String[] args) {
		System.out.println("Hello World!");

		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "35.197.141.20:9092");
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);

		ProducerRecord<String, String> record = new ProducerRecord<>("test2", "Precision Products France");
		try {
			for(int i = 0; i < 10; i++) {
				producer.send(record).get();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
