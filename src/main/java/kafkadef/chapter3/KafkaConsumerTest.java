package kafkadef.chapter3;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerTest {
	private static Logger log = LoggerFactory.getLogger(KafkaConsumerTest.class);

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "35.197.141.20:9092");
		props.put("group.id", "Test");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		consumer.subscribe(Collections.singletonList("hoaipn"));
		
		try {
			Map<String, Integer> custCountryMap = new HashMap<>();
			
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (ConsumerRecord<String, String> record : records)
				{
					log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}\n",
							record.topic(), record.partition(), record.offset(),
							record.key(), record.value());
					
					int updatedCount = 1;
	
					if (custCountryMap.containsKey(record.value())) {
						updatedCount = custCountryMap.get(record.value()) + 1;
					}
					custCountryMap.put(record.value(), updatedCount);
					
					JSONObject json = new JSONObject(custCountryMap);
					System.out.println(json.toString(4));
				}
			}
		} finally {
			consumer.close();
		}		
	}
}
