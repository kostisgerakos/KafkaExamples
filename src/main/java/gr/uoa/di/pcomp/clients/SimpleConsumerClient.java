package gr.uoa.di.pcomp.clients;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.BasicConfigurator;

import gr.uoa.di.pcomp.sampleAvro.SampleAvro;


public class SimpleConsumerClient {
	
	private final String bootstrap = "localhost:9092";
	
	//Uncomment for avroMessage
	//private final String schema_registry = "http://localhost:8081";
	
	private final String group = "TestGroup";
	private final String stringTopic = "testStringTopic";
	
	//Uncomment for avroMessage
	//private final String avroTopic = "testAvroTopic";

	public static void main(String[] args) {
		
		//Uncomment for debug and info messages
		//BasicConfigurator.configure();
		
		final SimpleConsumerClient client = new SimpleConsumerClient();


		final Properties props = new Properties();
		props.put("bootstrap.servers", client.bootstrap);
		props.put("enable.auto.commit", "false");
		props.put("group.id", client.group);
	    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put("auto.offset.reset", "latest");
		
		//Uncomment for avroMessage
		/*final Properties avroProps = new Properties();
		avroProps.put("bootstrap.servers", client.bootstrap);
		avroProps.put("enable.auto.commit", "false");
		avroProps.put("group.id", client.group);
		avroProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		avroProps.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
		avroProps.put("schema.registry.url", client.schema_registry);
		avroProps.put("specific.avro.reader", "true");
		avroProps.put("auto.offset.reset", "latest");*/


		final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		//Uncomment for avroMessage
		//final KafkaConsumer<String, SampleAvro> avroConsumer = new KafkaConsumer<>(avroProps);
		
		consumer.subscribe(Arrays.asList(client.stringTopic));
		
		//Uncomment for avroMessage
		//avroConsumer.subscribe(Arrays.asList(client.avroTopic));
		
		while (true) {
			//Comment out next two lines for avroMessage
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1L));
			for (ConsumerRecord<String, String> record : records) {
				
			//Uncomment for avroMessage
			/*ConsumerRecords<String, SampleAvro> records = avroConsumer.poll(Duration.ofSeconds(1L));
			for (ConsumerRecord<String, SampleAvro> record : records) {*/
				System.out.println("MESSAGE RECEIVED: in partition: "+ record.partition() +" for message: " + record.value());
			}				
		}
	}
}
