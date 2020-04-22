package gr.uoa.di.pcomp.clients;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.BasicConfigurator;

import gr.uoa.di.pcomp.sampleAvro.SampleAvro;

public class SimpleProducerClient {

	private KafkaProducer<String, String> stringProducer;
	
	//Uncomment for avroMessage
	//private KafkaProducer<String, SampleAvro> avroProducer;

	private final String bootstrap = "localhost:9092";
	
	//Uncomment for avroMessage
	//private final String schema_registry = "http://localhost:8081";
	
	private final String stringTopic = "testStringTopic";
	
	//Uncomment for avroMessage
	//private final String avroTopic = "testAvroTopic";

	public static void main(String[] args) {
		
		BasicConfigurator.configure();
		final SimpleProducerClient client = new SimpleProducerClient();

		final Properties props = new Properties();
		props.put("bootstrap.servers", client.bootstrap);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		//Uncomment for avroMessage
		/*final Properties avroProps = new Properties();
		avroProps.put("bootstrap.servers", client.bootstrap);
		avroProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		avroProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		avroProps.put("schema.registry.url", client.schema_registry);*/
		

		client.stringProducer = new KafkaProducer<String, String>(props);
		
		//Uncomment for avroMessage
		//client.avroProducer = new KafkaProducer<String, SampleAvro>(avroProps);

		Runnable produceRunnable = new Runnable() {
			public void run() {	
					client.sendStringMessage(client.stringTopic);
					
					//Uncomment for avroMessage
					/*try {
						client.sendAvroMessage(client.avroTopic);
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}*/
			}
		};

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(produceRunnable, 0, 3, TimeUnit.SECONDS);
		try {
			if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
				executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			executor.shutdownNow();
		}

		client.stringProducer.close();
		
		//Uncomment for avroMessage
		//client.avroProducer.close();

	}

	public void sendStringMessage(String topic) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, "key",
				"Test message at " + (long) System.currentTimeMillis());
		stringProducer.send(producerRecord);
	}

	//Uncomment for avroMessage
	/*
	public void sendAvroMessage(String topic) throws InterruptedException, ExecutionException {
		ProducerRecord<String, SampleAvro> producerRecord = new ProducerRecord<String, SampleAvro>(topic, "key",
				new SampleAvro((long) System.currentTimeMillis(), 0.0));
		avroProducer.send(producerRecord);
	}*/

}
