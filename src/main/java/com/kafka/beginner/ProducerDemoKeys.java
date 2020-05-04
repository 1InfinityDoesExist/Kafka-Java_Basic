package com.kafka.beginner;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootstrapServer = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //send data
        for (int iter = 0; iter < 10; iter++) {

            String topic = "first_topic";
            String value = "Hello Workd" + Integer.toString(iter);
            String key = "id_ " + Integer.toString(iter);
            //create Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key:" + key);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a record is successfully send or exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata. \n" + "Topic:" + recordMetadata.topic() + "\n" + "Partition:" + recordMetadata.partition() + "\n" + "Offset:" + recordMetadata.offset() + "\n" + "Timestamp:" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); //block the .send() to make it sync
        }

        //since its asysnc
        producer.flush();
        producer.close();
    }
}
