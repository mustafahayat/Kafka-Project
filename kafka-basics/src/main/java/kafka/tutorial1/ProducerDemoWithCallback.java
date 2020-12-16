package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {
        System.out.println( "Hello, Kafka!!");
        String boostrapServer =  "127.0.0.1:9092";
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        // Three step:
        //  1) create properties
        Properties properties = new Properties();
        // from the kafka documentation
//        properties.setProperty("bootstrap.servers", boostrapServer);
//
        // these both mean what type of data we are going to be sending
        // to the kafka.
        // this mean that we are sending string type of date
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", "");

        // the above is hard coded data let's do it dynamically.


        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //  2) create producer
        KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);

        // 3) send data

        // first let's create a record
        for (int i=0; i<10; i++)
        {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World!"+ i);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Recieved New Meta data: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n\n");
                    } else {
                        logger.error("Error is Occured On Producing a data.==> " + e);
                        System.out.println("\n==================================================\n\n");
                        e.printStackTrace();
                    }
                }
            });
        }
        // flush data
        producer.flush();

        // flush and close the producer
        producer.close();



    }
}
