package kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        System.out.println( "Hello, Kafka!!");
        String bootstrapServer =  "127.0.0.1:9092";

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


        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());



    //  2) create producer
    KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);

    // 3) send data

    // first let's create a record
    ProducerRecord<String, String> record =
            new ProducerRecord<String, String>("first_topic", "My Name is Mohammad Mustafa");
    producer.send(record);

//        // flush data
    producer.flush();
//
//        // flush and close the producer
    producer.close();




        
    }
}
