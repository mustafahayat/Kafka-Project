package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-first-app";
        String topic = "first_topic";


        //1)  Create Consumer Config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // the value for this one will be: earliest(get the earliest data), latest(get the latest data), or none(throw error)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //2) Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        //3) Subscribe the consumer with topic(s)

            // 3.0 To work with one Topic
        consumer.subscribe(Collections.singleton(topic));

            //3.1 To work with multiple topics
//          consumer.subscribe(Arrays.asList("topic1, topic2, topic..."));

        // 4) poll the new data
        while (true){ // just for better understanding
           ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100)); // this is new from 2.0 ---> 2...
            for (ConsumerRecord<String, String > record:records) {
                System.out.println("Key: " + record.key() +", Value: " + record.value());
                System.out.println("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }

    }
}
