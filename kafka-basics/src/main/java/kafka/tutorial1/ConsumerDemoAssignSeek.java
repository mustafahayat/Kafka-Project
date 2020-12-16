package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";


        //1)  Create Consumer Config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // the value for this one will be: earliest(get the earliest data), latest(get the latest data), or none(throw error)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        //2) Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

//      assign and seek is mostly used to replay data or fetch specific message.
        //)Assign topic with partition Subscribe the consumer with topic(s)
        TopicPartition topicPartitionToReadFrom = new TopicPartition("first_topic", 0);
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));

        // New We are seeking the assigned partition(s)
        // mean start from 15th offset
        consumer.seek(topicPartitionToReadFrom, 15L);

        int numberOfMessageToRead = 5;
        boolean keepReading = true;
        int numberOfMessageReadSoFor= 0;


        // 4) poll the new data

        while (keepReading){ // just for better understanding
           ConsumerRecords<String, String> records =
                   consumer.poll(Duration.ofMillis(100)); // this is new from 2.0 ---> 2...

            int size = records.count();
            System.out.println("The Size is: " + size);

            for (ConsumerRecord<String, String > record:records) {
                numberOfMessageReadSoFor++;
                System.out.println("Key: " + record.key() +", Value: " + record.value());
                System.out.println("Partition: " + record.partition() + ", Offset: " + record.offset());
                if(numberOfMessageReadSoFor >= numberOfMessageToRead){
                    keepReading = false; // to stop the while loop
                    break; // to exit the for loop
                }
            }
        }

    }
}
