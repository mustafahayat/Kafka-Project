import com.google.gson.JsonParser;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.Doc;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class MongoDBConsumer {
    public static void main(String[] args) {

        String topic = "twitter_tweets";
        Logger logger = LoggerFactory.getLogger(MongoDBConsumer.class.getName());


        logger.info("Starting Application...");
        logger.info("Connecting with MongoDB...");
        // 1) Create MongoDB Connection
        MongoClient mongoClient = new MongoClient("localhost", 27017);

        logger.info("Create the Database...");
        // 2) Create the Database:
        MongoDatabase database = mongoClient.getDatabase("twitter");

        logger.info("Creating the Collection...");
        // 3) Create Collections:
        MongoCollection<Document> table = database.getCollection("tweets");

        logger.info("Start writing the record to the collection...");

        // Create the Record and in the loop we will initialize this
        Document document;
        KafkaConsumer<String, String> consumer = createKafkaConsumer(topic);
        int i = 1;
        while (true){ // just for better understanding
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // this is new from 2.0 ---> 2...
            for (ConsumerRecord<String, String > record:records) {
                //Here we weill send data to the MongoDB
               try {

                   String id = extractIdFromTweet(record.value());

                   // 4) Creating document or record or rows:

                    document = new Document("_id", id);

                   document.append(id, record.value());
                   table.insertOne(document);
                   logger.info(id);
               }catch (NullPointerException e){ // it useful if we use the twitter feed specific id
                   logger.warn("Skipping bad data - " + record.value());
               }
               i++;
            }
            //5) Send the data
            // And Finally Send the Data


        }




    }

    // create Kafka Consumer
    public  static KafkaConsumer<String, String> createKafkaConsumer(String topic){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-mongodb";


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



        return  consumer;
    }

    // Extract id from tweets
    private static JsonParser jsonParser = new JsonParser();
    private  static String extractIdFromTweet(String tweet){

        return   jsonParser.parse(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();

    }

}
