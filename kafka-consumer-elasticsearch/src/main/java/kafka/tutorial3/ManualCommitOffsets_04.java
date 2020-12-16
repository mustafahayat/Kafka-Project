package kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ManualCommitOffsets_04 {
    public static void main(String[] args) throws IOException {

        // There are two strategy to commit the offsets
         // 1) is the Enable_Auto_Commit = true
        //      Which automatically commit the message after 5 second synchronously
        // 2) is the Enable_Auto_Commit = false
        //      which is manualy adding a commit to the offset
                // we can do this be setting the property of Enable_Auto_Commit_Config = fals like
        //          in createKafkaConsumer method.


        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer_01.class.getName());
        String topic = "twitter_tweets";

        RestHighLevelClient client = createClient();


        KafkaConsumer<String, String> consumer = createKafkaConsumer(topic);
        // 4) poll the new data
        while (true){ // just for better understanding
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100)); // this is new from 2.0 ---> 2...

            logger.info("Received " + records.count() + " records");
            for (ConsumerRecord<String, String > record:records) {
                // here we will send data to elasticsearch

//             =========================
                /*
                TO make this Idempotance we have two strategy:
                This provided Id will make Idempotance which is prevent the duplicate message

                 1) Kafka genari Id: */
                    String id = record.topic() +"_"+ record.partition() + "_" + record.offset();
/*
                 2) twitter feed Specifc id: */

//                String id = extractIdFromTweet(record.value());

                // Insert data to elastic search
                IndexRequest indexRequest = new IndexRequest("twitter")
                        .id(id)
                        .source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                String responseId = indexResponse.getId();
                logger.info(responseId);

                // for one second latency
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
            logger.info("Committing the offsets...");
            consumer.commitSync();
            logger.info("Consumer have been committed");
            logger.info("=================================");

            // for one second latency
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // closing client
        // client.close();
    }
    // Create the RestHighLevelClient
    public  static RestHighLevelClient createClient(){

        //This is use for local Elastic Search
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200));

        // If we are using the remote or online (like bonsaisearch) we do like so::

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    // Create Kafka Consumer
    public  static KafkaConsumer<String, String> createKafkaConsumer(String topic){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";


        //1)  Create Consumer Config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // This will disable auto commit
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // we should receive 10 message at a time and then add commit
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");




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
   //private static JsonParser jsonParser = new JsonParser();
//    private  static String extractIdFromTweet(String tweet){
//
//      return   jsonParser.parse(tweet)
//                .getAsJsonObject()
//                .get("id_str")
//                .getAsString();
//
//    }

}
