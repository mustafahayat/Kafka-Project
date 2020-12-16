package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer_01 {

    Logger logger = LoggerFactory.getLogger(TwitterProducer_01.class.getName());
    String consumerKey = "M4ieKUnZF9Usc07HeiybZsAAH";
    String consumerSecret = "GFWgFuUr7Temu4YhZIv9Y7D5whgKkMLF2qqBkzOhJPObiuWOuO";
    String secret = "z0BcZgubQdunbUg5IJoR8Jpwz0lbVGG0W4A17RpxQ8hEY";
    String token = "1336131553425055750-2Kooj4LdJiin5AfrcnbxJ0JNnasvNp";



    // the constructor:
    public TwitterProducer_01(){}



    public static void main(String[] args) {
        new TwitterProducer_01().run();
    }

    // this is run method
    public void run() {

        logger.info("Setup the application");
//        Declaring the connection information:
//        ```java
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);


        //1) create twitter client
        Client hosebirdClient = createTwitterClients(msgQueue);
        // Attempts to establish a connection.
        hosebirdClient.connect();

        //2) create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();


        // add shutDown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Stopping Application...");
            logger.info("Shutting down Twitter from tweets");
            hosebirdClient.stop();
            logger.info("Closing Producer");
            logger.info("Done!");
        }));


        // on a different thread, or multiple different threads....
        // loop to send tweets to kafka
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(4, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
                logger.info("Error is Occurred");
            }
            if(msg != null){
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>
                        ("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            logger.error("Something bad was happened" + e);
                        }
                    }
                });
            }
//            something(msg);
//            profit();
        }

        logger.info("End of Application");





    }

    public Client createTwitterClients(BlockingQueue<String> msgQueue){





/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms

        // I have comment the below, because this is for people
        // and in this tutorial we are using term only
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);

        // This is for terms
        List<String> terms = Lists.newArrayList("Mustafa");
       // hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms( terms);

    // These secrets should be read from a config file
        Authentication hosebirdAuth =

                new OAuth1(consumerKey, consumerSecret, token, secret);


//Creating a client:
//```java
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
               // .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        return hosebirdClient;


    }


    // Create kafka Producer
    public KafkaProducer<String, String> createKafkaProducer(){
        String bootstrapServer =  "127.0.0.1:9092";

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return  new KafkaProducer<String, String>(properties);

    }
}
