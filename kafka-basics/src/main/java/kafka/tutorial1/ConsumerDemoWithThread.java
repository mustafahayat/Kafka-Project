package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
    //
        // **** The Better way of Closing an application ***
        // *** Use This like Framework **** This is little bit advance
         new ConsumerDemoWithThread().run();






    }
    // this is constructor
    private   ConsumerDemoWithThread(){

    }
    private   void run(){
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-third-app011";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());

        // Creating the Consumer Runnable
        logger.info("Creating the Consumer Runnable...");

        // Latch for multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        Runnable myConsumerRunnable = new ConsumerRunnable(topic, bootstrapServer, groupId, latch);


        // Start the thread.
        Thread myThread = new Thread();
        myThread.start();

        // add a shot down hook
       Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.info("Caught Shut Down Hook");
        // this will shut down the consumer
        ((ConsumerRunnable) myConsumerRunnable).shutDown();
           try {
               latch.await();
           } catch (InterruptedException e) {
               e.printStackTrace();
           }
           logger.info("Application has exited! ");
       }));



        try {
            // this will wait until all the application is over.
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted.");
            e.printStackTrace();
        }finally {
            logger.info("Application is Closing... ");
        }


    }


    public    class  ConsumerRunnable implements  Runnable{

        // This latch is used to shutdown the app correctly.
        private  CountDownLatch latch;
        private  KafkaConsumer<String, String> consumer;
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        //The latch is used for Concurrency.
        public  ConsumerRunnable( String topic, String bootstrapServer, String groupId, CountDownLatch latch){
            this.latch = latch;

            //1)  Create Consumer Config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            // the value for this one will be: earliest(get the earliest data), latest(get the latest data), or none(throw error)
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            //2) Create Consumer
              consumer = new KafkaConsumer<String, String>(properties);

              //3) Subscribe the consumer with topic(s)
                //3.1 To work with multiple topics
    // consumer.subscribe(Arrays.asList("topic1, topic2, topic..."));

            // 3.0 To work with one Topic
            consumer.subscribe(Collections.singleton(topic));

        }
        @Override
        public void run() {
            // 4) poll the new data
           try {
               while (true){ // just for better understanding
                   ConsumerRecords<String, String> records =
                           consumer.poll(Duration.ofMillis(100)); // this is new from 2.0 ---> 2...
                   for (ConsumerRecord<String, String > record:records) {
                       logger.info("Key: " + record.key() +", Value: " + record.value());
                       logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                   }
               }
           }catch (WakeupException e){
               logger.info("Received Shutdown Signal!");
           }finally {
               consumer.close();
               // tell our  main code we are done with consumer
               latch.countDown();
           }

        }

        public void shutDown(){
            // This is special method which interrupt the consumer.poll()
            // and throw an exception : wackup exception
            consumer.wakeup();
        }
    }
}
