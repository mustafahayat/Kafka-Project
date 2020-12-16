package kafka.tutorial_4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
        // properties

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        // Input Topic
        KStream<String, String> inputTopics = streamsBuilder.stream("twitter_tweets");
        // Filter tweets
        KStream<String, String> filteredStreams = inputTopics.filter(
                // filter for tweets which has a user with over then 1000 followers
                (k, jsonTweet) ->  // this is the java 8 style called delegates
                    extractUserFollowersInTweets(jsonTweet) > 1000
        );

        filteredStreams.to("important_tweets");

        // Built topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        // Start the streaming application
        kafkaStreams.start();
    }

  private static   JsonParser jsonParser = new JsonParser();
    public static Integer extractUserFollowersInTweets(String tweet){

        try {

            return jsonParser.parse(tweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();

        }catch (NullPointerException e){
            return 0;
        }
    }

}
