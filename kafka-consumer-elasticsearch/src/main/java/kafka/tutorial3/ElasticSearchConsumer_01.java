package kafka.tutorial3;

import org.apache.http.HttpHost;
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

public class ElasticSearchConsumer_01 {
    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer_01.class.getName());

        RestHighLevelClient client = createClient();
        String jsonString = "{ \"Instructor\" : \"Hayat\"}";
        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "tweets" // with using a tpe paramenter this is deprecated


        ).source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        String id = indexResponse.getId();
        logger.info(id);

        // closing client
        client.close();
    }

    // Create the RestHighLevelClient
    public  static RestHighLevelClient createClient(){

        //This is use for local Elastic Search
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("localhost", 9200));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    // Create Kafka Consumer
}
