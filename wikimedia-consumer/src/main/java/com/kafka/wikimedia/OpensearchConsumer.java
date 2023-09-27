package com.kafka.wikimedia;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpensearchConsumer {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(OpensearchConsumer.class.getSimpleName());
    private static final String topic = "wikimedia.recentchange", connString = "http://localhost:9200";

    private static RestHighLevelClient createOpenSearchClient() {
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                    .setHttpClientConfigCallback(
                        httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        log.info("Kafka Consumer Started");
        Properties properties = new Properties();
        String groupId = "consumer-opensearch";

        // Setting consumer properties
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Turning auto commit off
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    private static String extractId(String record){
        return JsonParser
                .parseString(record)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        // create OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();


        try (openSearchClient; kafkaConsumer) { // will close the consumer client

            if (openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT))
                log.info("Wikimedia client already exists");
            else {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia index has been created.");
            }

            // Subscribing to the topic
            kafkaConsumer.subscribe(Collections.singleton(topic));

            // Shutdown hook
            final Thread mainThread = Thread.currentThread();

            Runtime.getRuntime().addShutdownHook(new Thread(){
                public void run(){
                    log.info("Detected shutdown - Exit consumer...");
                    kafkaConsumer.wakeup();

                    try {
                        mainThread.join();
                    } catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            });

            while (true){
                ConsumerRecords<String, String> batch = kafkaConsumer.poll(Duration.ofMillis(3000));

                int recordCount = batch.count();
                log.info("Received " + recordCount + " messages.");

                // Improving requests to OpenSearch with bulkRequest
                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : batch) {

                    // Create an id if you want to use idempotent consumers. or you can use the id of the message if it has one. In my case I use the id from the data
//                    String id = record.topic() + '_' + record.partition() + '_' + record.offset();

                    try {
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(extractId(record.value()));

                        bulkRequest.add(indexRequest);

                    } catch (Exception e){
                        log.info("There was an error");
                        log.error(e.toString());
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Done adding " + bulkResponse.getItems().length + " messages.");

//                    Committing offsets ofter done batching -- make sure the ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG is "false"
                    log.info("Committing offset after batch manually.");
                    kafkaConsumer.commitAsync();
//                    kafkaConsumer.commitSync(); // you can also commit Syncronously
                }
            }
        }catch (WakeupException e){
            log.info("Consumer is starting to shutdown");
        } catch (Exception e){
            log.error("Unexpected: ",e);
        } finally {
            kafkaConsumer.close();
            openSearchClient.close();
            log.info("The consumer has been closed.");
            log.info("OpenSearch has been closed.");
        }
    }
}

