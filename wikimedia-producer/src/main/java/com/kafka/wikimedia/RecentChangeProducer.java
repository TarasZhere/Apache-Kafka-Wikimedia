package com.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class RecentChangeProducer {
    private static final String bootstrapServer = "localhost:9092", topic = "wikimedia.recentchange";
    private static Properties properties = new Properties();

    static {
        // Define properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        // Define serializer type
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // High throughput producer configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    }

    public static void main(String[] args) throws InterruptedException {
        // Wikimedia api url
        final String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        // Define the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        EventHandler eventHandler = new ChangeHandler(producer, topic);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));

        EventSource eventSource = builder.build();

        // Start the producer in another thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);
    }
}
