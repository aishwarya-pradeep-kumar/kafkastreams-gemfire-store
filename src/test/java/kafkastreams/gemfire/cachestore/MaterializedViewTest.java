package kafkastreams.gemfire.cachestore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Category({IntegrationTest.class})
public class MaterializedViewTest {
    private static final int NUM_BROKERS = 3;
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final String latestTopic = "latest";
    private static final String latestStore = latestTopic + "-store";
    private static final String updateTopic = "update";
    private ObjectMapper mapper = new ObjectMapper();


    @BeforeClass
    public static void startKafkaCluster() throws InterruptedException {
        CLUSTER.createTopic(latestTopic);
        CLUSTER.createTopic(updateTopic);
    }

    @Test
    public void shouldMergeValue() throws Exception {

        // Input 1: current state of latest topic.
        List<KeyValue<String, String>> initialMembershipData = Arrays.asList(
                new KeyValue<>("key1", "value1"),
                new KeyValue<>("key2", "value2")
        );

        List<KeyValue<String, String>> membershipUpdates = Arrays.asList(
                new KeyValue<>("key1", "value1"),
                new KeyValue<>("key2", "value2"),
                new KeyValue<>("key1", "value1")
        );

        List<KeyValue<String, String>> someMoreData = Arrays.asList(
                new KeyValue<>("key3", "value3"),
                new KeyValue<>("key2", "value2")
        );


        //
        // Step 1: Configure and start the processor topology.
        //
        final Serde<String> stringSerde = Serdes.String();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-lambda-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // The commit interval for flushing records to store stores and downstream must be lower than
        // this integration test's timeout (30 secs) to ensure we observe the expected processing results.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Use a temporary directory for storing store, which will be automatically removed after the test.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());


        StreamsBuilder builder = new StreamsBuilder();
        builder.table(latestTopic, Materialized.as(latestStore));

        builder
                .stream(updateTopic, Consumed.with(stringSerde, stringSerde))
                .transform(() -> new KStreamStateJoin(latestStore), latestStore)
                .to(latestTopic, Produced.with(stringSerde, stringSerde));


        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        //
        // Step 2: Publish initial latest information.
        //
        Properties latestProducerConfig = new Properties();
        latestProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        latestProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        latestProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        latestProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        latestProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        latestProducerConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0 /* milliseconds */);
        latestProducerConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        IntegrationTestUtils.produceKeyValuesSynchronously(latestTopic, initialMembershipData, latestProducerConfig, Time.SYSTEM);

        //
        // Step 3: Publish some update events.
        //
        Properties updatesProducerConfig = new Properties();
        updatesProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        updatesProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        updatesProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        updatesProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        updatesProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        updatesProducerConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0 /* milliseconds */);
        updatesProducerConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        IntegrationTestUtils.produceKeyValuesSynchronously(updateTopic, membershipUpdates,  updatesProducerConfig, Time.SYSTEM);


        //
        // Step 4: Verify the application's output data.
        //
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "join-lambda-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<KeyValue<String, String>> latestTopicMemberships = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
                latestTopic, 5);

        streams.close();
        latestTopicMemberships.stream().forEach(System.out::println);
        streams = new KafkaStreams(builder.build(), streamsConfiguration);
        // the state restore listener will append one record to the log
        streams.setGlobalStateRestoreListener(new UpdatingSourceTopicOnRestoreStartStateRestoreListener());
        streams.start();
        IntegrationTestUtils.produceKeyValuesSynchronously(latestTopic, someMoreData,  latestProducerConfig, Time.SYSTEM);
        List<KeyValue<String, String>> allMessages = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig,
                latestTopic, 7, 40000L);
        allMessages.stream().forEach(System.out::println);
        streams.close();
    }

    public class KStreamStateJoin implements Transformer<String, String, KeyValue<String, String>> {

        private final String storeName;
        private KeyValueStore<String, String> store;

        KStreamStateJoin(String storeName) {
            this.storeName = storeName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.store = (KeyValueStore<String, String>) context.getStateStore(storeName);
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            String existing = this.store.get(key);
            String merged = existing + " + " + value;
            this.store.put(key, merged);  // save latest value to store
            return new KeyValue<>(key, merged);
        }

        @Override
        public void close() {}
    }

    private class UpdatingSourceTopicOnRestoreStartStateRestoreListener implements StateRestoreListener {

        @Override
        public void onRestoreStart(final TopicPartition topicPartition,
                                   final String storeName,
                                   final long startingOffset,
                                   final long endingOffset) {
            try {
                Properties latestProducerConfig = new Properties();
                latestProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
                latestProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
                latestProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
                latestProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                latestProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                latestProducerConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0 /* milliseconds */);
                latestProducerConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
                List<KeyValue<String, String>> someData = Arrays.asList(
                        new KeyValue<>("key4", "value4")
                );
                IntegrationTestUtils.produceKeyValuesSynchronously(latestTopic, someData , latestProducerConfig, Time.SYSTEM);
            } catch (final ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void onBatchRestored(final TopicPartition topicPartition,
                                    final String storeName,
                                    final long batchEndOffset,
                                    final long numRestored) {
        }

        @Override
        public void onRestoreEnd(final TopicPartition topicPartition,
                                 final String storeName,
                                 final long totalRestored) {
        }
    }

}
