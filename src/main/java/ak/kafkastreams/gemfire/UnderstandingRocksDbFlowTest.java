package kafkastreams.gemfire.store.cache;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class UnderstandingRocksDbFlowTest {

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(2);

    private static String updateTopic = "update-topic";
    private static String latestTopic = "latest-topic";
    private static Properties streamsConfig;
    private static Properties messageConsumerConfig;
    private static Properties messageProducerConfig;

    private List<KeyValue<String, String>> data1 =
            Arrays.asList(
                    new KeyValue<>(
                            "key5",
                            "5"),
                    new KeyValue<>(
                            "key6",
                            "6"),
                    new KeyValue<>(
                            "key7",
                            "7"),
                    new KeyValue<>(
                            "key8",
                            "8"));

    private List<KeyValue<String, String>> data2 =
            Arrays.asList(
                    new KeyValue<>(
                            "key1",
                            "First_Data_Key1"),
                    new KeyValue<>(
                            "key2",
                            "Second_Data_Key2"),
                    new KeyValue<>(
                            "key3",
                            "Third_Data_Key3"),
                    new KeyValue<>(
                            "key4",
                            "Fourth_Data_Key4"));

    private List<KeyValue<String, String>> data3 =
            Arrays.asList(
                    new KeyValue<>(
                            "key1",
                            "Should_get_joined_Key1"));

    private List<KeyValue<String, String>> data4 =
            Arrays.asList(
                    new KeyValue<>(
                            "key2",
                            "Should_get_joined_key2"));

    @BeforeClass
    public static void setup() throws Exception {
        CLUSTER.createTopic(updateTopic,1,1);
        CLUSTER.createTopic(latestTopic,1,1);
        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-rocksdb");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamsConfig.put(
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 20 * 1024 * 1024L);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
        // advanced config for consumers

        // advance config for producer
        messageProducerConfig = new Properties();
        messageProducerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        messageProducerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        messageProducerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        messageProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        messageProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        messageConsumerConfig = new Properties();
        messageConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        messageConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        messageConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        messageConsumerConfig.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        messageConsumerConfig.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @Test
    public void loadDataToRocksDBTest() throws ExecutionException, InterruptedException {
        StreamsBuilder builder = new StreamsBuilder();
        joinStreams(builder,latestTopic);
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();
        IntegrationTestUtils.produceKeyValuesSynchronously(
                latestTopic, data1, messageProducerConfig, Time.SYSTEM);
        List<KeyValue<String, String>> valueAfterReceivingData = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(messageConsumerConfig, latestTopic, 4);
        streams.close();
        valueAfterReceivingData.size();
    }

    @Test
    public void howDoesRocksDbWorkTest() throws ExecutionException, InterruptedException {
        StreamsBuilder builder = new StreamsBuilder();
        joinStreams(builder,latestTopic);
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();IntegrationTestUtils.produceKeyValuesSynchronously(
                latestTopic, data1, messageProducerConfig, Time.SYSTEM);
        IntegrationTestUtils.produceKeyValuesSynchronously(
                latestTopic, data2, messageProducerConfig, Time.SYSTEM);
        IntegrationTestUtils.produceKeyValuesSynchronously(
                updateTopic, data3, messageProducerConfig, Time.SYSTEM);
        IntegrationTestUtils.produceKeyValuesSynchronously(
                updateTopic, data4, messageProducerConfig, Time.SYSTEM);
        List<KeyValue<String, String>> valueAfterReceivingData = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(messageConsumerConfig, latestTopic, 10);
        streams.close();
        valueAfterReceivingData.size();
    }

    public void joinStreams(
            StreamsBuilder kStreamBuilder, String latestTopic) {

        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("test-store"),Serdes.String(),Serdes.String()).withLoggingDisabled();

        KTable<String,String> latestTable = kStreamBuilder.addStateStore(storeBuilder).table(latestTopic);

        KStream<String,String> updateStream = kStreamBuilder.stream(updateTopic);

        updateStream.join(latestTable, (left,right) -> left+" : "+right).to(latestTopic);

    }
}
