package kafkastreams.gemfire.store.builder;

public class GemfireStore {
    public static <K, V> GemfireKeyValueStoreBuilder<K, V> gemfireStore(String name) {
        return new GemfireKeyValueStoreBuilder<>(name);
    }
}
