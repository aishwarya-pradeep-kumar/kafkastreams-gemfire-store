package kafkastreams.gemfire.service;

import kafkastreams.gemfire.builder.GemfireKeyValueStoreBuilder;

public class GemfireStore {
    public static <K, V> GemfireKeyValueStoreBuilder<K, V> gemfireStore(String name) {
        return new GemfireKeyValueStoreBuilder<>(name);
    }
}
