package kafkastreams.gemfire.store.service;

import kafkastreams.gemfire.store.builder.GemfireKeyValueStoreBuilder;

public class GemfireStore {
    public static <K, V> GemfireKeyValueStoreBuilder<K, V> gemfireStore(String name) {
        return new GemfireKeyValueStoreBuilder<>(name);
    }
}
