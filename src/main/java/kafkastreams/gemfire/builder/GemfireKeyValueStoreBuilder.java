package kafkastreams.gemfire.builder;

import org.apache.geode.cache.Cache;
import org.apache.kafka.common.serialization.Serde;

import static java.util.Objects.requireNonNull;

public class GemfireKeyValueStoreBuilder<K, V> {
    private final String name;
    private Cache cache;
    private Serde<Object> keySerde;
    private Serde<Object> valueSerde;
    private boolean cached;

    public GemfireKeyValueStoreBuilder(String name) {
        requireNonNull(name, "name cannot be null");
        this.name = name;
    }

    public GemfireKeyValueStoreSupplier<K, V> build() {
        return new GemfireKeyValueStoreSupplier<>(
                name,
                cache,
                keySerde,
                valueSerde,
                cached);
    }

    public GemfireKeyValueStoreBuilder<K, V> withCache(Cache gemfireCache) {
        requireNonNull(gemfireCache, "gemfire cache cannot be null");
        this.cache = gemfireCache;
        return this;
    }

    public GemfireKeyValueStoreBuilder<K, V> withKeys(Serde<Object> serde) {
        requireNonNull(serde, "serde cannot be null");
        this.keySerde = serde;
        return this;
    }

    public GemfireKeyValueStoreBuilder<K, V> withValues(Serde<Object> serde) {
        requireNonNull(serde, "serde cannot be null");
        this.valueSerde = serde;
        return this;
    }

    public GemfireKeyValueStoreBuilder<K, V> cached() {
        this.cached = true;
        return this;
    }

    public GemfireKeyValueStoreBuilder<K, V> cached(boolean cached) {
        this.cached = cached;
        return this;
    }

}
