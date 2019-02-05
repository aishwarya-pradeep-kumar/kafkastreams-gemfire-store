package kafkastreams.gemfire.store.builder;

import kafkastreams.gemfire.store.cache.GemfireKeyValueStore;
import org.apache.geode.cache.Cache;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;

import java.lang.reflect.Constructor;

import static java.util.Objects.requireNonNull;

public class GemfireKeyValueStoreSupplier<K, V> implements StoreSupplier<KeyValueStore> {

    private String name;
    private Cache cache;
    private Serde<Object> keySerde;
    private Serde<Object> valueSerde;
    private boolean cached;

    public GemfireKeyValueStoreSupplier(
            String name,
            Cache cache,
            Serde<Object> keySerde,
            Serde<Object> valueSerde,
            boolean cached) {
        requireNonNull(name, "name cannot be null");
        requireNonNull(cache, "cache cannot be null");
        requireNonNull(keySerde, "keySerde cannot be null");
        requireNonNull(valueSerde, "valueSerde cannot be null");

        this.name = name;
        this.cache = cache;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.cached = cached;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public KeyValueStore get() {
        if (cached) {
            return cachedStore();
        }
        try {
            Constructor<?> meteredKeyValueStore = Class
                    .forName("org.apache.kafka.streams.state.internals.MeteredKeyValueStore")
                    .getDeclaredConstructor(KeyValueStore.class, String.class, Time.class, Serde.class, Serde.class);
            meteredKeyValueStore.setAccessible(true);
            return (KeyValueStore<K, V>) meteredKeyValueStore.newInstance(new GemfireKeyValueStore(name, cache, keySerde, valueSerde),
                    "gemfire-store",
                    Time.SYSTEM,
                    Serdes.Bytes(),
                    Serdes.ByteArray());
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Fatal exception while trying to instantiate cache", e);
        }
    }

    private KeyValueStore<K, V> cachedStore() {
        try {
            Constructor<?> cachingKeyValueStore = Class
                    .forName("org.apache.kafka.streams.state.internals.CachingKeyValueStore")
                    .getDeclaredConstructor(KeyValueStore.class, Serde.class, Serde.class);
            cachingKeyValueStore.setAccessible(true);
            Constructor<?> meteredKeyValueStore = Class
                    .forName("org.apache.kafka.streams.state.internals.MeteredKeyValueStore")
                    .getDeclaredConstructor(KeyValueStore.class, String.class, Time.class, Serde.class, Serde.class);
            meteredKeyValueStore.setAccessible(true);
            return (KeyValueStore<K, V>) cachingKeyValueStore.newInstance(
                    meteredKeyValueStore.newInstance(new GemfireKeyValueStore(name, cache, keySerde, valueSerde),
                            "gemfire-store",
                            Time.SYSTEM,
                            Serdes.Bytes(),
                            Serdes.ByteArray()),
                    keySerde, valueSerde);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Fatal exception while trying to instantiate cache", e);
        }
    }

    @Override
    public String metricsScope() {
        return null;
    }
}
