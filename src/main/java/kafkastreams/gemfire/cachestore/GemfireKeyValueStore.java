package kafkastreams.gemfire.cachestore;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections.map.HashedMap;
import org.apache.geode.cache.*;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.AbstractNotifyingBatchingRestoreCallback;
import org.apache.kafka.streams.processor.BatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.RocksDBStore;

import java.util.*;

@Log4j2
public class GemfireKeyValueStore implements KeyValueStore<Object, Object> {

    private String name;
    private Cache cache;
    private Serde<?> keySerde;
    private Serde<?> valueSerde;
    private Region<Object,Object> storeRegion;
    private volatile boolean open;
    private final Set<KeyValueIterator> openIterators = Collections.synchronizedSet(new HashSet<>());
    volatile BatchingStateRestoreCallback batchingStateRestoreCallback = null;
    private volatile boolean prepareForBulkload = false;
    private ProcessorContext context;

    public GemfireKeyValueStore(
            String name,
            Cache cache,
            Serde<?> keySerde,
            Serde<?> valueSerde) {
        this.cache = cache;
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Override
    public void put(Object k, Object v) {
        validateStoreOpen();
        Objects.requireNonNull(k, "key cannot be null");
        Objects.requireNonNull(v, "value cannot be null");
        this.storeRegion.put(k,v);
    }

    @Override
    public synchronized Object putIfAbsent(Object k, Object v) {
        validateStoreOpen();
        Objects.requireNonNull(k, "key cannot be null");
        Objects.requireNonNull(v, "value cannot be null");
        return this.storeRegion.putIfAbsent(k,v);
    }

    @Override
    public void putAll(List<KeyValue<Object,Object>> list) {
        validateStoreOpen();
        Objects.requireNonNull(list, "Key Values cannot be null");
        Map<Object,Object> entries = new HashedMap(list.size());
        for (KeyValue<Object,Object> entry : list) {
            entries.put(entry.key,entry.value);
        }
        this.storeRegion.putAll(entries);
    }

    @Override
    public synchronized Object delete(Object k) {
        validateStoreOpen();
        Objects.requireNonNull(k, "key cannot be null");
        return this.storeRegion.remove(k);
    }

    @Override
    public String name() {
        return this.name;
    }

    private Region<Object,Object> getRegion(Cache cache, String regionName, ProcessorContext context) {
        Region region = this.cache.getRegion(regionName);
        if (region == null) {
            RegionAttributes<Object,Object> regionAttributes = new RegionAttributesCreation();
            RegionFactory regionFactory = this.cache.createRegionFactory(regionAttributes);
            region = regionFactory.create(regionName);
            //if a new region gets created then bulkload from topic
            bulkLoadFromTopic(context);
        }
        return region;
    }

    private void initializeCache(ProcessorContext context) {
        this.storeRegion = this.getRegion(this.cache,this.name,context);
        this.open = true;
    }

    private void bulkLoadFromTopic(ProcessorContext context) {
        Map<String, Object> config = context.appConfigs();
    }

    @Override
    public void init(ProcessorContext processorContext, StateStore stateStore) {
        this.context = processorContext;
        this.initializeCache(this.context);
        batchingStateRestoreCallback = new GemfireBatchingRestoreCallback(this);
        processorContext.register(stateStore,batchingStateRestoreCallback);
    }

    @Override
    public void flush() {
        this.storeRegion.clear();
    }

    @Override
    public void close() {
        this.cache.close();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public Object get(Object k) {
        return this.storeRegion.get(k);
    }

    @Override
    public KeyValueIterator<Object, Object> range(Object k, Object v) {
        return null;
    }

    @Override
    public synchronized KeyValueIterator<Object, Object> all() {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        this.validateStoreOpen();

        long value;
        try {
            value = this.storeRegion.size();
        } catch (StatisticsDisabledException exception) {
            throw new ProcessorStateException("Error fetching property from kafkastreams " + this.name, exception);
        }
        return this.isOverflowing(value) ? 9223372036854775807L : value;
    }

    private boolean isOverflowing(long value) {
        return value < 0L;
    }

    private void validateStoreOpen() {
        if (!this.open) {
            throw new InvalidStateStoreException("Store " + this.name + " is currently closed");
        }
    }

    static class GemfireBatchingRestoreCallback extends AbstractNotifyingBatchingRestoreCallback {
        private final GemfireKeyValueStore gemfireKeyValueStore;

        GemfireBatchingRestoreCallback(GemfireKeyValueStore gemfireKeyValueStore) {
            this.gemfireKeyValueStore = gemfireKeyValueStore;
        }

        @Override
        public void restoreAll(final Collection<KeyValue<byte[], byte[]>> records) {
//            try (final WriteBatch batch = new WriteBatch()) {
//                rocksDBStore.dbAccessor.prepareBatchForRestore(records, batch);
//                rocksDBStore.write(batch);
//            } catch (final RocksDBException e) {
//                throw new ProcessorStateException("Error restoring batch to store " + rocksDBStore.name, e);
//            }
        }
    }
}
