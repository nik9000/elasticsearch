package org.elasticsearch.indices;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.TimeSeriesIdGenerator;
import org.elasticsearch.index.mapper.MapperService;

import java.util.concurrent.ExecutionException;
import java.util.function.Function;

/**
 * Looks up the {@link TimeSeriesIdGenerator} for an index. If the index is
 * local we read from the local metadata. If the index isn't local we parse
 * the mapping, read it, and cache it.
 */
class TimeSeriesIdGeneratorService implements Function<IndexMetadata, TimeSeriesIdGenerator> {
    public static final Setting<ByteSizeValue> CACHE_SIZE = Setting.memorySizeSetting(
        "indices.time_series.generator.cache.size",
        "0.1%", // 512k - 30mb
        Property.NodeScope
    );
    public static final Setting<TimeValue> CACHE_EXPIRE = Setting.positiveTimeSetting(
        "indices.time_series.generator.cache.expire",
        new TimeValue(0),
        Property.NodeScope
    );

    private final Cache<Key, TimeSeriesIdGenerator> nonLocalGeneratorCache;
    private final IndicesService indicesService;

    TimeSeriesIdGeneratorService(Settings nodeSettings, IndicesService indicesService) {
        CacheBuilder<Key, TimeSeriesIdGenerator> cacheBuilder = CacheBuilder.<Key, TimeSeriesIdGenerator>builder();
        cacheBuilder.weigher((key, gen) -> key.ramBytesUsed() + gen.ramBytesUsed());
        cacheBuilder.setMaximumWeight(CACHE_SIZE.get(nodeSettings).getBytes());
        TimeValue expire = CACHE_EXPIRE.get(nodeSettings);
        if (false == expire.equals(TimeValue.ZERO)) {
            cacheBuilder.setExpireAfterAccess(expire);
        }
        nonLocalGeneratorCache = cacheBuilder.build();

        this.indicesService = indicesService;
    }

    @Override
    public TimeSeriesIdGenerator apply(IndexMetadata meta) {
        if (false == meta.inTimeSeriesMode()) {
            return null;
        }
        /*
         * If we have the index locally we can just grab the time series
         * generator with two volatile reads.
         */
        IndexService localIndex = indicesService.indexService(meta.getIndex());
        if (localIndex != null) {
            return localIndex.mapperService().documentMapper().getTimeSeriesIdGenerator();
        }
        // We don't have a local copy. Go to the cache.
        try {
            return nonLocalGeneratorCache.computeIfAbsent(new Key(meta.getIndex(), meta.getMappingVersion()), source -> {
                try (MapperService tmp = indicesService.createIndexMapperService(meta)) {
                    return tmp.documentMapper().getTimeSeriesIdGenerator();
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("oh no", e); // NOCOMMIT error message
        }
    }

    private static class Key implements Accountable {
        private final String name;
        private final String uuid;
        private final long mappingVersion;

        Key(Index index, long mappingVersion) {
            this.name = index.getName();
            this.uuid = index.getUUID();
            this.mappingVersion = mappingVersion;
        }

        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Key.class);

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.alignObjectSize(SHALLOW_SIZE + RamUsageEstimator.sizeOf(name) + RamUsageEstimator.sizeOf(uuid));
        }

        @Override
        public int hashCode() {
            int hash = name.hashCode();
            hash = 31 * hash + uuid.hashCode();
            hash = 31 * hash + Long.hashCode(mappingVersion);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Key that = (Key) obj;
            return name.equals(that.name) && uuid.equals(that.uuid) && mappingVersion == that.mappingVersion;
        }
    }
}
