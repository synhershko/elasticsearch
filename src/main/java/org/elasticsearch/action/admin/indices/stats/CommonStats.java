/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.cache.id.IdCacheStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;

import java.io.IOException;

/**
 */
public class CommonStats implements Streamable, ToXContent {

    @Nullable
    public DocsStats docs;

    @Nullable
    public StoreStats store;

    @Nullable
    public IndexingStats indexing;

    @Nullable
    public GetStats get;

    @Nullable
    public SearchStats search;

    @Nullable
    public MergeStats merge;

    @Nullable
    public RefreshStats refresh;

    @Nullable
    public FlushStats flush;

    @Nullable
    public WarmerStats warmer;

    @Nullable
    public FilterCacheStats filterCache;

    @Nullable
    public IdCacheStats idCache;

    @Nullable
    public FieldDataStats fieldData;

    public void add(CommonStats stats) {
        if (docs == null) {
            if (stats.getDocs() != null) {
                docs = new DocsStats();
                docs.add(stats.getDocs());
            }
        } else {
            docs.add(stats.getDocs());
        }
        if (store == null) {
            if (stats.getStore() != null) {
                store = new StoreStats();
                store.add(stats.getStore());
            }
        } else {
            store.add(stats.getStore());
        }
        if (indexing == null) {
            if (stats.getIndexing() != null) {
                indexing = new IndexingStats();
                indexing.add(stats.getIndexing());
            }
        } else {
            indexing.add(stats.getIndexing());
        }
        if (get == null) {
            if (stats.getGet() != null) {
                get = new GetStats();
                get.add(stats.getGet());
            }
        } else {
            get.add(stats.getGet());
        }
        if (search == null) {
            if (stats.getSearch() != null) {
                search = new SearchStats();
                search.add(stats.getSearch());
            }
        } else {
            search.add(stats.getSearch());
        }
        if (merge == null) {
            if (stats.getMerge() != null) {
                merge = new MergeStats();
                merge.add(stats.getMerge());
            }
        } else {
            merge.add(stats.getMerge());
        }
        if (refresh == null) {
            if (stats.getRefresh() != null) {
                refresh = new RefreshStats();
                refresh.add(stats.getRefresh());
            }
        } else {
            refresh.add(stats.getRefresh());
        }
        if (flush == null) {
            if (stats.getFlush() != null) {
                flush = new FlushStats();
                flush.add(stats.getFlush());
            }
        } else {
            flush.add(stats.getFlush());
        }
        if (warmer == null) {
            if (stats.getWarmer() != null) {
                warmer = new WarmerStats();
                warmer.add(stats.getWarmer());
            }
        } else {
            warmer.add(stats.getWarmer());
        }
        if (filterCache == null) {
            if (stats.getFilterCache() != null) {
                filterCache = new FilterCacheStats();
                filterCache.add(stats.getFilterCache());
            }
        } else {
            filterCache.add(stats.getFilterCache());
        }

        if (idCache == null) {
            if (stats.getIdCache() != null) {
                idCache = new IdCacheStats();
                idCache.add(stats.getIdCache());
            }
        } else {
            idCache.add(stats.getIdCache());
        }

        if (fieldData == null) {
            if (stats.getFieldData() != null) {
                fieldData = new FieldDataStats();
                fieldData.add(stats.getFieldData());
            }
        } else {
            fieldData.add(stats.getFieldData());
        }
    }

    @Nullable
    public DocsStats getDocs() {
        return this.docs;
    }

    @Nullable
    public StoreStats getStore() {
        return store;
    }

    @Nullable
    public IndexingStats getIndexing() {
        return indexing;
    }

    @Nullable
    public GetStats getGet() {
        return get;
    }

    @Nullable
    public SearchStats getSearch() {
        return search;
    }

    @Nullable
    public MergeStats getMerge() {
        return merge;
    }

    @Nullable
    public RefreshStats getRefresh() {
        return refresh;
    }

    @Nullable
    public FlushStats getFlush() {
        return flush;
    }

    @Nullable
    public WarmerStats getWarmer() {
        return this.warmer;
    }

    @Nullable
    public FilterCacheStats getFilterCache() {
        return this.filterCache;
    }

    @Nullable
    public IdCacheStats getIdCache() {
        return this.idCache;
    }

    @Nullable
    public FieldDataStats getFieldData() {
        return this.fieldData;
    }

    public static CommonStats readCommonStats(StreamInput in) throws IOException {
        CommonStats stats = new CommonStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            docs = DocsStats.readDocStats(in);
        }
        if (in.readBoolean()) {
            store = StoreStats.readStoreStats(in);
        }
        if (in.readBoolean()) {
            indexing = IndexingStats.readIndexingStats(in);
        }
        if (in.readBoolean()) {
            get = GetStats.readGetStats(in);
        }
        if (in.readBoolean()) {
            search = SearchStats.readSearchStats(in);
        }
        if (in.readBoolean()) {
            merge = MergeStats.readMergeStats(in);
        }
        if (in.readBoolean()) {
            refresh = RefreshStats.readRefreshStats(in);
        }
        if (in.readBoolean()) {
            flush = FlushStats.readFlushStats(in);
        }
        if (in.readBoolean()) {
            warmer = WarmerStats.readWarmerStats(in);
        }
        if (in.readBoolean()) {
            filterCache = FilterCacheStats.readFilterCacheStats(in);
        }
        if (in.readBoolean()) {
            idCache = IdCacheStats.readIdCacheStats(in);
        }
        if (in.readBoolean()) {
            fieldData = FieldDataStats.readFieldDataStats(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (docs == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            docs.writeTo(out);
        }
        if (store == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            store.writeTo(out);
        }
        if (indexing == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            indexing.writeTo(out);
        }
        if (get == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            get.writeTo(out);
        }
        if (search == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            search.writeTo(out);
        }
        if (merge == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            merge.writeTo(out);
        }
        if (refresh == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            refresh.writeTo(out);
        }
        if (flush == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            flush.writeTo(out);
        }
        if (warmer == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            warmer.writeTo(out);
        }
        if (filterCache == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            filterCache.writeTo(out);
        }
        if (idCache == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            idCache.writeTo(out);
        }
        if (fieldData == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            fieldData.writeTo(out);
        }
    }

    // note, requires a wrapping object
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (docs != null) {
            docs.toXContent(builder, params);
        }
        if (store != null) {
            store.toXContent(builder, params);
        }
        if (indexing != null) {
            indexing.toXContent(builder, params);
        }
        if (get != null) {
            get.toXContent(builder, params);
        }
        if (search != null) {
            search.toXContent(builder, params);
        }
        if (merge != null) {
            merge.toXContent(builder, params);
        }
        if (refresh != null) {
            refresh.toXContent(builder, params);
        }
        if (flush != null) {
            flush.toXContent(builder, params);
        }
        if (warmer != null) {
            warmer.toXContent(builder, params);
        }
        if (filterCache != null) {
            filterCache.toXContent(builder, params);
        }
        if (idCache != null) {
            idCache.toXContent(builder, params);
        }
        if (fieldData != null) {
            fieldData.toXContent(builder, params);
        }
        return builder;
    }
}
