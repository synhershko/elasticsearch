/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet.histogram;

import gnu.trove.map.hash.TLongLongHashMap;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.facet.DoubleFacetAggregatorBase;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 * A histogram facet collector that uses the same field as the key as well as the
 * value.
 */
public class CountHistogramFacetExecutor extends FacetExecutor {

    private final IndexNumericFieldData indexFieldData;
    private final HistogramFacet.ComparatorType comparatorType;
    final long interval;

    final TLongLongHashMap counts;

    public CountHistogramFacetExecutor(IndexNumericFieldData indexFieldData, long interval, HistogramFacet.ComparatorType comparatorType, SearchContext context) {
        this.comparatorType = comparatorType;
        this.indexFieldData = indexFieldData;
        this.interval = interval;

        this.counts = CacheRecycler.popLongLongMap();
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalCountHistogramFacet(facetName, comparatorType, counts, true);
    }

    public static long bucket(double value, long interval) {
        return (((long) (value / interval)) * interval);
    }

    class Collector extends FacetExecutor.Collector {

        private final HistogramProc histoProc;
        private DoubleValues values;

        public Collector() {
            histoProc = new HistogramProc(interval, counts);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            values = indexFieldData.load(context).getDoubleValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            histoProc.onDoc(doc, values);
        }

        @Override
        public void postCollection() {
        }
    }

    public final static class HistogramProc extends DoubleFacetAggregatorBase {

        private final long interval;
        private final TLongLongHashMap counts;

        public HistogramProc(long interval, TLongLongHashMap counts) {
            this.interval = interval;
            this.counts = counts;
        }

        @Override
        public void onValue(int docId, double value) {
            long bucket = bucket(value, interval);
            counts.adjustOrPutValue(bucket, 1, 1);
        }

        public TLongLongHashMap counts() {
            return counts;
        }
    }
}