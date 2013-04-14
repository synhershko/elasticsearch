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

package org.elasticsearch.search.facet.geodistance;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class ValueGeoDistanceFacetExecutor extends GeoDistanceFacetExecutor {

    private final IndexNumericFieldData valueIndexFieldData;

    public ValueGeoDistanceFacetExecutor(IndexGeoPointFieldData indexFieldData, double lat, double lon, DistanceUnit unit, GeoDistance geoDistance,
                                         GeoDistanceFacet.Entry[] entries, SearchContext context, IndexNumericFieldData valueIndexFieldData) {
        super(indexFieldData, lat, lon, unit, geoDistance, entries, context);
        this.valueIndexFieldData = valueIndexFieldData;
    }

    @Override
    public Collector collector() {
        return new Collector(new Aggregator(fixedSourceDistance, entries));
    }

    class Collector extends GeoDistanceFacetExecutor.Collector {

        Collector(Aggregator aggregator) {
            super(aggregator);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            super.setNextReader(context);
            ((Aggregator) this.aggregator).valueValues = valueIndexFieldData.load(context).getDoubleValues();
        }
    }

    public static class Aggregator extends  GeoDistanceFacetExecutor.Aggregator {

        DoubleValues valueValues;

        public Aggregator(GeoDistance.FixedSourceDistance fixedSourceDistance, GeoDistanceFacet.Entry[] entries) {
            super(fixedSourceDistance, entries);
        }

        
        @Override
        protected void collectGeoPoint(GeoDistanceFacet.Entry entry, int docId, double distance) {
            entry.foundInDoc = true;
            entry.count++;
            DoubleValues.Iter iter = valueValues.getIter(docId);
            while(iter.hasNext()) {
                double value = iter.next();
                entry.totalCount++;
                entry.total += value;
                if (value < entry.min) {
                    entry.min = value;
                }
                if (value > entry.max) {
                    entry.max = value;
                }
            }
        }
       
    }
}
