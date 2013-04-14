/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.BytesValues.Iter;

/**
 */
public abstract class AtomicGeoPointFieldData<Script extends ScriptDocValues> implements AtomicFieldData<Script> {

    public abstract GeoPointValues getGeoPointValues();

    @Override
    public BytesValues getBytesValues() {
        final GeoPointValues values = getGeoPointValues();
        return new BytesValues(values.isMultiValued()) {

            @Override
            public boolean hasValue(int docId) {
                return values.hasValue(docId);
            }

            @Override
            public BytesRef getValueScratch(int docId, BytesRef ret) {
                GeoPoint value = values.getValue(docId);
                if (value != null) {
                    ret.copyChars(GeoHashUtils.encode(value.lat(), value.lon()));
                } else {
                    ret.length = 0;
                }
                return ret;
            }

            @Override
            public Iter getIter(int docId) {
                final GeoPointValues.Iter iter = values.getIter(docId);
                return new BytesValues.Iter() {
                    private final BytesRef spare = new BytesRef();

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public BytesRef next() {
                        GeoPoint value  = iter.next();
                        spare.copyChars(GeoHashUtils.encode(value.lat(), value.lon()));
                        return spare;
                    }

                    @Override
                    public int hash() {
                        return spare.hashCode();
                    }

                };
            }
        };
    }

    @Override
    public BytesValues getHashedBytesValues() {
        return getBytesValues();
    }

}
