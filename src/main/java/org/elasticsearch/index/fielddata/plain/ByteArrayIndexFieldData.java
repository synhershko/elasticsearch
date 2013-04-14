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

package org.elasticsearch.index.fielddata.plain;

import gnu.trove.list.array.TByteArrayList;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.ByteValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.Ordinals.Docs;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

/**
 */
public class ByteArrayIndexFieldData extends AbstractIndexFieldData<ByteArrayAtomicFieldData> implements IndexNumericFieldData<ByteArrayAtomicFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            return new ByteArrayIndexFieldData(index, indexSettings, fieldNames, type, cache);
        }
    }

    public ByteArrayIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public NumericType getNumericType() {
        return NumericType.BYTE;
    }

    @Override
    public boolean valuesOrdered() {
        // because we might have single values? we can dynamically update a flag to reflect that
        // based on the atomic field data loaded
        return false;
    }

    @Override
    public ByteArrayAtomicFieldData load(AtomicReaderContext context) {
        try {
            return cache.load(context, this);
        } catch (Throwable e) {
            if (e instanceof ElasticSearchException) {
                throw (ElasticSearchException) e;
            } else {
                throw new ElasticSearchException(e.getMessage(), e);
            }
        }
    }

    @Override
    public ByteArrayAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();
        Terms terms = reader.terms(getFieldNames().indexName());
        if (terms == null) {
            return ByteArrayAtomicFieldData.EMPTY;
        }
        // TODO: how can we guess the number of terms? numerics end up creating more terms per value...
        final TByteArrayList values = new TByteArrayList();

        values.add((byte) 0); // first "t" indicates null value
        OrdinalsBuilder builder = new OrdinalsBuilder(terms, reader.maxDoc());
        BytesRefIterator iter = builder.buildFromTerms(builder.wrapNumeric32Bit(terms.iterator(null)), reader.getLiveDocs());
        BytesRef term;
        while ((term = iter.next()) != null) {
            values.add((byte) NumericUtils.prefixCodedToInt(term));
        }
        try {
            Ordinals build = builder.build(fieldDataType.getSettings());
            return build(reader, fieldDataType, builder, build, new BuilderBytes() {
                @Override
                public byte get(int index) {
                    return values.get(index);
                }

                @Override
                public byte[] toArray() {
                    return values.toArray();
                }
            });
        } finally {
            builder.close();
        }
    }

    static interface BuilderBytes {
        byte get(int index);

        byte[] toArray();
    }

    static ByteArrayAtomicFieldData build(AtomicReader reader, FieldDataType fieldDataType, OrdinalsBuilder builder, Ordinals build, BuilderBytes values) {
        if (!build.isMultiValued() && CommonSettings.removeOrdsOnSingleValue(fieldDataType)) {
            Docs ordinals = build.ordinals();
            byte[] sValues = new byte[reader.maxDoc()];
            int maxDoc = reader.maxDoc();
            for (int i = 0; i < maxDoc; i++) {
                sValues[i] = values.get(ordinals.getOrd(i));
            }
            final FixedBitSet set = builder.buildDocsWithValuesSet();
            if (set == null) {
                return new ByteArrayAtomicFieldData.Single(sValues, reader.maxDoc());
            } else {
                return new ByteArrayAtomicFieldData.SingleFixedSet(sValues, reader.maxDoc(), set);
            }
        } else {
            return new ByteArrayAtomicFieldData.WithOrdinals(values.toArray(), reader.maxDoc(), build);
        }
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        return new ByteValuesComparatorSource(this, missingValue, sortMode);
    }
}
