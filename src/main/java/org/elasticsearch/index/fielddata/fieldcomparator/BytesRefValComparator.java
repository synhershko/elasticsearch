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

package org.elasticsearch.index.fielddata.fieldcomparator;

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;

/**
 * Sorts by field's natural Term sort order.  All
 * comparisons are done using BytesRef.compareTo, which is
 * slow for medium to large result sets but possibly
 * very fast for very small results sets.
 */
public final class BytesRefValComparator extends FieldComparator<BytesRef> {

    private final IndexFieldData<?> indexFieldData;
    private final SortMode sortMode;

    private final BytesRef[] values;
    private BytesRef bottom;
    private BytesValues docTerms;

    BytesRefValComparator(IndexFieldData<?> indexFieldData, int numHits, SortMode sortMode) {
        this.sortMode = sortMode;
        values = new BytesRef[numHits];
        this.indexFieldData = indexFieldData;
    }

    @Override
    public int compare(int slot1, int slot2) {
        final BytesRef val1 = values[slot1];
        final BytesRef val2 = values[slot2];
        return compareValues(val1, val2);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        final BytesRef val2 = docTerms.getValue(doc);
        return compareValues(bottom, val2);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        if (values[slot] == null) {
            values[slot] = new BytesRef();
        }
        docTerms.getValueScratch(doc, values[slot]);
    }

    @Override
    public FieldComparator<BytesRef> setNextReader(AtomicReaderContext context) throws IOException {
        docTerms = indexFieldData.load(context).getBytesValues();
        if (docTerms.isMultiValued()) {
            docTerms = new MultiValuedBytesWrapper(docTerms, sortMode);
        }
        return this;
    }

    @Override
    public void setBottom(final int bottom) {
        this.bottom = values[bottom];
    }

    @Override
    public BytesRef value(int slot) {
        return values[slot];
    }

    @Override
    public int compareValues(BytesRef val1, BytesRef val2) {
        if (val1 == null) {
            if (val2 == null) {
                return 0;
            }
            return -1;
        } else if (val2 == null) {
            return 1;
        }
        return val1.compareTo(val2);
    }

    @Override
    public int compareDocToValue(int doc, BytesRef value) {
        return docTerms.getValue(doc).compareTo(value);
    }

    public static class FilteredByteValues extends BytesValues {

        protected final BytesValues delegate;

        public FilteredByteValues(BytesValues delegate) {
            super(delegate.isMultiValued());
            this.delegate = delegate;
        }

        public boolean hasValue(int docId) {
            return delegate.hasValue(docId);
        }

        public BytesRef makeSafe(BytesRef bytes) {
            return delegate.makeSafe(bytes);
        }

        public BytesRef getValueScratch(int docId, BytesRef ret) {
            return delegate.getValueScratch(docId, ret);
        }

        public Iter getIter(int docId) {
            return delegate.getIter(docId);
        }

    }

    private static final class MultiValuedBytesWrapper extends FilteredByteValues {

        private final SortMode sortMode;

        public MultiValuedBytesWrapper(BytesValues delegate, SortMode sortMode) {
            super(delegate);
            this.sortMode = sortMode;
        }

        @Override
        public BytesRef getValueScratch(int docId, BytesRef scratch) {
            BytesValues.Iter iter = delegate.getIter(docId);
            if (!iter.hasNext()) {
                return null;
            }

            BytesRef currentVal = iter.next();
            BytesRef relevantVal = currentVal;
            while (true) {
                int cmp = currentVal.compareTo(relevantVal);
                if (sortMode == SortMode.MAX) {
                    if (cmp > 0) {
                        relevantVal = currentVal;
                    }
                } else {
                    if (cmp < 0) {
                        relevantVal = currentVal;
                    }
                }
                if (!iter.hasNext()) {
                    break;
                }
                currentVal = iter.next();
            }
            return relevantVal;
        }

    }

}
