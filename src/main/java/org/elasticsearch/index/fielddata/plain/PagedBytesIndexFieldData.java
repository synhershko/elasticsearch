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

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

/**
 */
public class PagedBytesIndexFieldData extends AbstractBytesIndexFieldData<PagedBytesAtomicFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData<PagedBytesAtomicFieldData> build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            return new PagedBytesIndexFieldData(index, indexSettings, fieldNames, type, cache);
        }
    }

    public PagedBytesIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public PagedBytesAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();

        Terms terms = reader.terms(getFieldNames().indexName());
        if (terms == null) {
            return PagedBytesAtomicFieldData.empty(reader.maxDoc());
        }

        final PagedBytes bytes = new PagedBytes(15);
        int startBytesBPV;
        int startNumUniqueTerms;

        int maxDoc = reader.maxDoc();
        final int termCountHardLimit;
        if (maxDoc == Integer.MAX_VALUE) {
            termCountHardLimit = Integer.MAX_VALUE;
        } else {
            termCountHardLimit = maxDoc + 1;
        }

        // Try for coarse estimate for number of bits; this
        // should be an underestimate most of the time, which
        // is fine -- GrowableWriter will reallocate as needed
        long numUniqueTerms = terms.size();
        if (numUniqueTerms != -1L) {
            if (numUniqueTerms > termCountHardLimit) {
                // app is misusing the API (there is more than
                // one term per doc); in this case we make best
                // effort to load what we can (see LUCENE-2142)
                numUniqueTerms = termCountHardLimit;
            }

            startBytesBPV = PackedInts.bitsRequired(numUniqueTerms * 4);

            startNumUniqueTerms = (int) numUniqueTerms;
        } else {
            startBytesBPV = 1;
            startNumUniqueTerms = 1;
        }

        // TODO: expose this as an option..., have a nice parser for it...
        float acceptableOverheadRatio = PackedInts.FAST;

        GrowableWriter termOrdToBytesOffset = new GrowableWriter(startBytesBPV, 1 + startNumUniqueTerms, acceptableOverheadRatio);
        OrdinalsBuilder builder = new OrdinalsBuilder(terms, reader.maxDoc());
        try {
            // 0 is reserved for "unset"
            bytes.copyUsingLengthPrefix(new BytesRef());
            TermsEnum termsEnum = filter(terms, reader);
            DocsEnum docsEnum = null;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                final int termOrd = builder.nextOrdinal();
                if (termOrd == termOrdToBytesOffset.size()) {
                    // NOTE: this code only runs if the incoming
                    // reader impl doesn't implement
                    // size (which should be uncommon)
                    termOrdToBytesOffset = termOrdToBytesOffset.resize(ArrayUtil.oversize(1 + termOrd, 1));
                }
                termOrdToBytesOffset.set(termOrd, bytes.copyUsingLengthPrefix(term));
                docsEnum = termsEnum.docs(reader.getLiveDocs(), docsEnum, DocsEnum.FLAG_NONE);
                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    builder.addDoc(docId);
                }
            }
            final long sizePointer = bytes.getPointer();
            PagedBytes.Reader bytesReader = bytes.freeze(true);
            PackedInts.Reader termOrdToBytesOffsetReader = termOrdToBytesOffset.getMutable();
            final Ordinals ordinals = builder.build(fieldDataType.getSettings());

            return new PagedBytesAtomicFieldData(bytesReader, sizePointer, termOrdToBytesOffsetReader, ordinals);
        } finally {
            builder.close();
        }
    }
}
