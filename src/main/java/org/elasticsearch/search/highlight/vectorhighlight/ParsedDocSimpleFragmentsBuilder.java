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

package org.elasticsearch.search.highlight.vectorhighlight;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.SimpleFragmentsBuilder;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

/**
 *
 */
public class ParsedDocSimpleFragmentsBuilder extends SimpleFragmentsBuilder {

    private final FieldMapper<?> mapper;
    private final SearchContext searchContext;
    private Document parsedDoc;

    public ParsedDocSimpleFragmentsBuilder(FieldMapper<?> mapper, SearchContext searchContext,
                                           String[] preTags, String[] postTags, BoundaryScanner boundaryScanner, Document parsedDoc) {
        super(preTags, postTags, boundaryScanner);
        this.mapper = mapper;
        this.searchContext = searchContext;
        this.parsedDoc = parsedDoc;
    }

    public static final Field[] EMPTY_FIELDS = new Field[0];

    @Override
    protected Field[] getFields(IndexReader reader, int docId, String fieldName) throws IOException {
        final Field[] fields;
        String[] values = parsedDoc.getValues(fieldName);
        if (values == null || values.length == 0) {
            String value = parsedDoc.get(fieldName);
            if (value == null) return EMPTY_FIELDS;

            fields = new Field[1];
            fields[0] = new Field(mapper.names().indexName(), value, TextField.TYPE_NOT_STORED);
            return fields;
        }

        fields = new Field[values.length];
        for (int i = 0; i < values.length; i++) {
            fields[i] = new Field(mapper.names().indexName(), values[i], TextField.TYPE_NOT_STORED);
        }
        return fields;
    }

//    protected String makeFragment( StringBuilder buffer, int[] index, Field[] values, WeightedFragInfo fragInfo,
//            String[] preTags, String[] postTags, Encoder encoder ){
//        return super.makeFragment(buffer, index, values, FragmentBuilderHelper.fixWeightedFragInfo(mapper, values, fragInfo), preTags, postTags, encoder);
//   }

}
