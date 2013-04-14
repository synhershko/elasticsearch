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
package org.elasticsearch.search.suggest;

import java.io.IOException;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.suggest.SuggestionSearchContext.SuggestionContext;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestParser;
import org.elasticsearch.search.suggest.term.TermSuggestParser;

/**
 *
 */
public class SuggestParseElement implements SearchParseElement {
    private final SuggestContextParser termSuggestParser = new TermSuggestParser();
    private final SuggestContextParser phraseSuggestParser = new PhraseSuggestParser();
    
    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        SuggestionSearchContext suggestionSearchContext = parseInternal(parser, context.mapperService());
        context.suggest(suggestionSearchContext);
    }

    public SuggestionSearchContext parseInternal(XContentParser parser, MapperService mapperService) throws IOException {
        SuggestionSearchContext suggestionSearchContext = new SuggestionSearchContext();
        BytesRef globalText = null;
        String fieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("text".equals(fieldName)) {
                    globalText = parser.bytes();
                } else {
                    throw new ElasticSearchIllegalArgumentException("[suggest] does not support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                String suggestionName = fieldName;
                BytesRef suggestText = null;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("text".equals(fieldName)) {
                            suggestText = parser.bytes();
                        } else {
                            throw new ElasticSearchIllegalArgumentException("[suggest] does not support [" + fieldName + "]");
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (suggestionName == null) {
                            throw new ElasticSearchIllegalArgumentException("Suggestion must have name");
                        }
                        final SuggestContextParser contextParser;
                        if ("term".equals(fieldName)) {
                            contextParser = termSuggestParser;
                        } else if ("phrase".equals(fieldName)) {
                            contextParser = phraseSuggestParser;
                        } else {
                            throw new ElasticSearchIllegalArgumentException("Suggester[" + fieldName + "] not supported");
                        }
                        parseAndVerify(parser, mapperService, suggestionSearchContext, globalText, suggestionName, suggestText, contextParser);
                        
                    }
                }
            }
        }
        return suggestionSearchContext;
    }

    public void parseAndVerify(XContentParser parser, MapperService mapperService, SuggestionSearchContext suggestionSearchContext,
            BytesRef globalText, String suggestionName, BytesRef suggestText, SuggestContextParser suggestParser ) throws IOException {
        SuggestionContext suggestion = suggestParser.parse(parser, mapperService);
        suggestion.setText(suggestText);
        SuggestUtils.verifySuggestion(mapperService, globalText, suggestion);
        suggestionSearchContext.addSuggestion(suggestionName, suggestion);
    }

   
}
