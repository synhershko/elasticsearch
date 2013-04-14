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

package org.elasticsearch.action.suggest;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.suggest.Suggest;

/**
 * The response of the suggest action.
 */
public final class SuggestResponse extends BroadcastOperationResponse {

    private final Suggest suggest;

    SuggestResponse(Suggest suggest) {
        this.suggest = suggest;
    }

    SuggestResponse(Suggest suggest, int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.suggest = suggest;
    }

    /**
     * The Suggestions of the phrase.
     */
    public Suggest getSuggest() {
        return suggest;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.suggest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.suggest.writeTo(out);
    }
    
    @Override
    public String toString() {
        String source; 
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            suggest.toXContent(builder, null);
            source = XContentHelper.convertToJson(builder.bytes(), true);
        } catch (IOException e) {
            source = "Error: " + e.getMessage();
        }
        return "Suggest Response["+source+"]";
    }
}
