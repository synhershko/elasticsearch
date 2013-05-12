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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 *
 */
public class BulkItemRequest implements Streamable {

    private int id;

    private ActionRequest request;

    BulkItemRequest() {

    }

    public BulkItemRequest(int id, ActionRequest request) {
        this.id = id;
        this.request = request;
    }

    public int id() {
        return id;
    }

    public ActionRequest request() {
        return request;
    }

    public static BulkItemRequest readBulkItem(StreamInput in) throws IOException {
        BulkItemRequest item = new BulkItemRequest();
        item.readFrom(in);
        return item;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readVInt();
        byte type = in.readByte();
        if (type == 0) {
            request = new IndexRequest();
        } else if (type == 1) {
            request = new DeleteRequest();
        } else if (type == 2) {
            request = new UpdateRequest();
        }
        request.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        if (request instanceof IndexRequest) {
            out.writeByte((byte) 0);
        } else if (request instanceof DeleteRequest) {
            out.writeByte((byte) 1);
        } else if (request instanceof UpdateRequest) {
            out.writeByte((byte) 2);
        }
        request.writeTo(out);
    }
}
