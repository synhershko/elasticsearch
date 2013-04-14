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

package org.elasticsearch.index.fielddata;

import gnu.trove.iterator.TObjectLongIterator;
import gnu.trove.map.hash.TObjectLongHashMap;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 */
public class FieldDataStats implements Streamable, ToXContent {

    long memorySize;
    long evictions;
    @Nullable
    TObjectLongHashMap<String> fields;

    public FieldDataStats() {

    }

    public FieldDataStats(long memorySize, long evictions, @Nullable TObjectLongHashMap<String> fields) {
        this.memorySize = memorySize;
        this.evictions = evictions;
        this.fields = fields;
    }

    public void add(FieldDataStats stats) {
        this.memorySize += stats.memorySize;
        this.evictions += stats.evictions;
        if (stats.fields != null) {
            if (fields == null) fields = new TObjectLongHashMap<String>();
            for (TObjectLongIterator<String> it = stats.fields.iterator(); it.hasNext(); ) {
                it.advance();
                fields.adjustOrPutValue(it.key(), it.value(), it.value());
            }
        }
    }

    public long getMemorySizeInBytes() {
        return this.memorySize;
    }

    public ByteSizeValue getMemorySize() {
        return new ByteSizeValue(memorySize);
    }

    public long getEvictions() {
        return this.evictions;
    }

    @Nullable
    public TObjectLongHashMap<String> getFields() {
        return fields;
    }

    public static FieldDataStats readFieldDataStats(StreamInput in) throws IOException {
        FieldDataStats stats = new FieldDataStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        memorySize = in.readVLong();
        evictions = in.readVLong();
        if (in.readBoolean()) {
            int size = in.readVInt();
            fields = new TObjectLongHashMap<String>(size);
            for (int i = 0; i < size; i++) {
                fields.put(in.readString(), in.readVLong());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(memorySize);
        out.writeVLong(evictions);
        if (fields == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(fields.size());
            for (TObjectLongIterator<String> it = fields.iterator(); it.hasNext(); ) {
                it.advance();
                out.writeString(it.key());
                out.writeVLong(it.value());
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.FIELDDATA);
        builder.field(Fields.MEMORY_SIZE, getMemorySize().toString());
        builder.field(Fields.MEMORY_SIZE_IN_BYTES, memorySize);
        builder.field(Fields.EVICTIONS, getEvictions());
        if (fields != null) {
            builder.startObject(Fields.FIELDS);
            for (TObjectLongIterator<String> it = fields.iterator(); it.hasNext(); ) {
                it.advance();
                builder.startObject(it.key(), XContentBuilder.FieldCaseConversion.NONE);
                builder.field(Fields.MEMORY_SIZE, new ByteSizeValue(it.value()).toString());
                builder.field(Fields.MEMORY_SIZE_IN_BYTES, it.value());
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString FIELDDATA = new XContentBuilderString("fielddata");
        static final XContentBuilderString MEMORY_SIZE = new XContentBuilderString("memory_size");
        static final XContentBuilderString MEMORY_SIZE_IN_BYTES = new XContentBuilderString("memory_size_in_bytes");
        static final XContentBuilderString EVICTIONS = new XContentBuilderString("evictions");
        static final XContentBuilderString FIELDS = new XContentBuilderString("fields");
    }
}
