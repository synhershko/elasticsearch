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

package org.elasticsearch.index.mapper;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.lucene.BytesRefs;

/**
 *
 */
public final class Uid {

    public static final char DELIMITER = '#';
    public static final byte DELIMITER_BYTE = 0x23;
    public static final BytesRef DELIMITER_BYTES = new BytesRef(new byte[]{DELIMITER_BYTE});

    private final String type;

    private final String id;

    public Uid(String type, String id) {
        this.type = type;
        this.id = id;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Uid uid = (Uid) o;

        if (id != null ? !id.equals(uid.id) : uid.id != null) return false;
        if (type != null ? !type.equals(uid.type) : uid.type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return createUid(type, id);
    }

    public BytesRef toBytesRef() {
        return createUidAsBytes(type, id);
    }

    public static String typePrefix(String type) {
        return type + DELIMITER;
    }

    public static BytesRef typePrefixAsBytes(BytesRef type) {
        BytesRef bytesRef = new BytesRef(type.length + 1);
        bytesRef.append(type);
        bytesRef.append(DELIMITER_BYTES);
        return bytesRef;
    }

    public static String idFromUid(String uid) {
        int delimiterIndex = uid.indexOf(DELIMITER); // type is not allowed to have # in it..., ids can
        return uid.substring(delimiterIndex + 1);
    }

    public static HashedBytesArray idFromUid(BytesRef uid) {
        return splitUidIntoTypeAndId(uid)[1];
    }

    public static HashedBytesArray typeFromUid(BytesRef uid) {
        return splitUidIntoTypeAndId(uid)[0];
    }

    public static String typeFromUid(String uid) {
        int delimiterIndex = uid.indexOf(DELIMITER); // type is not allowed to have # in it..., ids can
        return uid.substring(0, delimiterIndex);
    }

    public static Uid createUid(String uid) {
        int delimiterIndex = uid.indexOf(DELIMITER); // type is not allowed to have # in it..., ids can
        return new Uid(uid.substring(0, delimiterIndex), uid.substring(delimiterIndex + 1));
    }

    public static BytesRef createUidAsBytes(String type, String id) {
        return createUidAsBytes(new BytesRef(type), new BytesRef(id));
    }

    public static BytesRef createUidAsBytes(String type, BytesRef id) {
        return createUidAsBytes(new BytesRef(type), id);
    }

    public static BytesRef createUidAsBytes(BytesRef type, BytesRef id) {
        final BytesRef ref = new BytesRef(type.length + 1 + id.length);
        System.arraycopy(type.bytes, type.offset, ref.bytes, 0, type.length);
        ref.offset = type.length;
        ref.bytes[ref.offset++] = DELIMITER_BYTE;
        System.arraycopy(id.bytes, id.offset, ref.bytes, ref.offset, id.length);
        ref.offset = 0;
        ref.length = ref.bytes.length;
        return ref;
    }

    public static BytesRef[] createTypeUids(Collection<String> types, Object ids) {
        return createTypeUids(types, Collections.singletonList(ids));
    }

    public static BytesRef[] createTypeUids(Collection<String> types, List<? extends Object> ids) {
        final int numIds = ids.size();
        BytesRef[] uids = new BytesRef[types.size() * ids.size()];
        BytesRef typeBytes = new BytesRef();
        BytesRef idBytes = new BytesRef();
        int index = 0;
        for (String type : types) {
            UnicodeUtil.UTF16toUTF8(type, 0, type.length(), typeBytes);
            for (int i = 0; i < numIds; i++, index++) {
                uids[index] = Uid.createUidAsBytes(typeBytes, BytesRefs.toBytesRef(ids.get(i), idBytes));
            }
        }
        return uids;
    }

    public static String createUid(String type, String id) {
        return createUid(new StringBuilder(), type, id);
    }

    public static String createUid(StringBuilder sb, String type, String id) {
        return sb.append(type).append(DELIMITER).append(id).toString();
    }

    public static boolean hasDelimiter(BytesRef uid) {
        final int limit = uid.offset + uid.length;
        for (int i = uid.offset; i < limit; i++) {
            if (uid.bytes[i] == DELIMITER_BYTE) { // 0x23 is equal to '#'
                return true;
            }
        }
        return false;
    }

    // LUCENE 4 UPGRADE: HashedBytesArray or BytesRef as return type?
    public static HashedBytesArray[] splitUidIntoTypeAndId(BytesRef uid) {
        int loc = -1;
        final int limit = uid.offset + uid.length;
        for (int i = uid.offset; i < limit; i++) {
            if (uid.bytes[i] == DELIMITER_BYTE) { // 0x23 is equal to '#'
                loc = i;
                break;
            }
        }

        if (loc == -1) {
            return null;
        }

        byte[] type = new byte[loc - uid.offset];
        System.arraycopy(uid.bytes, uid.offset, type, 0, type.length);

        byte[] id = new byte[uid.length - type.length - 1];
        System.arraycopy(uid.bytes, loc + 1, id, 0, id.length);
        return new HashedBytesArray[]{new HashedBytesArray(type), new HashedBytesArray(id)};
    }

}
