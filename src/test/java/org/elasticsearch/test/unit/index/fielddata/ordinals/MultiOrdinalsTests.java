/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.elasticsearch.test.unit.index.fielddata.ordinals;

import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.fielddata.ordinals.Ordinals;
import org.elasticsearch.index.fielddata.ordinals.OrdinalsBuilder;
import org.testng.annotations.Test;

import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 */
public abstract class MultiOrdinalsTests {

    protected final Ordinals creationMultiOrdinals(OrdinalsBuilder builder) {
        return this.creationMultiOrdinals(builder, ImmutableSettings.builder());
    }


    protected abstract Ordinals creationMultiOrdinals(OrdinalsBuilder builder, ImmutableSettings.Builder settings);

    @Test
    public void testRandomValues() {
        Random random = new Random(100);
        int numDocs = 100 + random.nextInt(1000);
        int numOrdinals = 1 + random.nextInt(200);
        int numValues = 100 + random.nextInt(100000);
        OrdinalsBuilder builder = new OrdinalsBuilder(numDocs);
        Set<OrdAndId> ordsAndIdSet = new HashSet<OrdAndId>();
        for (int i = 0; i < numValues; i++) {
            ordsAndIdSet.add(new OrdAndId(1 + random.nextInt(numOrdinals), random.nextInt(numDocs)));
        }
        List<OrdAndId> ordsAndIds = new ArrayList<OrdAndId>(ordsAndIdSet);
        Collections.sort(ordsAndIds, new Comparator<OrdAndId>() {

            @Override
            public int compare(OrdAndId o1, OrdAndId o2) {
                if (o1.ord < o2.ord) {
                    return -1;
                }
                if (o1.ord == o2.ord) {
                    if (o1.id < o2.id) {
                        return -1;
                    }
                    if (o1.id > o2.id) {
                        return 1;
                    }
                    return 0;
                }
                return 1;
            }
        });
        int lastOrd = -1;
        for (OrdAndId ordAndId : ordsAndIds) {
            if (lastOrd != ordAndId.ord) {
                lastOrd = ordAndId.ord;
                builder.nextOrdinal();
            }
            builder.addDoc(ordAndId.id);
        }

        Collections.sort(ordsAndIds, new Comparator<OrdAndId>() {

            @Override
            public int compare(OrdAndId o1, OrdAndId o2) {
                if (o1.id < o2.id) {
                    return -1;
                }
                if (o1.id == o2.id) {
                    if (o1.ord < o2.ord) {
                        return -1;
                    }
                    if (o1.ord > o2.ord) {
                        return 1;
                    }
                    return 0;
                }
                return 1;
            }
        });
        Ordinals ords = creationMultiOrdinals(builder);
        Ordinals.Docs docs = ords.ordinals();
        int docId = ordsAndIds.get(0).id;
        List<Integer> docOrds = new ArrayList<Integer>();
        for (OrdAndId ordAndId : ordsAndIds) {
            if (docId == ordAndId.id) {
                docOrds.add(ordAndId.ord);
            } else {
                if (!docOrds.isEmpty()) {
                    assertThat(docs.getOrd(docId), equalTo(docOrds.get(0)));
                    IntsRef ref = docs.getOrds(docId);
                    assertThat(ref.offset, equalTo(0));

                    for (int i = ref.offset; i < ref.length; i++) {
                        assertThat(ref.ints[i], equalTo(docOrds.get(i)));
                    }
                    final int[] array = new int[docOrds.size()];
                    for (int i = 0; i < array.length; i++) {
                        array[i] = docOrds.get(i);
                    }
                    assertIter(docs.getIter(docId), array);
                }
                for (int i = docId + 1; i < ordAndId.id; i++) {
                    assertThat(docs.getOrd(i), equalTo(0));
                }
                docId = ordAndId.id;
                docOrds.clear();
                docOrds.add(ordAndId.ord);

            }
        }

    }

    public static class OrdAndId {
        final int ord;
        final int id;

        public OrdAndId(int ord, int id) {
            this.ord = ord;
            this.id = id;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + id;
            result = prime * result + ord;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            OrdAndId other = (OrdAndId) obj;
            if (id != other.id)
                return false;
            if (ord != other.ord)
                return false;
            return true;
        }
    }

    @Test
    public void testOrdinals() throws Exception {
        int maxDoc = 7;
        int maxOrds = 32;
        OrdinalsBuilder builder = new OrdinalsBuilder(maxDoc);
        builder.nextOrdinal(); // 1
        builder.addDoc(1).addDoc(4).addDoc(5).addDoc(6);
        builder.nextOrdinal(); // 2
        builder.addDoc(0).addDoc(5).addDoc(6);
        builder.nextOrdinal(); // 3
        builder.addDoc(2).addDoc(4).addDoc(5).addDoc(6);
        builder.nextOrdinal(); // 4
        builder.addDoc(0).addDoc(4).addDoc(5).addDoc(6);
        builder.nextOrdinal(); // 5
        builder.addDoc(4).addDoc(5).addDoc(6);
        int ord = builder.nextOrdinal(); // 6
        builder.addDoc(4).addDoc(5).addDoc(6);
        for (int i = ord; i < maxOrds; i++) {
            builder.nextOrdinal();
            builder.addDoc(5).addDoc(6);
        }


        Ordinals ordinals = creationMultiOrdinals(builder);
        Ordinals.Docs docs = ordinals.ordinals();
        assertThat(docs.getNumDocs(), equalTo(maxDoc));
        assertThat(docs.getNumOrds(), equalTo(maxOrds));
        assertThat(docs.getMaxOrd(), equalTo(maxOrds + 1)); // Includes null ord
        assertThat(docs.isMultiValued(), equalTo(true));
        assertThat(ordinals.getMemorySizeInBytes(), greaterThan(0l));

        // Document 1
        assertThat(docs.getOrd(0), equalTo(2));
        IntsRef ref = docs.getOrds(0);
        assertThat(ref.offset, equalTo(0));
        assertThat(ref.ints[0], equalTo(2));
        assertThat(ref.ints[1], equalTo(4));
        assertThat(ref.length, equalTo(2));
        assertIter(docs.getIter(0), 2, 4);

        // Document 2
        assertThat(docs.getOrd(1), equalTo(1));
        ref = docs.getOrds(1);
        assertThat(ref.offset, equalTo(0));
        assertThat(ref.ints[0], equalTo(1));
        assertThat(ref.length, equalTo(1));
        assertIter(docs.getIter(1), 1);

        // Document 3
        assertThat(docs.getOrd(2), equalTo(3));
        ref = docs.getOrds(2);
        assertThat(ref.offset, equalTo(0));
        assertThat(ref.ints[0], equalTo(3));
        assertThat(ref.length, equalTo(1));
        assertIter(docs.getIter(2), 3);

        // Document 4
        assertThat(docs.getOrd(3), equalTo(0));
        ref = docs.getOrds(3);
        assertThat(ref.offset, equalTo(0));
        assertThat(ref.length, equalTo(0));
        assertIter(docs.getIter(3));

        // Document 5
        assertThat(docs.getOrd(4), equalTo(1));
        ref = docs.getOrds(4);
        assertThat(ref.offset, equalTo(0));
        assertThat(ref.ints[0], equalTo(1));
        assertThat(ref.ints[1], equalTo(3));
        assertThat(ref.ints[2], equalTo(4));
        assertThat(ref.ints[3], equalTo(5));
        assertThat(ref.ints[4], equalTo(6));
        assertThat(ref.length, equalTo(5));
        assertIter(docs.getIter(4), 1, 3, 4, 5, 6);

        // Document 6
        assertThat(docs.getOrd(5), equalTo(1));
        ref = docs.getOrds(5);
        assertThat(ref.offset, equalTo(0));
        int[] expectedOrds = new int[maxOrds];
        for (int i = 0; i < maxOrds; i++) {
            expectedOrds[i] = i + 1;
            assertThat(ref.ints[i], equalTo(i + 1));
        }
        assertIter(docs.getIter(5), expectedOrds);
        assertThat(ref.length, equalTo(maxOrds));

        // Document 7
        assertThat(docs.getOrd(6), equalTo(1));
        ref = docs.getOrds(6);
        assertThat(ref.offset, equalTo(0));
        expectedOrds = new int[maxOrds];
        for (int i = 0; i < maxOrds; i++) {
            expectedOrds[i] = i + 1;
            assertThat(ref.ints[i], equalTo(i + 1));
        }
        assertIter(docs.getIter(6), expectedOrds);
        assertThat(ref.length, equalTo(maxOrds));
    }

    protected static void assertIter(Ordinals.Docs.Iter iter, int... expectedOrdinals) {
        for (int expectedOrdinal : expectedOrdinals) {
            assertThat(iter.next(), equalTo(expectedOrdinal));
        }
        assertThat(iter.next(), equalTo(0)); // Last one should always be 0
        assertThat(iter.next(), equalTo(0)); // Just checking it stays 0
    }

}
