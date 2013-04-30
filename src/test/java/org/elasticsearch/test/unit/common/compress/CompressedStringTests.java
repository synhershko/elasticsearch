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

package org.elasticsearch.test.unit.common.compress;

import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 *
 */
public class CompressedStringTests {

    @Test
    public void simpleTestsLZF() throws IOException {
        simpleTests("lzf");
    }

    public void simpleTests(String compressor) throws IOException {
        CompressorFactory.configure(ImmutableSettings.settingsBuilder().put("compress.default.type", compressor).build());
        String str = "this is a simple string";
        CompressedString cstr = new CompressedString(str);
        assertThat(cstr.string(), equalTo(str));
        assertThat(new CompressedString(str), equalTo(cstr));

        String str2 = "this is a simple string 2";
        CompressedString cstr2 = new CompressedString(str2);
        assertThat(cstr2.string(), not(equalTo(str)));
        assertThat(new CompressedString(str2), not(equalTo(cstr)));
        assertThat(new CompressedString(str2), equalTo(cstr2));
    }
}
