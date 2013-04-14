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

package org.elasticsearch.test.unit.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.bloom.BloomFilteringPostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.codecs.lucene41.Lucene41Codec;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.codecs.memory.DirectPostingsFormat;
import org.apache.lucene.codecs.memory.MemoryPostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.codecs.pulsing.Pulsing41PostingsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.codec.CodecModule;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.codec.PerFieldMappingPostingFormatCodec;
import org.elasticsearch.index.codec.postingsformat.*;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperServiceModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityModule;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 */
public class CodecTests {

    @Test
    public void testResolveDefaultCodecs() throws Exception {
        CodecService codecService = createCodecService();
        assertThat(codecService.codec("default"), instanceOf(PerFieldMappingPostingFormatCodec.class));
        assertThat(codecService.codec("Lucene40"), instanceOf(Lucene40Codec.class));
        assertThat(codecService.codec("Lucene41"), instanceOf(Lucene41Codec.class));
        assertThat(codecService.codec("SimpleText"), instanceOf(SimpleTextCodec.class));
    }

    @Test
    public void testResolveDefaultPostingFormats() throws Exception {
        PostingsFormatService postingsFormatService = createCodecService().postingsFormatService();
        assertThat(postingsFormatService.get("default"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("default").get(), instanceOf(ElasticSearch090PostingsFormat.class));

        // Should fail when upgrading Lucene with codec changes
        assertThat(((ElasticSearch090PostingsFormat)postingsFormatService.get("default").get()).getDefaultWrapped(), instanceOf(((PerFieldPostingsFormat) Codec.getDefault().postingsFormat()).getPostingsFormatForField(null).getClass()));
        assertThat(postingsFormatService.get("Lucene41"), instanceOf(PreBuiltPostingsFormatProvider.class));
        // Should fail when upgrading Lucene with codec changes
        assertThat(postingsFormatService.get("Lucene41").get(), instanceOf(((PerFieldPostingsFormat) Codec.getDefault().postingsFormat()).getPostingsFormatForField(null).getClass()));

        assertThat(postingsFormatService.get("bloom_default"), instanceOf(PreBuiltPostingsFormatProvider.class));
        if (PostingFormats.luceneBloomFilter) {
            assertThat(postingsFormatService.get("bloom_default").get(), instanceOf(BloomFilteringPostingsFormat.class));
        } else {
            assertThat(postingsFormatService.get("bloom_default").get(), instanceOf(BloomFilterPostingsFormat.class));
        }
        assertThat(postingsFormatService.get("BloomFilter"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("BloomFilter").get(), instanceOf(BloomFilteringPostingsFormat.class));

        assertThat(postingsFormatService.get("XBloomFilter"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("XBloomFilter").get(), instanceOf(BloomFilterPostingsFormat.class));

        if (PostingFormats.luceneBloomFilter) {
            assertThat(postingsFormatService.get("bloom_pulsing").get(), instanceOf(BloomFilteringPostingsFormat.class));
        } else {
            assertThat(postingsFormatService.get("bloom_pulsing").get(), instanceOf(BloomFilterPostingsFormat.class));
        }

        assertThat(postingsFormatService.get("pulsing"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("pulsing").get(), instanceOf(Pulsing41PostingsFormat.class));
        assertThat(postingsFormatService.get("Pulsing41"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("Pulsing41").get(), instanceOf(Pulsing41PostingsFormat.class));

        assertThat(postingsFormatService.get("memory"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("memory").get(), instanceOf(MemoryPostingsFormat.class));
        assertThat(postingsFormatService.get("Memory"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("Memory").get(), instanceOf(MemoryPostingsFormat.class));

        assertThat(postingsFormatService.get("direct"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("direct").get(), instanceOf(DirectPostingsFormat.class));
        assertThat(postingsFormatService.get("Direct"), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(postingsFormatService.get("Direct").get(), instanceOf(DirectPostingsFormat.class));
    }

    @Test
    public void testResolvePostingFormatsFromMapping_default() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "default").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "default")
                .put("index.codec.postings_format.my_format1.min_block_size", 16)
                .put("index.codec.postings_format.my_format1.max_block_size", 64)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(ElasticSearch090PostingsFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(DefaultPostingsFormatProvider.class));
        DefaultPostingsFormatProvider provider = (DefaultPostingsFormatProvider) documentMapper.mappers().name("field2").mapper().postingsFormatProvider();
        assertThat(provider.minBlockSize(), equalTo(16));
        assertThat(provider.maxBlockSize(), equalTo(64));
    }

    @Test
    public void testResolvePostingFormatsFromMapping_memory() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "memory").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "memory")
                .put("index.codec.postings_format.my_format1.pack_fst", true)
                .put("index.codec.postings_format.my_format1.acceptable_overhead_ratio", 0.3f)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(MemoryPostingsFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(MemoryPostingsFormatProvider.class));
        MemoryPostingsFormatProvider provider = (MemoryPostingsFormatProvider) documentMapper.mappers().name("field2").mapper().postingsFormatProvider();
        assertThat(provider.packFst(), equalTo(true));
        assertThat(provider.acceptableOverheadRatio(), equalTo(0.3f));
    }

    @Test
    public void testResolvePostingFormatsFromMapping_direct() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "direct").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "direct")
                .put("index.codec.postings_format.my_format1.min_skip_count", 16)
                .put("index.codec.postings_format.my_format1.low_freq_cutoff", 64)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(DirectPostingsFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(DirectPostingsFormatProvider.class));
        DirectPostingsFormatProvider provider = (DirectPostingsFormatProvider) documentMapper.mappers().name("field2").mapper().postingsFormatProvider();
        assertThat(provider.minSkipCount(), equalTo(16));
        assertThat(provider.lowFreqCutoff(), equalTo(64));
    }

    @Test
    public void testResolvePostingFormatsFromMapping_pulsing() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "pulsing").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "pulsing")
                .put("index.codec.postings_format.my_format1.freq_cut_off", 2)
                .put("index.codec.postings_format.my_format1.min_block_size", 32)
                .put("index.codec.postings_format.my_format1.max_block_size", 64)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(Pulsing41PostingsFormat.class));

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(PulsingPostingsFormatProvider.class));
        PulsingPostingsFormatProvider provider = (PulsingPostingsFormatProvider) documentMapper.mappers().name("field2").mapper().postingsFormatProvider();
        assertThat(provider.freqCutOff(), equalTo(2));
        assertThat(provider.minBlockSize(), equalTo(32));
        assertThat(provider.maxBlockSize(), equalTo(64));
    }

    @Test
    public void testResolvePostingFormatsFromMappingLuceneBloom() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field1").field("type", "string").field("postings_format", "bloom_default").endObject()
                .startObject("field2").field("type", "string").field("postings_format", "bloom_pulsing").endObject()
                .startObject("field3").field("type", "string").field("postings_format", "my_format1").endObject()
                .endObject()
                .endObject().endObject().string();

        Settings indexSettings = ImmutableSettings.settingsBuilder()
                .put("index.codec.postings_format.my_format1.type", "bloom_filter_lucene")
                .put("index.codec.postings_format.my_format1.desired_max_saturation", 0.2f)
                .put("index.codec.postings_format.my_format1.saturation_limit", 0.8f)
                .put("index.codec.postings_format.my_format1.delegate", "delegate1")
                .put("index.codec.postings_format.delegate1.type", "direct")
                .put("index.codec.postings_format.delegate1.min_skip_count", 16)
                .put("index.codec.postings_format.delegate1.low_freq_cutoff", 64)
                .build();
        CodecService codecService = createCodecService(indexSettings);
        DocumentMapper documentMapper = codecService.mapperService().documentMapperParser().parse(mapping);
        assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        if (PostingFormats.luceneBloomFilter) {
            assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(BloomFilteringPostingsFormat.class));
        } else {
            assertThat(documentMapper.mappers().name("field1").mapper().postingsFormatProvider().get(), instanceOf(BloomFilterPostingsFormat.class));
        }

        assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider(), instanceOf(PreBuiltPostingsFormatProvider.class));
        if (PostingFormats.luceneBloomFilter) {
            assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider().get(), instanceOf(BloomFilteringPostingsFormat.class));
        } else {
            assertThat(documentMapper.mappers().name("field2").mapper().postingsFormatProvider().get(), instanceOf(BloomFilterPostingsFormat.class));
        }

        assertThat(documentMapper.mappers().name("field3").mapper().postingsFormatProvider(), instanceOf(BloomFilterLucenePostingsFormatProvider.class));
        BloomFilterLucenePostingsFormatProvider provider = (BloomFilterLucenePostingsFormatProvider) documentMapper.mappers().name("field3").mapper().postingsFormatProvider();
        assertThat(provider.desiredMaxSaturation(), equalTo(0.2f));
        assertThat(provider.saturationLimit(), equalTo(0.8f));
        assertThat(provider.delegate(), instanceOf(DirectPostingsFormatProvider.class));
        DirectPostingsFormatProvider delegate = (DirectPostingsFormatProvider) provider.delegate();
        assertThat(delegate.minSkipCount(), equalTo(16));
        assertThat(delegate.lowFreqCutoff(), equalTo(64));
    }

    private static CodecService createCodecService() {
        return createCodecService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    private static CodecService createCodecService(Settings settings) {
        Index index = new Index("test");
        Injector injector = new ModulesBuilder()
                .add(new SettingsModule(settings))
                .add(new IndexNameModule(index))
                .add(new IndexSettingsModule(index, settings))
                .add(new SimilarityModule(settings))
                .add(new CodecModule(settings))
                .add(new MapperServiceModule())
                .add(new AnalysisModule(settings))
                .createInjector();
        return injector.getInstance(CodecService.class);
    }

}
