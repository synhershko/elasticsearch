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
package org.elasticsearch.test.unit.index.mapper.geo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

import java.io.IOException;

import org.apache.lucene.spatial.prefix.PrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

public class GeoShapeFieldMapperTests {

    @Test
    public void testDefaultConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(GeoShapeFieldMapper.Defaults.DISTANCE_ERROR_PCT));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoShapeFieldMapper.Defaults.GEOHASH_LEVELS));
    }

    @Test
    public void testGeohashConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "geohash")
                    .field("tree_levels", "4")
                    .field("distance_error_pct", "0.1")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.1));
        assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(4));
    }

    @Test
    public void testQuadtreeConfiguration() throws IOException {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                    .field("type", "geo_shape")
                    .field("tree", "quadtree")
                    .field("tree_levels", "6")
                    .field("distance_error_pct", "0.5")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
        FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
        assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

        GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
        PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

        assertThat(strategy.getDistErrPct(), equalTo(0.5));
        assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
        assertThat(strategy.getGrid().getMaxLevels(), equalTo(6));
    }
    
    @Test
    public void testLevelPrecisionConfiguration() throws IOException {
        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("tree_levels", "6")
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            /* 70m is more precise so it wins */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(70d))); 
        }
        
        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("tree_levels", "6")
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            /* 70m is more precise so it wins */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(70d))); 
        }
        
        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("tree_levels",  GeoUtils.geoHashLevelsForPrecision(70d)+1)
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            assertThat(strategy.getGrid().getMaxLevels(),  equalTo(GeoUtils.geoHashLevelsForPrecision(70d)+1)); 
        }
        
        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("tree_levels", GeoUtils.quadTreeLevelsForPrecision(70d)+1)
                        .field("precision", "70m")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(70d)+1)); 
        }
    }
    
    @Test
    public void testLevelDefaults() throws IOException {
        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(QuadPrefixTree.class));
            /* 50m is default */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.quadTreeLevelsForPrecision(50d))); 
        }
        
        {
            String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                    .startObject("properties").startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "geohash")
                        .field("distance_error_pct", "0.5")
                    .endObject().endObject()
                    .endObject().endObject().string();

            DocumentMapper defaultMapper = MapperTests.newParser().parse(mapping);
            FieldMapper fieldMapper = defaultMapper.mappers().name("location").mapper();
            assertThat(fieldMapper, instanceOf(GeoShapeFieldMapper.class));

            GeoShapeFieldMapper geoShapeFieldMapper = (GeoShapeFieldMapper) fieldMapper;
            PrefixTreeStrategy strategy = geoShapeFieldMapper.defaultStrategy();

            assertThat(strategy.getDistErrPct(), equalTo(0.5));
            assertThat(strategy.getGrid(), instanceOf(GeohashPrefixTree.class));
            /* 50m is default */
            assertThat(strategy.getGrid().getMaxLevels(), equalTo(GeoUtils.geoHashLevelsForPrecision(50d))); 
        }
    }
}
