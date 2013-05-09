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

package org.elasticsearch.test.integration.search.geo;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.geoDistanceFilter;
import static org.elasticsearch.index.query.FilterBuilders.geoDistanceRangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.testng.AssertJUnit.fail;

/**
 */
public class GeoDistanceTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
    }

    @Test
    public void simpleDistanceTests() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();
        client.admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "New York")
                .startObject("location").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                .endObject()).execute().actionGet();

        // to NY: 5.286 km
        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Times Square")
                .startObject("location").field("lat", 40.759011).field("lon", -73.9844722).endObject()
                .endObject()).execute().actionGet();

        // to NY: 0.4621 km
        client.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("name", "Tribeca")
                .startObject("location").field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endObject()).execute().actionGet();

        // to NY: 1.055 km
        client.prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("name", "Wall Street")
                .startObject("location").field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                .endObject()).execute().actionGet();

        // to NY: 1.258 km
        client.prepareIndex("test", "type1", "5").setSource(jsonBuilder().startObject()
                .field("name", "Soho")
                .startObject("location").field("lat", 40.7247222).field("lon", -74).endObject()
                .endObject()).execute().actionGet();

        // to NY: 2.029 km
        client.prepareIndex("test", "type1", "6").setSource(jsonBuilder().startObject()
                .field("name", "Greenwich Village")
                .startObject("location").field("lat", 40.731033).field("lon", -73.9962255).endObject()
                .endObject()).execute().actionGet();

        // to NY: 8.572 km
        client.prepareIndex("test", "type1", "7").setSource(jsonBuilder().startObject()
                .field("name", "Brooklyn")
                .startObject("location").field("lat", 40.65).field("lon", -73.95).endObject()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("3km").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(5l));
        assertThat(searchResponse.getHits().hits().length, equalTo(5));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5"), equalTo("6")));
        }
        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("3km").point(40.7143528, -74.0059731).optimizeBbox("indexed")))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(5l));
        assertThat(searchResponse.getHits().hits().length, equalTo(5));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5"), equalTo("6")));
        }

        // now with a PLANE type
        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("3km").geoDistance(GeoDistance.PLANE).point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(5l));
        assertThat(searchResponse.getHits().hits().length, equalTo(5));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5"), equalTo("6")));
        }

        // factor type is really too small for this resolution

        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("2km").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("2km").point(40.7143528, -74.0059731).optimizeBbox("indexed")))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }

        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("1.242mi").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }
        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceFilter("location").distance("1.242mi").point(40.7143528, -74.0059731).optimizeBbox("indexed")))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("1"), equalTo("3"), equalTo("4"), equalTo("5")));
        }

        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceRangeFilter("location").from("1.0km").to("2.0km").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("4"), equalTo("5")));
        }
        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceRangeFilter("location").from("1.0km").to("2.0km").point(40.7143528, -74.0059731).optimizeBbox("indexed")))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.id(), anyOf(equalTo("4"), equalTo("5")));
        }

        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceRangeFilter("location").to("2.0km").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));

        searchResponse = client.prepareSearch() // from NY
                .setQuery(filteredQuery(matchAllQuery(), geoDistanceRangeFilter("location").from("2.0km").point(40.7143528, -74.0059731)))
                .execute().actionGet();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(3l));
        assertThat(searchResponse.getHits().hits().length, equalTo(3));

        // SORTING

        searchResponse = client.prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("location").point(40.7143528, -74.0059731).order(SortOrder.ASC))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(7l));
        assertThat(searchResponse.getHits().hits().length, equalTo(7));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("5"));
        assertThat(searchResponse.getHits().getAt(4).id(), equalTo("6"));
        assertThat(searchResponse.getHits().getAt(5).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(6).id(), equalTo("7"));

        searchResponse = client.prepareSearch().setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("location").point(40.7143528, -74.0059731).order(SortOrder.DESC))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(7l));
        assertThat(searchResponse.getHits().hits().length, equalTo(7));
        assertThat(searchResponse.getHits().getAt(6).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(5).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(4).id(), equalTo("4"));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("5"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("6"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("7"));
    }

    @Test
    public void testDistanceSortingMVFields() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("locations").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping("type1", mapping)
                .execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("names", "New York")
                .startObject("locations").field("lat", 40.7143528).field("lon", -74.0059731).endObject()
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("names", "Times Square", "Tribeca")
                .startArray("locations")
                        // to NY: 5.286 km
                .startObject().field("lat", 40.759011).field("lon", -73.9844722).endObject()
                        // to NY: 0.4621 km
                .startObject().field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "3").setSource(jsonBuilder().startObject()
                .field("names", "Wall Street", "Soho")
                .startArray("locations")
                        // to NY: 1.055 km
                .startObject().field("lat", 40.7051157).field("lon", -74.0088305).endObject()
                        // to NY: 1.258 km
                .startObject().field("lat", 40.7247222).field("lon", -74).endObject()
                .endArray()
                .endObject()).execute().actionGet();


        client.prepareIndex("test", "type1", "4").setSource(jsonBuilder().startObject()
                .field("names", "Greenwich Village", "Brooklyn")
                .startArray("locations")
                        // to NY: 2.029 km
                .startObject().field("lat", 40.731033).field("lon", -73.9962255).endObject()
                        // to NY: 8.572 km
                .startObject().field("lat", 40.65).field("lon", -73.95).endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        // Order: Asc
        SearchResponse searchResponse = client.prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.ASC))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), equalTo(0d));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(0.4621d, 0.01d));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("3"));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1.055d, 0.01d));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("4"));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(2.029d, 0.01d));

        // Order: Asc, Mode: max
        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.ASC).sortMode("max"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), equalTo(0d));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1.258d, 0.01d));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("2"));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(5.286d, 0.01d));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("4"));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(8.572d, 0.01d));

        // Order: Desc
        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.DESC))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("4"));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(8.572d, 0.01d));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(5.286d, 0.01d));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("3"));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1.258d, 0.01d));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("1"));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), equalTo(0d));

        // Order: Desc, Mode: min
        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.DESC).sortMode("min"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("4"));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(2.029d, 0.01d));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1.055d, 0.01d));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("2"));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(0.4621d, 0.01d));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("1"));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), equalTo(0d));

        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).sortMode("avg").order(SortOrder.ASC))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), equalTo(0d));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("3"));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(1.157d, 0.01d));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("2"));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(2.874d, 0.01d));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("4"));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), closeTo(5.301d, 0.01d));

        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).sortMode("avg").order(SortOrder.DESC))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4l));
        assertThat(searchResponse.getHits().hits().length, equalTo(4));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("4"));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(5.301d, 0.01d));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(2.874d, 0.01d));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("3"));
        assertThat(((Number) searchResponse.getHits().getAt(2).sortValues()[0]).doubleValue(), closeTo(1.157d, 0.01d));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("1"));
        assertThat(((Number) searchResponse.getHits().getAt(3).sortValues()[0]).doubleValue(), equalTo(0d));

        try {
            client.prepareSearch("test").setQuery(matchAllQuery())
                    .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).sortMode("sum"))
                    .execute().actionGet();
            fail("Expected error");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.shardFailures()[0].status(), equalTo(RestStatus.BAD_REQUEST));
        }
    }

    @Test
    // Regression bug: https://github.com/elasticsearch/elasticsearch/issues/2851
    public void testDistanceSortingWithMissingGeoPoint() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("locations").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();

        client.admin().indices().prepareCreate("test")
                .setSettings(settingsBuilder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .addMapping("type1", mapping)
                .execute().actionGet();
        client.admin().cluster().prepareHealth("test").setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("names", "Times Square", "Tribeca")
                .startArray("locations")
                        // to NY: 5.286 km
                .startObject().field("lat", 40.759011).field("lon", -73.9844722).endObject()
                        // to NY: 0.4621 km
                .startObject().field("lat", 40.718266).field("lon", -74.007819).endObject()
                .endArray()
                .endObject()).execute().actionGet();

        client.prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("names", "Wall Street", "Soho")
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        // Order: Asc
        SearchResponse searchResponse = client.prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.ASC))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), closeTo(0.4621d, 0.01d));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));

        // Order: Desc
        searchResponse = client.prepareSearch("test").setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations").point(40.7143528, -74.0059731).order(SortOrder.DESC))
                .execute().actionGet();

        // Doc with missing geo point is first, is consistent with 0.20.x
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2l));
        assertThat(searchResponse.getHits().hits().length, equalTo(2));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("2"));
        assertThat(((Number) searchResponse.getHits().getAt(0).sortValues()[0]).doubleValue(), equalTo(Double.MAX_VALUE));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("1"));
        assertThat(((Number) searchResponse.getHits().getAt(1).sortValues()[0]).doubleValue(), closeTo(5.286d, 0.01d));
    }

    @Test
    public void distanceScriptTests() throws Exception {
        try {
            client.admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        double source_lat = 32.798;
        double source_long = -117.151;
        double target_lat = 32.81;
        double target_long = -117.21;

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point").field("lat_lon", true).endObject().endObject()
                .endObject().endObject().string();
        client.admin().indices().prepareCreate("test").addMapping("type1", mapping).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        client.prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "TestPosition")
                .startObject("location").field("lat", source_lat).field("lon", source_long).endObject()
                .endObject()).execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        SearchResponse searchResponse1 = client.prepareSearch().addField("_source").addScriptField("distance", "doc['location'].arcDistance(" + target_lat + "," + target_long + ")").execute().actionGet();
        Double resultDistance1 = searchResponse1.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance1, equalTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.MILES)));

        SearchResponse searchResponse2 = client.prepareSearch().addField("_source").addScriptField("distance", "doc['location'].distance(" + target_lat + "," + target_long + ")").execute().actionGet();
        Double resultDistance2 = searchResponse2.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance2, equalTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.MILES)));

        SearchResponse searchResponse3 = client.prepareSearch().addField("_source").addScriptField("distance", "doc['location'].arcDistanceInKm(" + target_lat + "," + target_long + ")").execute().actionGet();
        Double resultArcDistance3 = searchResponse3.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance3, equalTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS)));

        SearchResponse searchResponse4 = client.prepareSearch().addField("_source").addScriptField("distance", "doc['location'].distanceInKm(" + target_lat + "," + target_long + ")").execute().actionGet();
        Double resultDistance4 = searchResponse4.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance4, equalTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS)));

        SearchResponse searchResponse5 = client.prepareSearch().addField("_source").addScriptField("distance", "doc['location'].arcDistanceInKm(" + (target_lat) + "," + (target_long + 360) + ")").execute().actionGet();
        Double resultArcDistance5 = searchResponse5.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance5, equalTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS)));

        SearchResponse searchResponse6 = client.prepareSearch().addField("_source").addScriptField("distance", "doc['location'].arcDistanceInKm(" + (target_lat + 360) + "," + (target_long) + ")").execute().actionGet();
        Double resultArcDistance6 = searchResponse6.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance6, equalTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS)));
    }
}