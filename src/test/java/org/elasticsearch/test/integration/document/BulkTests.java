package org.elasticsearch.test.integration.document;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public class BulkTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node1");
    }

    @Test
    public void testBulkUpdate_simple() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        BulkResponse bulkResponse = client.prepareBulk()
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("1").setSource("field", 1))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("2").setSource("field", 2).setCreate(true))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("3").setSource("field", 3))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("4").setSource("field", 4))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("5").setSource("field", 5))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(5));

        bulkResponse = client.prepareBulk()
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("1").setScript("ctx._source.field += 1"))
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("2").setScript("ctx._source.field += 1").setRetryOnConflict(3))
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("3").setDoc(jsonBuilder().startObject().field("field1", "test").endObject()))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(3));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getId(), equalTo("1"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getVersion(), equalTo(2l));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getId(), equalTo("2"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getVersion(), equalTo(2l));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getId(), equalTo("3"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getVersion(), equalTo(2l));

        GetResponse getResponse = client.prepareGet().setIndex("test").setType("type1").setId("1").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(2l));

        getResponse = client.prepareGet().setIndex("test").setType("type1").setId("2").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(3l));

        getResponse = client.prepareGet().setIndex("test").setType("type1").setId("3").setFields("field1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(2l));
        assertThat(getResponse.getField("field1").getValue().toString(), equalTo("test"));

        bulkResponse = client.prepareBulk()
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("6").setScript("ctx._source.field += 1")
                        .setUpsertRequest(jsonBuilder().startObject().field("field", 0).endObject()))
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("7").setScript("ctx._source.field += 1"))
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("2").setScript("ctx._source.field += 1"))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(true));
        assertThat(bulkResponse.getItems().length, equalTo(3));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getId(), equalTo("6"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[0].getResponse()).getVersion(), equalTo(1l));
        assertThat(bulkResponse.getItems()[1].getResponse(), nullValue());
        assertThat(bulkResponse.getItems()[1].getFailure().getId(), equalTo("7"));
        assertThat(bulkResponse.getItems()[1].getFailure().getMessage(), containsString("DocumentMissingException"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getId(), equalTo("2"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[2].getResponse()).getVersion(), equalTo(3l));

        getResponse = client.prepareGet().setIndex("test").setType("type1").setId("6").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(1l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(0l));

        getResponse = client.prepareGet().setIndex("test").setType("type1").setId("7").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));

        getResponse = client.prepareGet().setIndex("test").setType("type1").setId("2").setFields("field").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getVersion(), equalTo(3l));
        assertThat(((Long) getResponse.getField("field").getValue()), equalTo(4l));
    }

    @Test
    public void testBulkUpdate_malformedScripts() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 0)
                ).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        BulkResponse bulkResponse = client.prepareBulk()
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("1").setSource("field", 1))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("2").setSource("field", 1))
                .add(client.prepareIndex().setIndex("test").setType("type1").setId("3").setSource("field", 1))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(3));

        bulkResponse = client.prepareBulk()
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("1").setScript("ctx._source.field += a").setFields("field"))
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("2").setScript("ctx._source.field += 1").setFields("field"))
                .add(client.prepareUpdate().setIndex("test").setType("type1").setId("3").setScript("ctx._source.field += a").setFields("field"))
                .execute().actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(true));
        assertThat(bulkResponse.getItems().length, equalTo(3));
        assertThat(bulkResponse.getItems()[0].getFailure().getId(), equalTo("1"));
        assertThat(bulkResponse.getItems()[0].getFailure().getMessage(), containsString("failed to execute script"));
        assertThat(bulkResponse.getItems()[0].getResponse(), nullValue());

        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getId(), equalTo("2"));
        assertThat(((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getVersion(), equalTo(2l));
        assertThat(((Integer)((UpdateResponse) bulkResponse.getItems()[1].getResponse()).getGetResult().field("field").getValue()), equalTo(2));
        assertThat(bulkResponse.getItems()[1].getFailure(), nullValue());

        assertThat(bulkResponse.getItems()[2].getFailure().getId(), equalTo("3"));
        assertThat(bulkResponse.getItems()[2].getFailure().getMessage(), containsString("failed to execute script"));
        assertThat(bulkResponse.getItems()[2].getResponse(), nullValue());
    }

    @Test
    public void testBulkUpdate_largerVolume() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
                .setSettings(
                        ImmutableSettings.settingsBuilder()
                                .put("index.number_of_shards", 2)
                                .put("index.number_of_replicas", 1)
                ).execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        int numDocs = 2000;
        BulkRequestBuilder builder = client.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            builder.add(
                    client.prepareUpdate()
                            .setIndex("test").setType("type1").setId(Integer.toString(i))
                            .setScript("ctx._source.counter += 1").setFields("counter")
                            .setUpsertRequest(jsonBuilder().startObject().field("counter", 1).endObject())
            );
        }

        BulkResponse response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(i)));
            assertThat(response.getItems()[i].getVersion(), equalTo(1l));
            assertThat(response.getItems()[i].getIndex(), equalTo("test"));
            assertThat(response.getItems()[i].getType(), equalTo("type1"));
            assertThat(response.getItems()[i].getOpType(), equalTo("update"));
            assertThat(((UpdateResponse) response.getItems()[i].getResponse()).getId(), equalTo(Integer.toString(i)));
            assertThat(((UpdateResponse) response.getItems()[i].getResponse()).getVersion(), equalTo(1l));
            assertThat(((Integer)((UpdateResponse) response.getItems()[i].getResponse()).getGetResult().field("counter").getValue()), equalTo(1));

            for (int j = 0; j < 5; j++) {
                GetResponse getResponse = client.prepareGet("test", "type1", Integer.toString(i)).setFields("counter").execute().actionGet();
                assertThat(getResponse.isExists(), equalTo(true));
                assertThat(getResponse.getVersion(), equalTo(1l));
                assertThat((Long) getResponse.getField("counter").getValue(), equalTo(1l));
            }
        }

        builder = client.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            UpdateRequestBuilder updateBuilder = client.prepareUpdate()
                    .setIndex("test").setType("type1").setId(Integer.toString(i)).setFields("counter");
            if (i % 2 == 0) {
                updateBuilder.setScript("ctx._source.counter += 1");
            } else {
                updateBuilder.setDoc(jsonBuilder().startObject().field("counter", 2).endObject());
            }
            if (i % 3 == 0) {
                updateBuilder.setRetryOnConflict(3);
            }

            builder.add(updateBuilder);
        }

        response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(i)));
            assertThat(response.getItems()[i].getVersion(), equalTo(2l));
            assertThat(response.getItems()[i].getIndex(), equalTo("test"));
            assertThat(response.getItems()[i].getType(), equalTo("type1"));
            assertThat(response.getItems()[i].getOpType(), equalTo("update"));
            assertThat(((UpdateResponse) response.getItems()[i].getResponse()).getId(), equalTo(Integer.toString(i)));
            assertThat(((UpdateResponse) response.getItems()[i].getResponse()).getVersion(), equalTo(2l));
            assertThat(((Integer)((UpdateResponse) response.getItems()[i].getResponse()).getGetResult().field("counter").getValue()), equalTo(2));
        }

        builder = client.prepareBulk();
        int maxDocs = numDocs / 2 + numDocs;
        for (int i = (numDocs / 2); i < maxDocs; i++) {
            builder.add(
                    client.prepareUpdate()
                            .setIndex("test").setType("type1").setId(Integer.toString(i)).setScript("ctx._source.counter += 1")
            );
        }
        response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(true));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            int id = i + (numDocs / 2);
            if (i >= (numDocs / 2)) {
                assertThat(response.getItems()[i].getFailure().getId(), equalTo(Integer.toString(id)));
                assertThat(response.getItems()[i].getFailure().getMessage(), containsString("DocumentMissingException"));
            } else {
                assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(id)));
                assertThat(response.getItems()[i].getVersion(), equalTo(3l));
                assertThat(response.getItems()[i].getIndex(), equalTo("test"));
                assertThat(response.getItems()[i].getType(), equalTo("type1"));
                assertThat(response.getItems()[i].getOpType(), equalTo("update"));
            }
        }

        builder = client.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            builder.add(
                    client.prepareUpdate()
                            .setIndex("test").setType("type1").setId(Integer.toString(i)).setScript("ctx.op = \"none\"")
            );
        }
        response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getItems()[i].getItemId(), equalTo(i));
            assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(i)));
            assertThat(response.getItems()[i].getIndex(), equalTo("test"));
            assertThat(response.getItems()[i].getType(), equalTo("type1"));
            assertThat(response.getItems()[i].getOpType(), equalTo("update"));
        }

        builder = client.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            builder.add(
                    client.prepareUpdate()
                            .setIndex("test").setType("type1").setId(Integer.toString(i)).setScript("ctx.op = \"delete\"")
            );
        }
        response = builder.execute().actionGet();
        assertThat(response.hasFailures(), equalTo(false));
        assertThat(response.getItems().length, equalTo(numDocs));
        for (int i = 0; i < numDocs; i++) {
            assertThat(response.getItems()[i].getItemId(), equalTo(i));
            assertThat(response.getItems()[i].getId(), equalTo(Integer.toString(i)));
            assertThat(response.getItems()[i].getIndex(), equalTo("test"));
            assertThat(response.getItems()[i].getType(), equalTo("type1"));
            assertThat(response.getItems()[i].getOpType(), equalTo("update"));
            for (int j = 0; j < 5; j++) {
                GetResponse getResponse = client.prepareGet("test", "type1", Integer.toString(i)).setFields("counter").execute().actionGet();
                assertThat(getResponse.isExists(), equalTo(false));
            }
        }
    }

}
