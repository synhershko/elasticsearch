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

package org.elasticsearch.test.unit.cluster.routing.allocation;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.command.AllocateAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.DisableAllocationDecider;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.testng.annotations.Test;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.metadata.IndexMetaData.newIndexMetaDataBuilder;
import static org.elasticsearch.cluster.metadata.MetaData.newMetaDataBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;
import static org.elasticsearch.cluster.routing.RoutingBuilders.routingTable;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.unit.cluster.routing.allocation.RoutingAllocationTests.newNode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@Test
public class AllocationCommandsTests {

    private final ESLogger logger = Loggers.getLogger(AllocationCommandsTests.class);

    @Test
    public void moveShardCommand() {
        AllocationService allocation = new AllocationService(settingsBuilder().put("cluster.routing.allocation.concurrent_recoveries", 10).build());

        logger.info("creating an index with 1 shard, no replica");
        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(1).numberOfReplicas(0))
                .build();
        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("adding two nodes and performing rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder().put(newNode("node1")).put(newNode("node2"))).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("start primary shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        logger.info("move the shard");
        String existingNodeId = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String toNodeId;
        if ("node1".equals(existingNodeId)) {
            toNodeId = "node2";
        } else {
            toNodeId = "node1";
        }
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new MoveAllocationCommand(new ShardId("test", 0), existingNodeId, toNodeId)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node(existingNodeId).shards().get(0).state(), equalTo(ShardRoutingState.RELOCATING));
        assertThat(clusterState.routingNodes().node(toNodeId).shards().get(0).state(), equalTo(ShardRoutingState.INITIALIZING));

        logger.info("finish moving the shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();

        assertThat(clusterState.routingNodes().node(existingNodeId).shards().isEmpty(), equalTo(true));
        assertThat(clusterState.routingNodes().node(toNodeId).shards().get(0).state(), equalTo(ShardRoutingState.STARTED));
    }

    @Test
    public void allocateCommand() {
        AllocationService allocation = new AllocationService(settingsBuilder()
                .put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION, true)
                .put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, true)
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 3 nodes on same rack and do rerouting");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(newNode("node1"))
                .put(newNode("node2"))
                .put(newNode("node3"))
        ).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> allocating with primary flag set to false, should fail");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node1", false)));
            assert false;
        } catch (ElasticSearchIllegalArgumentException e) {
        }

        logger.info("--> allocating with primary flag set to true");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node1", true)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(0));

        logger.info("--> start the primary shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(0));

        logger.info("--> allocate the replica shard on the primary shard node, should fail");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node1", false)));
            assert false;
        } catch (ElasticSearchIllegalArgumentException e) {
        }

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node2", false)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));


        logger.info("--> start the replica shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> verify that we fail when there are no unassigned shards");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node3", false)));
            assert false;
        } catch (ElasticSearchIllegalArgumentException e) {
        }
    }

    @Test
    public void cancelCommand() {
        AllocationService allocation = new AllocationService(settingsBuilder()
                .put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_NEW_ALLOCATION, true)
                .put(DisableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_DISABLE_ALLOCATION, true)
                .build());

        logger.info("--> building initial routing table");
        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(1).numberOfReplicas(1))
                .build();
        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding 3 nodes");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(newNode("node1"))
                .put(newNode("node2"))
                .put(newNode("node3"))
        ).build();
        RoutingAllocation.Result rerouteResult = allocation.reroute(clusterState);
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));

        logger.info("--> allocating with primary flag set to true");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node1", true)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(0));

        logger.info("--> cancel primary allocation, make sure it fails...");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node1", false)));
            assert false;
        } catch (ElasticSearchIllegalArgumentException e) {
        }

        logger.info("--> start the primary shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(0));

        logger.info("--> cancel primary allocation, make sure it fails...");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node1", false)));
            assert false;
        } catch (ElasticSearchIllegalArgumentException e) {
        }

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node2", false)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the relocation allocation");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node2", false)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(0));
        assertThat(clusterState.routingNodes().node("node3").shards().size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node2", false)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));

        logger.info("--> cancel the primary being replicated, make sure it fails");
        try {
            allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node1", false)));
            assert false;
        } catch (ElasticSearchIllegalArgumentException e) {
        }

        logger.info("--> start the replica shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> cancel allocation of the replica shard");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node2", false)));
        assertThat(rerouteResult.changed(), equalTo(true));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(0));
        assertThat(clusterState.routingNodes().node("node3").shards().size(), equalTo(0));

        logger.info("--> allocate the replica shard on on the second node");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new AllocateAllocationCommand(new ShardId("test", 0), "node2", false)));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(rerouteResult.changed(), equalTo(true));
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(INITIALIZING).size(), equalTo(1));
        logger.info("--> start the replica shard");
        rerouteResult = allocation.applyStartedShards(clusterState, clusterState.routingNodes().shardsWithState(INITIALIZING));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node1").shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shards().size(), equalTo(1));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).size(), equalTo(1));

        logger.info("--> cancel the primary allocation (with allow_primary set to true)");
        rerouteResult = allocation.reroute(clusterState, new AllocationCommands(new CancelAllocationCommand(new ShardId("test", 0), "node1", true)));
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(rerouteResult.routingTable()).build();
        assertThat(rerouteResult.changed(), equalTo(true));
        assertThat(clusterState.routingNodes().node("node2").shardsWithState(STARTED).get(0).primary(), equalTo(true));
        assertThat(clusterState.routingNodes().node("node1").shards().size(), equalTo(0));
        assertThat(clusterState.routingNodes().node("node3").shards().size(), equalTo(0));
    }

    @Test
    public void serialization() throws Exception {
        AllocationCommands commands = new AllocationCommands(
                new AllocateAllocationCommand(new ShardId("test", 1), "node1", true),
                new MoveAllocationCommand(new ShardId("test", 3), "node2", "node3"),
                new CancelAllocationCommand(new ShardId("test", 4), "node5", true)
        );
        BytesStreamOutput bytes = new BytesStreamOutput();
        AllocationCommands.writeTo(commands, bytes);
        AllocationCommands sCommands = AllocationCommands.readFrom(new BytesStreamInput(bytes.bytes()));

        assertThat(sCommands.commands().size(), equalTo(3));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).shardId(), equalTo(new ShardId("test", 1)));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).node(), equalTo("node1"));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).allowPrimary(), equalTo(true));

        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).shardId(), equalTo(new ShardId("test", 3)));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).fromNode(), equalTo("node2"));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).toNode(), equalTo("node3"));

        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).shardId(), equalTo(new ShardId("test", 4)));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).node(), equalTo("node5"));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).allowPrimary(), equalTo(true));
    }

    @Test
    public void xContent() throws Exception {
        String commands = "{\n" +
                "    \"commands\" : [\n" +
                "        {\"allocate\" : {\"index\" : \"test\", \"shard\" : 1, \"node\" : \"node1\", \"allow_primary\" : true}}\n" +
                "       ,{\"move\" : {\"index\" : \"test\", \"shard\" : 3, \"from_node\" : \"node2\", \"to_node\" : \"node3\"}} \n" +
                "       ,{\"cancel\" : {\"index\" : \"test\", \"shard\" : 4, \"node\" : \"node5\", \"allow_primary\" : true}} \n" +
                "    ]\n" +
                "}\n";
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(commands);
        // move two tokens, parser expected to be "on" `commands` field
        parser.nextToken();
        parser.nextToken();
        AllocationCommands sCommands = AllocationCommands.fromXContent(parser);

        assertThat(sCommands.commands().size(), equalTo(3));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).shardId(), equalTo(new ShardId("test", 1)));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).node(), equalTo("node1"));
        assertThat(((AllocateAllocationCommand) (sCommands.commands().get(0))).allowPrimary(), equalTo(true));

        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).shardId(), equalTo(new ShardId("test", 3)));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).fromNode(), equalTo("node2"));
        assertThat(((MoveAllocationCommand) (sCommands.commands().get(1))).toNode(), equalTo("node3"));

        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).shardId(), equalTo(new ShardId("test", 4)));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).node(), equalTo("node5"));
        assertThat(((CancelAllocationCommand) (sCommands.commands().get(2))).allowPrimary(), equalTo(true));
    }
}
