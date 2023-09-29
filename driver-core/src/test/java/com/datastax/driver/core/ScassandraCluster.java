/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.scassandra.cql.MapType.map;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.SetType.set;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;

public class ScassandraCluster {

    private static final Logger logger = LoggerFactory.getLogger(ScassandraCluster.class);

    private final String ipPrefix;

    private final List<Scassandra> instances;

    private final Map<Integer, List<Scassandra>> dcNodeMap;

    private final List<Map<String, ? extends Object>> keyspaceRows;

    ScassandraCluster(Integer[] nodes, String ipPrefix, int binaryPort, int adminPort, List<Map<String, ? extends Object>> keyspaceRows) {
        this.ipPrefix = ipPrefix;

        int node = 1;
        ImmutableList.Builder<Scassandra> instanceListBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Integer, List<Scassandra>> dcNodeMapBuilder = ImmutableMap.builder();
        for (int dc = 0; dc < nodes.length; dc++) {
            ImmutableList.Builder<Scassandra> dcNodeListBuilder = ImmutableList.builder();
            for (int n = 0; n < nodes[dc]; n++) {
                String ip = ipPrefix + node++;
                Scassandra instance = ScassandraFactory.createServer(ip, binaryPort, ip, adminPort);
                instanceListBuilder = instanceListBuilder.add(instance);
                dcNodeListBuilder = dcNodeListBuilder.add(instance);
            }
            dcNodeMapBuilder.put(dc + 1, dcNodeListBuilder.build());
        }
        instances = instanceListBuilder.build();
        dcNodeMap = dcNodeMapBuilder.build();
        this.keyspaceRows = keyspaceRows;
    }

    public Scassandra node(int node) {
        return instances.get(node - 1);
    }

    public List<Scassandra> nodes() {
        return instances;
    }

    public String address(int node) {
        return ipPrefix + node;
    }

    public Scassandra node(int dc, int node) {
        return dcNodeMap.get(dc).get(node - 1);
    }

    public List<Scassandra> nodes(int dc) {
        return dcNodeMap.get(dc);
    }

    public int ipSuffix(int dc, int node) {
        // TODO: Scassandra should be updated to include address to avoid O(n) lookup.
        int nodeCount = 0;
        for (Integer dcNum : new TreeSet<Integer>(dcNodeMap.keySet())) {
            List<Scassandra> nodesInDc = dcNodeMap.get(dc);
            for (int n = 0; n < nodesInDc.size(); n++) {
                nodeCount++;
                if (dcNum == dc && n + 1 == node) {
                    return nodeCount;
                }
            }
        }
        return -1;
    }

    public String address(int dc, int node) {
        // TODO: Scassandra should be updated to include address to avoid O(n) lookup.
        int ipSuffix = ipSuffix(dc, node);
        return ipSuffix == -1 ? null : ipPrefix + ipSuffix;
    }

    public Host host(Cluster cluster, int dc, int node) {
        String address = address(dc, node);
        for (Host host : cluster.getMetadata().getAllHosts()) {
            if (host.getAddress().getHostAddress().equals(address)) {
                return host;
            }
        }
        return null;
    }

    public static String datacenter(int dc) {
        return "DC" + dc;
    }

    public void init() {
        for (Map.Entry<Integer, List<Scassandra>> dc : dcNodeMap.entrySet()) {
            for (Scassandra node : dc.getValue()) {
                node.start();
                primeMetadata(node);
            }
        }
    }

    public void stop() {
        logger.debug("Stopping ScassandraCluster.");
        for (Scassandra node : instances) {
            node.stop();
        }
    }

    /**
     * First stops each node in {@code dc} and then asserts that each node's {@link Host}
     * is marked down for the given {@link Cluster} instance within 10 seconds.
     * <p/>
     * If any of the nodes are the control host, this node is stopped last, to reduce
     * likelihood of control connection choosing a host that will be shut down.
     *
     * @param cluster cluster to wait for down statuses on.
     * @param dc      DC to stop.
     */
    public void stopDC(Cluster cluster, int dc) {
        logger.debug("Stopping all nodes in {}.", datacenter(dc));
        // If any node is the control host, stop it last.
        int controlHost = -1;
        for (int i = 1; i <= nodes(dc).size(); i++) {
            int id = ipSuffix(dc, i);
            Host host = TestUtils.findHost(cluster, id);
            if (cluster.manager.controlConnection.connectedHost() == host) {
                logger.debug("Node {} identified as control host.  Stopping last.", id);
                controlHost = id;
                continue;
            }
            stop(cluster, id);
        }

        if (controlHost != -1) {
            stop(cluster, controlHost);
        }
    }

    /**
     * Stops a node by id and then asserts that its {@link Host} is marked down
     * for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for down status on.
     * @param node    Node to stop.
     */
    public void stop(Cluster cluster, int node) {
        logger.debug("Stopping node {}.", node);
        Scassandra scassandra = node(node);
        scassandra.stop();
        assertThat(cluster).host(node).goesDownWithin(10, TimeUnit.SECONDS);
    }

    /**
     * Stops a node by dc and id and then asserts that its {@link Host} is marked down
     * for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for down status on.
     * @param dc      Data center node is in.
     * @param node    Node to stop.
     */
    public void stop(Cluster cluster, int dc, int node) {
        logger.debug("Stopping node {} in {}.", node, datacenter(dc));
        stop(cluster, ipSuffix(dc, node));
    }

    /**
     * First starts each node in {@code dc} and then asserts that each node's {@link Host}
     * is marked up for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for up statuses on.
     * @param dc      DC to start.
     */
    public void startDC(Cluster cluster, int dc) {
        logger.debug("Starting all nodes in {}.", datacenter(dc));
        for (int i = 1; i <= nodes(dc).size(); i++) {
            int id = ipSuffix(dc, i);
            start(cluster, id);
        }
    }

    /**
     * Starts a node by id and then asserts that its {@link Host} is marked up
     * for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for up status on.
     * @param node    Node to start.
     */
    public void start(Cluster cluster, int node) {
        logger.debug("Starting node {}.", node);
        Scassandra scassandra = node(node);
        scassandra.start();
        assertThat(cluster).host(node).comesUpWithin(10, TimeUnit.SECONDS);
    }

    /**
     * Starts a node by dc and id and then asserts that its {@link Host} is marked up
     * for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for up status on.
     * @param dc      Data center node is in.
     * @param node    Node to start.
     */
    public void start(Cluster cluster, int dc, int node) {
        logger.debug("Starting node {} in {}.", node, datacenter(dc));
        start(cluster, ipSuffix(dc, node));
    }

    private List<Long> getTokensForDC(int dc) {
        // Offset DCs by dc * 100 to ensure unique tokens.
        int offset = (dc - 1) * 100;
        int dcNodeCount = nodes(dc).size();
        List<Long> tokens = Lists.newArrayList(dcNodeCount);
        for (int i = 0; i < dcNodeCount; i++) {
            tokens.add((i * ((long) Math.pow(2, 64) / dcNodeCount) + offset));
        }
        return tokens;
    }

    private void primeMetadata(Scassandra node) {
        PrimingClient client = node.primingClient();
        int nodeCount = 0;

        ImmutableList.Builder<Map<String, ?>> rows = ImmutableList.builder();
        for (Integer dc : new TreeSet<Integer>(dcNodeMap.keySet())) {
            List<Scassandra> nodesInDc = dcNodeMap.get(dc);
            List<Long> tokens = getTokensForDC(dc);
            for (int n = 0; n < nodesInDc.size(); n++) {
                String address = ipPrefix + ++nodeCount;
                Scassandra peer = nodesInDc.get(n);
                String query;
                Map<String, Object> row;
                org.scassandra.http.client.types.ColumnMetadata[] metadata;
                if (node == peer) { // prime system.local.
                    metadata = SELECT_LOCAL;
                    query = "SELECT * FROM system.local WHERE key='local'";
                    row = ImmutableMap.<String, Object>builder()
                            .put("key", "local")
                            .put("bootstrapped", "COMPLETED")
                            .put("broadcast_address", address)
                            .put("cluster_name", "scassandra")
                            .put("cql_version", "3.2.0")
                            .put("data_center", datacenter(dc))
                            .put("listen_address", address)
                            .put("partitioner", "org.apache.cassandra.dht.Murmur3Partitioner")
                            .put("rack", "rack1")
                            .put("release_version", "2.1.8")
                            .put("tokens", ImmutableSet.of(tokens.get(n)))
                            .build();
                } else { // prime system.peers.
                    query = "SELECT * FROM system.peers WHERE peer='" + address + "'";
                    metadata = SELECT_PEERS;
                    row = ImmutableMap.<String, Object>builder()
                            .put("peer", address)
                            .put("rpc_address", address)
                            .put("data_center", datacenter(dc))
                            .put("rack", "rack1")
                            .put("release_version", "2.1.8")
                            .put("tokens", ImmutableSet.of(Long.toString(tokens.get(n))))
                            .build();
                    rows.add(row);
                }
                client.prime(PrimingRequest.queryBuilder()
                        .withQuery(query)
                        .withThen(then()
                                .withColumnTypes(metadata)
                                .withRows(row)
                                .build())
                        .build());
            }
        }

        client.prime(PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.peers")
                .withThen(then()
                        .withColumnTypes(SELECT_PEERS)
                        .withRows(rows.build())
                        .build())
                .build());

        // Needed to ensure cluster_name matches what we expect on connection.
        Map<String, Object> clusterNameRow = ImmutableMap.<String, Object>builder()
                .put("cluster_name", "scassandra")
                .build();
        client.prime(PrimingRequest.queryBuilder()
                .withQuery("select cluster_name from system.local")
                .withThen(then()
                        .withColumnTypes(SELECT_CLUSTER_NAME)
                        .withRows(clusterNameRow)
                        .build())
                .build());


        // Prime keyspaces
        client.prime(PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.schema_keyspaces")
                .withThen(then()
                        .withColumnTypes(SELECT_SCHEMA_KEYSPACES)
                        .withRows(keyspaceRows)
                        .build())
                .build());
    }

    static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_PEERS = {
            column("peer", INET),
            column("rpc_address", INET),
            column("data_center", TEXT),
            column("rack", TEXT),
            column("release_version", TEXT),
            column("tokens", set(TEXT))
    };

    static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_LOCAL = {
            column("key", TEXT),
            column("bootstrapped", TEXT),
            column("broadcast_address", INET),
            column("cluster_name", TEXT),
            column("cql_version", TEXT),
            column("data_center", TEXT),
            column("listen_address", INET),
            column("partitioner", TEXT),
            column("rack", TEXT),
            column("release_version", TEXT),
            column("tokens", set(TEXT)),
    };

    static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_CLUSTER_NAME = {
            column("cluster_name", TEXT)
    };

    static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_SCHEMA_KEYSPACES = {
            column("durable_writes", BOOLEAN),
            column("keyspace_name", TEXT),
            column("strategy_class", TEXT),
            column("strategy_options", TEXT)
    };

    static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_SCHEMA_COLUMN_FAMILIES = {
            column("bloom_filter_fp_chance", DOUBLE),
            column("caching", TEXT),
            column("cf_id", UUID),
            column("column_aliases", TEXT),
            column("columnfamily_name", TEXT),
            column("comment", TEXT),
            column("compaction_strategy_class", TEXT),
            column("compaction_strategy_options", TEXT),
            column("comparator", TEXT),
            column("compression_parameters", TEXT),
            column("default_time_to_live", INT),
            column("default_validator", TEXT),
            column("dropped_columns", map(TEXT, BIG_INT)),
            column("gc_grace_seconds", INT),
            column("index_interval", INT),
            column("is_dense", BOOLEAN),
            column("key_aliases", TEXT),
            column("key_validator", TEXT),
            column("keyspace_name", TEXT),
            column("local_read_repair_chance", DOUBLE),
            column("max_compaction_threshold", INT),
            column("max_index_interval", INT),
            column("memtable_flush_period_in_ms", INT),
            column("min_compaction_threshold", INT),
            column("min_index_interval", INT),
            column("read_repair_chance", DOUBLE),
            column("speculative_retry", TEXT),
            column("subcomparator", TEXT),
            column("type", TEXT),
            column("value_alias", TEXT)
    };

    static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_SCHEMA_COLUMNS = {
            column("column_name", TEXT),
            column("columnfamily_name", TEXT),
            column("component_index", INT),
            column("index_name", TEXT),
            column("index_options", TEXT),
            column("index_type", TEXT),
            column("keyspace_name", TEXT),
            column("type", TEXT),
            column("validator", TEXT),
    };

    public static ScassandraClusterBuilder builder() {
        return new ScassandraClusterBuilder();
    }

    public static class ScassandraClusterBuilder {

        private Integer nodes[] = {1};
        private String ipPrefix = CCMBridge.IP_PREFIX;
        private final List<Map<String, ? extends Object>> keyspaceRows = Lists.newArrayList();


        public ScassandraClusterBuilder withNodes(Integer... nodes) {
            this.nodes = nodes;
            return this;
        }

        public ScassandraClusterBuilder withIpPrefix(String ipPrefix) {
            this.ipPrefix = ipPrefix;
            return this;
        }

        public ScassandraClusterBuilder withSimpleKeyspace(String name, int replicationFactor) {
            Map<String, Object> simpleKeyspaceRow = ImmutableMap.<String, Object>builder()
                    .put("durable_writes", false)
                    .put("keyspace_name", name)
                    .put("strategy_class", "SimpleStrategy")
                    .put("strategy_options", "{\"replication_factor\":\"" + replicationFactor + "\"}")
                    .build();

            keyspaceRows.add(simpleKeyspaceRow);
            return this;
        }

        public ScassandraClusterBuilder withNetworkTopologyKeyspace(String name, Map<Integer, Integer> replicationFactors) {
            StringBuilder strategyOptionsBuilder = new StringBuilder("{");
            for (Map.Entry<Integer, Integer> dc : replicationFactors.entrySet()) {
                strategyOptionsBuilder.append("\"");
                strategyOptionsBuilder.append(datacenter(dc.getKey()));
                strategyOptionsBuilder.append("\":\"");
                strategyOptionsBuilder.append(dc.getValue());
                strategyOptionsBuilder.append("\",");
            }

            String strategyOptions = strategyOptionsBuilder.substring(0, strategyOptionsBuilder.length() - 1) + "}";

            Map<String, Object> ntsKeyspaceRow = ImmutableMap.<String, Object>builder()
                    .put("durable_writes", false)
                    .put("keyspace_name", name)
                    .put("strategy_class", "NetworkTopologyStrategy")
                    .put("strategy_options", strategyOptions)
                    .build();

            keyspaceRows.add(ntsKeyspaceRow);
            return this;
        }

        public ScassandraCluster build() {
            return new ScassandraCluster(nodes, ipPrefix, 9042, 9052, keyspaceRows);
        }
    }
}
