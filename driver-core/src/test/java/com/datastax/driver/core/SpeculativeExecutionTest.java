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

import java.util.*;

import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.*;

import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class SpeculativeExecutionTest {
    SCassandraCluster scassandras;

    Cluster cluster = null;
    SortingLoadBalancingPolicy loadBalancingPolicy;
    Metrics.Errors errors;
    Host host1, host2, host3;
    Session session;

    @BeforeClass(groups = "short")
    public void beforeClass() {
        scassandras = new SCassandraCluster(CCMBridge.IP_PREFIX, 3);
    }

    @BeforeMethod(groups = "short")
    public void beforeMethod() {
        int speculativeExecutionDelay = 200;

        loadBalancingPolicy = new SortingLoadBalancingPolicy();
        cluster = Cluster.builder()
            .addContactPoint(CCMBridge.ipOfNode(2))
            .withProtocolVersion(ProtocolVersion.V2) // Scassandra does not support V3 nor V4 yet
            .withLoadBalancingPolicy(loadBalancingPolicy)
            .withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(speculativeExecutionDelay, 1))
            .withQueryOptions(new QueryOptions().setDefaultIdempotence(true))
            .withRetryPolicy(new CustomRetryPolicy())
            .build();

        session = cluster.connect();

        host1 = TestUtils.findHost(cluster, 1);
        host2 = TestUtils.findHost(cluster, 2);
        host3 = TestUtils.findHost(cluster, 3);

        errors = cluster.getMetrics().getErrorMetrics();
    }

    @Test(groups = "short")
    public void should_not_start_speculative_execution_if_first_execution_completes_successfully() {
        scassandras.prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withRows(row("result", "result1"))
                .build()
        );

        long execStartCount = errors.getSpeculativeExecutions().getCount();

        ResultSet rs = session.execute("mock query");
        Row row = rs.one();

        assertThat(row.getString("result")).isEqualTo("result1");
        assertThat(errors.getSpeculativeExecutions().getCount()).isEqualTo(execStartCount);
        assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(host1);
    }

    @Test(groups = "short")
    public void should_not_start_speculative_execution_if_first_execution_retries_but_is_still_fast_enough() {
        // will retry once on this node:
        scassandras.prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withConsistency(PrimingRequest.Consistency.TWO)
                .withResult(PrimingRequest.Result.read_request_timeout)
                .build()
        ).prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withConsistency(PrimingRequest.Consistency.ONE)
                .withRows(row("result", "result1"))
                .build()
        );

        long execStartCount = errors.getSpeculativeExecutions().getCount();
        long retriesStartCount = errors.getRetriesOnUnavailable().getCount();

        SimpleStatement statement = session.newSimpleStatement("mock query");
        statement.setConsistencyLevel(ConsistencyLevel.TWO);
        ResultSet rs = session.execute(statement);
        Row row = rs.one();

        assertThat(row.getString("result")).isEqualTo("result1");
        assertThat(errors.getSpeculativeExecutions().getCount()).isEqualTo(execStartCount);
        assertThat(errors.getRetriesOnReadTimeout().getCount()).isEqualTo(retriesStartCount + 1);
        assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(host1);
    }

    @Test(groups = "short")
    public void should_start_speculative_execution_if_first_execution_takes_too_long() {
        scassandras.prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withFixedDelay(400)
                .withRows(row("result", "result1"))
                .build()
        ).prime(2, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withRows(row("result", "result2"))
                .build()
        );
        long execStartCount = errors.getSpeculativeExecutions().getCount();

        ResultSet rs = session.execute("mock query");
        Row row = rs.one();

        assertThat(row.getString("result")).isEqualTo("result2");
        assertThat(errors.getSpeculativeExecutions().getCount()).isEqualTo(execStartCount + 1);
        assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(host2);
    }

    @Test(groups = "short")
    public void should_wait_until_all_executions_have_finished() {
        // Rely on read timeouts to trigger errors that cause an execution to move to the next node
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1000);

        scassandras
            // execution1 starts with host1, which will time out at t=1000
            .prime(1, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withFixedDelay(2000)
                .withRows(row("result", "result1"))
                .build())
                // at t=1000, execution1 moves to host3, which eventually succeeds at t=1500
            .prime(3, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withFixedDelay(500)
                .withRows(row("result", "result3"))
                .build())
                // meanwhile, execution2 starts at t=200, using host2 which times out at t=1200
                // at that time, the query plan is empty so execution2 fails
                // The goal of this test is to check that execution2 does not fail the query, since execution1 is still running
            .prime(2, PrimingRequest.queryBuilder()
                .withQuery("mock query")
                .withFixedDelay(2000)
                .withRows(row("result", "result2"))
                .build());
        long execStartCount = errors.getSpeculativeExecutions().getCount();

        ResultSet rs = session.execute("mock query");
        Row row = rs.one();

        assertThat(row.getString("result")).isEqualTo("result3");
        assertThat(errors.getSpeculativeExecutions().getCount()).isEqualTo(execStartCount + 1);
        assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(host3);
    }

    /**
     * Validates that when a Cluster is initialized that {@link SpeculativeExecutionPolicy#init(Cluster)} is called and
     * that when a Cluster is closed {@link SpeculativeExecutionPolicy#close()} is called.
     *
     * @test_category queries:speculative_execution
     * @expected_result init and close are called on cluster init and close.
     * @jira_ticket JAVA-796
     * @since 2.0.11, 2.1.7, 2.2.1
     */
    @Test(groups="short")
    public void should_init_and_close_policy_on_cluster() {
        SpeculativeExecutionPolicy mockPolicy = mock(SpeculativeExecutionPolicy.class);

        Cluster cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(2))
                // Scassandra does not support V3 nor V4 yet, and V4 may cause the server to crash
                .withProtocolVersion(ProtocolVersion.V2)
                .withSpeculativeExecutionPolicy(mockPolicy)
                .build();

        verify(mockPolicy, times(0)).init(cluster);
        verify(mockPolicy, times(0)).close();

        try {
            cluster.init();
            verify(mockPolicy, times(1)).init(cluster);
        } finally {
            cluster.close();
            verify(mockPolicy, times(1)).close();
        }
    }

    @AfterMethod(groups = "short")
    public void afterMethod() {
        scassandras.clearAllPrimes();
        if (cluster != null)
            cluster.close();
    }

    @AfterClass(groups = "short")
    public void afterClass() {
        if (scassandras != null)
            scassandras.stop();
    }

    /**
     * Custom retry policy that retries at ONE on read timeout.
     * This deals with the fact that Scassandra only allows read timeouts with 0 replicas.
     */
    static class CustomRetryPolicy implements RetryPolicy {
        @Override
        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            if (nbRetry != 0)
                return RetryDecision.rethrow();
            return RetryDecision.retry(ConsistencyLevel.ONE);
        }

        @Override
        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return RetryDecision.rethrow();
        }

        @Override
        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return RetryDecision.rethrow();
        }

        @Override
        public void init(Cluster cluster) {}

        @Override
        public void close() {}
    }

    private static List<Map<String, ?>> row(String key, String value) {
        return ImmutableList.<Map<String, ?>>of(ImmutableMap.of(key, value));
    }
}
