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
package com.datastax.oss.driver.internal.core.metadata;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import io.netty.channel.EventLoop;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaAgreementCheckerTest {

  private static final UUID VERSION1 = UUID.randomUUID();
  private static final UUID VERSION2 = UUID.randomUUID();

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultConfig;
  @Mock private DriverChannel channel;
  @Mock private EventLoop eventLoop;
  @Mock private MetadataManager metadataManager;
  @Mock private MetricsFactory metricsFactory;
  @Mock private Metadata metadata;
  @Mock private DefaultNode node1;
  @Mock private DefaultNode node2;

  @Before
  public void setup() {
    when(context.getMetricsFactory()).thenReturn(metricsFactory);

    node1 = TestNodeFactory.newNode(1, context);
    node2 = TestNodeFactory.newNode(2, context);

    when(defaultConfig.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT))
        .thenReturn(Duration.ofSeconds(1));
    when(defaultConfig.getDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_INTERVAL))
        .thenReturn(Duration.ofMillis(200));
    when(defaultConfig.getDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT))
        .thenReturn(Duration.ofSeconds(10));
    when(defaultConfig.getBoolean(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_WARN))
        .thenReturn(true);
    when(config.getDefaultProfile()).thenReturn(defaultConfig);
    when(context.getConfig()).thenReturn(config);

    Map<UUID, Node> nodes = ImmutableMap.of(node1.getHostId(), node1, node2.getHostId(), node2);
    when(metadata.getNodes()).thenReturn(nodes);
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(context.getMetadataManager()).thenReturn(metadataManager);

    node2.state = NodeState.UP;

    when(eventLoop.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
        .thenAnswer(
            invocation -> { // Ignore delay and run immediately:
              Runnable task = invocation.getArgument(0);
              task.run();
              return null;
            });
    when(channel.eventLoop()).thenReturn(eventLoop);
  }

  @Test
  public void should_skip_if_timeout_is_zero() {
    // Given
    when(defaultConfig.getDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT))
        .thenReturn(Duration.ZERO);
    TestSchemaAgreementChecker checker = new TestSchemaAgreementChecker(channel, context);

    // When
    CompletionStage<Boolean> future = checker.run();

    // Then
    assertThatStage(future).isSuccess(b -> assertThat(b).isFalse());
  }

  @Test
  public void should_succeed_if_only_one_node() {
    // Given
    TestSchemaAgreementChecker checker = new TestSchemaAgreementChecker(channel, context);
    checker.stubQueries(
        new StubbedQuery(
            "SELECT schema_version FROM system.local WHERE key='local'",
            mockResult(mockRow(null, VERSION1))),
        new StubbedQuery(
            "SELECT host_id, schema_version FROM system.peers", mockResult(/*empty*/ )));

    // When
    CompletionStage<Boolean> future = checker.run();

    // Then
    assertThatStage(future).isSuccess(b -> assertThat(b).isTrue());
  }

  @Test
  public void should_succeed_if_versions_match_on_first_try() {
    // Given
    TestSchemaAgreementChecker checker = new TestSchemaAgreementChecker(channel, context);
    checker.stubQueries(
        new StubbedQuery(
            "SELECT schema_version FROM system.local WHERE key='local'",
            mockResult(mockRow(null, VERSION1))),
        new StubbedQuery(
            "SELECT host_id, schema_version FROM system.peers",
            mockResult(mockRow(node2.getHostId(), VERSION1))));

    // When
    CompletionStage<Boolean> future = checker.run();

    // Then
    assertThatStage(future).isSuccess(b -> assertThat(b).isTrue());
  }

  @Test
  public void should_ignore_down_peers() {
    // Given
    TestSchemaAgreementChecker checker = new TestSchemaAgreementChecker(channel, context);
    node2.state = NodeState.DOWN;
    checker.stubQueries(
        new StubbedQuery(
            "SELECT schema_version FROM system.local WHERE key='local'",
            mockResult(mockRow(null, VERSION1))),
        new StubbedQuery(
            "SELECT host_id, schema_version FROM system.peers",
            mockResult(mockRow(node2.getHostId(), VERSION2))));

    // When
    CompletionStage<Boolean> future = checker.run();

    // Then
    assertThatStage(future).isSuccess(b -> assertThat(b).isTrue());
  }

  @Test
  public void should_ignore_malformed_rows() {
    // Given
    TestSchemaAgreementChecker checker = new TestSchemaAgreementChecker(channel, context);
    checker.stubQueries(
        new StubbedQuery(
            "SELECT schema_version FROM system.local WHERE key='local'",
            mockResult(mockRow(null, VERSION1))),
        new StubbedQuery(
            "SELECT host_id, schema_version FROM system.peers",
            mockResult(mockRow(null, VERSION2)))); // missing host_id

    // When
    CompletionStage<Boolean> future = checker.run();

    // Then
    assertThatStage(future).isSuccess(b -> assertThat(b).isTrue());
  }

  @Test
  public void should_reschedule_if_versions_do_not_match_on_first_try() {
    // Given
    TestSchemaAgreementChecker checker = new TestSchemaAgreementChecker(channel, context);
    checker.stubQueries(
        // First round
        new StubbedQuery(
            "SELECT schema_version FROM system.local WHERE key='local'",
            mockResult(mockRow(null, VERSION1))),
        new StubbedQuery(
            "SELECT host_id, schema_version FROM system.peers",
            mockResult(mockRow(node2.getHostId(), VERSION2))),

        // Second round
        new StubbedQuery(
            "SELECT schema_version FROM system.local WHERE key='local'",
            mockResult(mockRow(null, VERSION1))),
        new StubbedQuery(
            "SELECT host_id, schema_version FROM system.peers",
            mockResult(mockRow(node2.getHostId(), VERSION1))));

    // When
    CompletionStage<Boolean> future = checker.run();

    // Then
    assertThatStage(future).isSuccess(b -> assertThat(b).isTrue());
  }

  @Test
  public void should_fail_if_versions_do_not_match_after_timeout() {
    // Given
    when(defaultConfig.getDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT))
        .thenReturn(Duration.ofNanos(10));
    TestSchemaAgreementChecker checker = new TestSchemaAgreementChecker(channel, context);
    checker.stubQueries(
        new StubbedQuery(
            "SELECT schema_version FROM system.local WHERE key='local'",
            mockResult(mockRow(null, VERSION1))),
        new StubbedQuery(
            "SELECT host_id, schema_version FROM system.peers",
            mockResult(mockRow(node2.getHostId(), VERSION1))));

    // When
    CompletionStage<Boolean> future = checker.run();

    // Then
    assertThatStage(future).isSuccess(b -> assertThat(b).isFalse());
  }

  /** Extend to mock the query execution logic. */
  private static class TestSchemaAgreementChecker extends SchemaAgreementChecker {

    private final Queue<StubbedQuery> queries = new ArrayDeque<>();

    TestSchemaAgreementChecker(DriverChannel channel, InternalDriverContext context) {
      super(channel, context, "test");
    }

    private void stubQueries(StubbedQuery... queries) {
      this.queries.addAll(Arrays.asList(queries));
    }

    @Override
    protected CompletionStage<AdminResult> query(String queryString) {
      StubbedQuery nextQuery = queries.poll();
      assertThat(nextQuery).isNotNull();
      assertThat(queryString).isEqualTo(nextQuery.queryString);
      return CompletableFuture.completedFuture(nextQuery.result);
    }
  }

  private static class StubbedQuery {
    private final String queryString;
    private final AdminResult result;

    private StubbedQuery(String queryString, AdminResult result) {
      this.queryString = queryString;
      this.result = result;
    }
  }

  private AdminRow mockRow(UUID hostId, UUID schemaVersion) {
    AdminRow row = mock(AdminRow.class);
    when(row.getUuid("host_id")).thenReturn(hostId);
    when(row.getUuid("schema_version")).thenReturn(schemaVersion);
    return row;
  }

  private AdminResult mockResult(AdminRow... rows) {
    AdminResult result = mock(AdminResult.class);
    when(result.iterator()).thenReturn(Iterators.forArray(rows));
    return result;
  }
}
