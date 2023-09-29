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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import org.assertj.core.api.Fail;
import org.scassandra.http.client.ClosedConnectionConfig;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Result;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.closed_connection;
import static org.scassandra.http.client.Result.write_request_timeout;

/**
 * Integration test with an IdempotenceAwareRetryPolicy.
 */
public class IdempotenceAwareRetryPolicyIntegrationTest extends AbstractRetryPolicyIntegrationTest {

    public IdempotenceAwareRetryPolicyIntegrationTest() {
        super(new IdempotenceAwareRetryPolicy(new CustomRetryPolicy(), new QueryOptions().setDefaultIdempotence(false)));
    }

    @Test(groups = "short")
    public void should_not_retry_on_write_timeout_if_statement_non_idempotent() {
        simulateError(1, write_request_timeout);
        try {
            query();
            fail("expected an WriteTimeoutException");
        } catch (WriteTimeoutException e) {/* expected */}
        assertOnWriteTimeoutWasCalled(1);
        assertThat(errors.getWriteTimeouts().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnWriteTimeout().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_retry_on_write_timeout_if_statement_idempotent() {
        simulateError(1, write_request_timeout);
        session.execute(new SimpleStatement("mock query").setIdempotent(true));
        assertOnWriteTimeoutWasCalled(1);
        assertThat(errors.getWriteTimeouts().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(1);
        assertThat(errors.getRetriesOnWriteTimeout().getCount()).isEqualTo(1);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 0);
    }

    @Test(groups = "short")
    public void should_not_retry_on_client_timeout_if_statement_non_idempotent() {
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1);
        try {
            scassandras
                .node(1).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withThen(then().withFixedDelay(1000L).withRows(row("result", "result1")))
                    .build());
            try {
                query();
                fail("expected a DriverInternalError");
            } catch (DriverInternalError e) {
                assertThat(e.getCause().getMessage()).isEqualTo(
                        String.format("[%s] Operation timed out", host1.getSocketAddress())
                );
            }
            assertOnRequestErrorWasCalled(1);
            assertThat(errors.getClientTimeouts().getCount()).isEqualTo(1);
            assertThat(errors.getRetries().getCount()).isEqualTo(0);
            assertThat(errors.getRetriesOnClientTimeout().getCount()).isEqualTo(0);
            assertQueried(1, 1);
            assertQueried(2, 0);
            assertQueried(3, 0);
        } finally {
            cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS);
        }
    }

    @Test(groups = "short")
    public void should_retry_on_client_timeout_if_statement_idempotent() {
        cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(1);
        try {
            scassandras
                .node(1).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("mock query")
                    .withThen(then().withFixedDelay(1000L).withRows(row("result", "result1")))
                    .build());
            session.execute(new SimpleStatement("mock query").setIdempotent(true));
            assertOnRequestErrorWasCalled(1);
            assertThat(errors.getClientTimeouts().getCount()).isEqualTo(1);
            assertThat(errors.getRetries().getCount()).isEqualTo(1);
            assertThat(errors.getRetriesOnClientTimeout().getCount()).isEqualTo(1);
            assertQueried(1, 1);
            assertQueried(2, 1);
            assertQueried(3, 0);
        } finally {
            cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS);
        }
    }


    @Test(groups = "short", dataProvider = "serverSideErrors")
    public void should_not_retry_on_server_error_if_statement_non_idempotent(Result error, Class<? extends DriverException> exception) {
        simulateError(1, error);
        try {
            query();
            fail("expected " + exception);
        } catch (DriverException e) {
            assertThat(e).isInstanceOf(exception);
        }
        assertOnRequestErrorWasCalled(1);
        assertThat(errors.getOthers().getCount()).isEqualTo(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnOtherErrors().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }

    @SuppressWarnings("UnusedParameters")
    @Test(groups = "short", dataProvider = "serverSideErrors")
    public void should_retry_on_server_error_if_statement_idempotent(Result error, Class<? extends DriverException> exception) {
        simulateError(1, error);
        simulateError(2, error);
        simulateError(3, error);
        try {
            session.execute(new SimpleStatement("mock query").setIdempotent(true));
            fail("expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertThat(e.getErrors().keySet()).hasSize(3).containsOnly(
                host1.getSocketAddress(),
                host2.getSocketAddress(),
                host3.getSocketAddress());
            assertThat(e.getErrors().values()).hasOnlyElementsOfType(exception);
        }
        assertOnRequestErrorWasCalled(3);
        assertThat(errors.getOthers().getCount()).isEqualTo(3);
        assertThat(errors.getRetries().getCount()).isEqualTo(3);
        assertThat(errors.getRetriesOnOtherErrors().getCount()).isEqualTo(3);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 1);
    }


    @Test(groups = "short", dataProvider = "connectionErrors")
    public void should_not_retry_on_connection_error_if_statement_non_idempotent(ClosedConnectionConfig.CloseType closeType) {
        simulateError(1, closed_connection, new ClosedConnectionConfig(closeType));
        simulateError(2, closed_connection, new ClosedConnectionConfig(closeType));
        simulateError(3, closed_connection, new ClosedConnectionConfig(closeType));
        try {
            query();
            Fail.fail("expected an error");
        } catch (DriverInternalError e) {
            assertThat(e.getCause().getMessage()).isEqualTo(
                    String.format("[%s] Connection has been closed", host1.getSocketAddress())
            );
        }
        assertOnRequestErrorWasCalled(1);
        assertThat(errors.getRetries().getCount()).isEqualTo(0);
        assertThat(errors.getConnectionErrors().getCount()).isEqualTo(1);
        assertThat(errors.getIgnoresOnConnectionError().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnConnectionError().getCount()).isEqualTo(0);
        assertQueried(1, 1);
        assertQueried(2, 0);
        assertQueried(3, 0);
    }


    @Test(groups = "short", dataProvider = "connectionErrors")
    public void should_retry_on_connection_error_if_statement_idempotent(ClosedConnectionConfig.CloseType closeType) {
        simulateError(1, closed_connection, new ClosedConnectionConfig(closeType));
        simulateError(2, closed_connection, new ClosedConnectionConfig(closeType));
        simulateError(3, closed_connection, new ClosedConnectionConfig(closeType));
        try {
            session.execute(new SimpleStatement("mock query").setIdempotent(true));
            Fail.fail("expected a NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            assertThat(e.getErrors().keySet()).hasSize(3).containsOnly(
                    host1.getSocketAddress(),
                    host2.getSocketAddress(),
                    host3.getSocketAddress());
        }
        assertOnRequestErrorWasCalled(3);
        assertThat(errors.getRetries().getCount()).isEqualTo(3);
        assertThat(errors.getConnectionErrors().getCount()).isEqualTo(3);
        assertThat(errors.getIgnoresOnConnectionError().getCount()).isEqualTo(0);
        assertThat(errors.getRetriesOnConnectionError().getCount()).isEqualTo(3);
        assertQueried(1, 1);
        assertQueried(2, 1);
        assertQueried(3, 1);
    }


    /**
     * Retries everything on the next host.
     */
    static class CustomRetryPolicy implements ExtendedRetryPolicy {

        @Override
        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
            return RetryDecision.tryNextHost(cl);
        }

        @Override
        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
            return RetryDecision.tryNextHost(cl);
        }

        @Override
        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
            return RetryDecision.tryNextHost(cl);
        }

        @Override
        public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, Exception e, int nbRetry) {
            return RetryDecision.tryNextHost(cl);
        }

    }
}
