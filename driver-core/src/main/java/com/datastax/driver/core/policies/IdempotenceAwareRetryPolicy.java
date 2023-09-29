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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;

/**
 * A retry policy that avoids retrying non-idempotent statements.
 * <p/>
 * In case of write timeouts or unexpected errors, this policy will always return {@link com.datastax.driver.core.policies.RetryPolicy.RetryDecision#rethrow()}
 * if the statement is deemed non-idempotent (see {@link #isIdempotent(Statement)}).
 * <p/>
 * For all other cases, this policy delegates the decision to the child policy.
 */
public class IdempotenceAwareRetryPolicy implements ExtendedRetryPolicy {

    private final ExtendedRetryPolicy childPolicy;

    private final QueryOptions queryOptions;

    /**
     * Creates a new instance.
     *
     * @param childPolicy the policy to wrap.
     * @param queryOptions the cluster's {@link QueryOptions} object.
     */
    public IdempotenceAwareRetryPolicy(ExtendedRetryPolicy childPolicy, QueryOptions queryOptions) {
        this.childPolicy = childPolicy;
        this.queryOptions = queryOptions;
    }

    @Override
    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
        return childPolicy.onReadTimeout(statement, cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
    }

    @Override
    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
        if(isIdempotent(statement))
            return childPolicy.onWriteTimeout(statement, cl, writeType, requiredAcks, receivedAcks, nbRetry);
        else
            return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
        return childPolicy.onUnavailable(statement, cl, requiredReplica, aliveReplica, nbRetry);
    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, Exception e, int nbRetry) {
        if(isIdempotent(statement))
            return childPolicy.onRequestError(statement, cl, e, nbRetry);
        else
            return RetryDecision.rethrow();
    }

    /**
     * Determines whether the given statement is idempotent or not.
     * <p/>
     * The current implementation inspects the statement's
     * {@link Statement#isIdempotent() idempotent flag};
     * if this flag is not set, then it inspects
     * {@link QueryOptions#getDefaultIdempotence()}.
     * <p/>
     * Subclasses may override if they have better knowledge of
     * the statement being executed.
     *
     * @param statement The statement to execute.
     * @return {@code true} if the given statement is idempotent,
     * {@code false} otherwise
     */
    protected boolean isIdempotent(Statement statement) {
        Boolean myValue = statement.isIdempotent();
        if (myValue != null)
            return myValue;
        else
            return queryOptions.getDefaultIdempotence();
    }

}
