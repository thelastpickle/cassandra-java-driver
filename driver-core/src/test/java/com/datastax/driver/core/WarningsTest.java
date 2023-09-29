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

import java.util.Collection;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.utils.CassandraVersion;

public class WarningsTest extends CCMBridge.PerClassSingleNodeCluster {

    /**
     * This is Cassandra's default value, found in o.a.c.config.Config
     */
    private static final int BATCH_SIZE_WARN_THRESHOLD_IN_BYTES = 5 * 1024;

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList(
            "CREATE TABLE foo(k int primary key, v text)"
        );
    }

    @CassandraVersion(major = 2.2)
    @Test(groups = "short")
    public void should_expose_warnings_on_execution_info() {
        ResultSet rs = session.execute(String.format("BEGIN UNLOGGED BATCH INSERT INTO foo (k, v) VALUES (1, '%s') APPLY BATCH",
            Strings.repeat("1", BATCH_SIZE_WARN_THRESHOLD_IN_BYTES)));

        List<String> warnings = rs.getExecutionInfo().getWarnings();
        assertThat(warnings).hasSize(1);
    }
}
