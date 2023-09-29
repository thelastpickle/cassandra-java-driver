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

import com.google.common.collect.Lists;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.utils.Bytes;

import static com.datastax.driver.core.Assertions.assertThat;

public class SchemaChangesTest extends CCMBridge.PerClassSingleNodeCluster {

    // Create a second cluster to check that other clients also get notified
    Cluster cluster2;
    // The metadatas of the two clusters should be kept in sync
    List<Metadata> metadatas;

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList();
    }

    @BeforeClass(groups = "short")
    public void setup() {
        cluster2 = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
        metadatas = Lists.newArrayList(cluster.getMetadata(), cluster2.getMetadata());
    }

    @Test(groups = "short")
    public void should_notify_of_table_creation() {
        session.execute("CREATE TABLE ks.table1(i int primary key)");

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks").getTable("table1"))
                .isNotNull();
    }

    @Test(groups = "short")
    public void should_notify_of_table_update() {
        session.execute("CREATE TABLE ks.table1(i int primary key)");
        session.execute("ALTER TABLE ks.table1 ADD j int");

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks").getTable("table1").getColumn("j"))
                .isNotNull();
    }

    @Test(groups = "short")
    public void should_notify_of_table_drop() {
        session.execute("CREATE TABLE ks.table1(i int primary key)");
        session.execute("DROP TABLE ks.table1");

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks").getTable("table1"))
                .isNull();
    }

    @Test(groups = "short")
    public void should_notify_of_keyspace_creation() {
        session.execute("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");

        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks2"))
                .isNotNull();
    }

    @Test(groups = "short")
    public void should_notify_of_keyspace_update() {
        session.execute("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks2").isDurableWrites())
                .isTrue();

        session.execute("ALTER KEYSPACE ks2 WITH durable_writes = false");
        for (Metadata m : metadatas)
            assertThat(m.getKeyspace("ks2").isDurableWrites())
                .isFalse();
    }

    @Test(groups = "short")
    public void should_notify_of_keyspace_drop() {
        session.execute("CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        for (Metadata m : metadatas)
            assertThat(m.getReplicas("ks2", Bytes.fromHexString("0xCAFEBABE")))
                .isNotEmpty();

        session.execute("DROP KEYSPACE ks2");

        for (Metadata m : metadatas) {
            assertThat(m.getKeyspace("ks2"))
                .isNull();
            assertThat(m.getReplicas("ks2", Bytes.fromHexString("0xCAFEBABE")))
                .isEmpty();
        }
    }

    @AfterMethod(groups = "short")
    public void cleanup() {
        session.execute("DROP TABLE IF EXISTS ks.table1");
        session.execute("DROP KEYSPACE IF EXISTS ks2");
    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster2 != null)
            cluster2.close();
    }
}
