/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import org.apache.log4j.Level;
import org.jboss.byteman.contrib.bmunit.BMNGListener;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Simple test of the Sessions methods against a one node cluster.
 */
@BMUnitConfig(loadDirectory = "target/test-classes")
@Listeners(BMNGListener.class)
@CCMConfig(createCluster = false)
public class SessionErrorTest extends CCMTestsSupport {

    private Cluster cluster1;
    private Cluster cluster2;

    @BeforeMethod
    public void setUp() throws Exception {
        cluster1 = register(createClusterBuilder()
                .addContactPointsWithPorts(ccm().addressOfNode(1)).build()).init();
        cluster2 = register(createClusterBuilder()
                .addContactPointsWithPorts(ccm().addressOfNode(1)).build()).init();
    }

    @Test
    @BMRule(name = "emulate OOME",
            targetClass = "com.datastax.driver.core.Connection$4",
            targetMethod = "apply(Void)",
            action = "throw new OutOfMemoryError(\"not really\")"
    )
    public void should_propagate_errors() {
        try {
            cluster1.connect();
            fail("Expecting OOME");
        } catch (OutOfMemoryError e) {
            assertThat(e).hasMessage("not really");
        }
    }

    @Test
    @BMRule(name = "emulate NPE",
            targetClass = "com.datastax.driver.core.Connection$4",
            targetMethod = "apply(Void)",
            action = "throw new NullPointerException(\"not really\")"
    )
    public void should_not_propagate_unchecked_exceptions() {
        Level previous = TestUtils.setLogLevel(HostConnectionPool.class, Level.WARN);
        MemoryAppender logs = new MemoryAppender().enableFor(HostConnectionPool.class);
        Session session;
        try {
            session = cluster2.connect();
        } finally {
            TestUtils.setLogLevel(HostConnectionPool.class, previous);
            logs.disableFor(HostConnectionPool.class);
        }
        // The pool is still considered valid, although it has 0 active connections
        assertThat(session.getState().getConnectedHosts()).hasSize(1);
        assertThat(logs.get())
                .contains(
                        "Unexpected error during transport initialization",
                        "not really",
                        NullPointerException.class.getSimpleName(),
                        "com.datastax.driver.core.Connection$4.apply");
    }

}
