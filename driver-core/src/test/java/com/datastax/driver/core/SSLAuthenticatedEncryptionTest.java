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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.base.Optional;
import org.testng.annotations.Test;

import static com.datastax.driver.core.CCMBridge.*;

public class SSLAuthenticatedEncryptionTest extends SSLTestBase {

    public SSLAuthenticatedEncryptionTest() {
        super(true);
    }

    /**
     * <p>
     * Validates that an SSL connection can be established with client auth if the target
     * cassandra cluster is using SSL, requires client auth and would validate with the client's
     * certificate.
     * </p>
     *
     * @test_category connection:ssl, authentication
     * @expected_result Connection can be established to a cassandra node using SSL that requires client auth.
     */
    @Test(groups = "short")
    public void should_connect_with_ssl_with_client_auth_and_node_requires_auth() throws Exception {
        connectWithSSLOptions(getSSLOptions(Optional.of(DEFAULT_CLIENT_KEYSTORE_PATH), Optional.of(DEFAULT_CLIENT_TRUSTSTORE_PATH)));
    }


    /**
     * <p>
     * Validates that an SSL connection can not be established with if the target
     * cassandra cluster is using SSL, requires client auth, but the client does not provide
     * sufficient certificate authentication.
     * </p>
     *
     * @test_category connection:ssl, authentication
     * @expected_result Connection is not established.
     */
    @Test(groups = "short", expectedExceptions = {NoHostAvailableException.class})
    public void should_not_connect_without_client_auth_but_node_requires_auth() throws Exception {
        connectWithSSLOptions(getSSLOptions(Optional.<String>absent(), Optional.of(DEFAULT_CLIENT_TRUSTSTORE_PATH)));
    }

    /**
     * <p>
     * Validates that SSL connectivity can be configured via the standard javax.net.ssl System properties.
     * </p>
     *
     * @test_category connection:ssl, authentication
     * @expected_result Connection can be established.
     */
    @Test(groups = "isolated")
    public void should_use_system_properties_with_default_ssl_options() throws Exception {
        System.setProperty("javax.net.ssl.keyStore", DEFAULT_CLIENT_KEYSTORE_FILE.getAbsolutePath());
        System.setProperty("javax.net.ssl.keyStorePassword", DEFAULT_CLIENT_KEYSTORE_PASSWORD);
        System.setProperty("javax.net.ssl.trustStore", DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath());
        System.setProperty("javax.net.ssl.trustStorePassword", DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

        connectWithSSL();
    }
}
