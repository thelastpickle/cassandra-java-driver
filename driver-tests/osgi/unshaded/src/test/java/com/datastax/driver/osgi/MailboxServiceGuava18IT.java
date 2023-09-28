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
package com.datastax.driver.osgi;

import static com.datastax.driver.osgi.BundleOptions.defaultOptions;
import static com.datastax.driver.osgi.BundleOptions.driverBundle;
import static com.datastax.driver.osgi.BundleOptions.dropwizardMetricsBundle;
import static com.datastax.driver.osgi.BundleOptions.extrasBundle;
import static com.datastax.driver.osgi.BundleOptions.guavaBundle;
import static com.datastax.driver.osgi.BundleOptions.mailboxBundle;
import static com.datastax.driver.osgi.BundleOptions.mappingBundle;
import static com.datastax.driver.osgi.BundleOptions.nettyBundles;
import static org.ops4j.pax.exam.CoreOptions.options;

import com.datastax.driver.osgi.api.MailboxException;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.testng.listener.PaxExam;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

@Listeners({CCMBridgeListener.class, PaxExam.class})
public class MailboxServiceGuava18IT extends MailboxServiceTests {

  @Configuration
  public Option[] guava18Config() {
    return options(
        defaultOptions(),
        nettyBundles(),
        dropwizardMetricsBundle(),
        guavaBundle().version("18.0"),
        driverBundle(),
        extrasBundle(),
        mappingBundle(),
        mailboxBundle());
  }

  /**
   * Exercises a 'mailbox' service provided by an OSGi bundle that depends on the driver with Guava
   * 18 explicitly enforced.
   *
   * @test_category packaging
   * @expected_result Can create, retrieve and delete data using the mailbox service.
   * @jira_ticket JAVA-620
   * @since 2.0.10, 2.1.5
   */
  @Test(groups = "short")
  public void test_guava_18() throws MailboxException {
    checkService();
  }
}
