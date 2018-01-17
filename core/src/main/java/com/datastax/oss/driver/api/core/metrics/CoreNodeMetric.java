/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;

public enum CoreNodeMetric implements NodeMetric {

  /**
   * The number of connections open to this node for regular requests (exposed as a {@link Gauge
   * Gauge&lt;Integer&gt;}.
   *
   * <p>This does not include the control connection (which uses at most one extra connection to a
   * random node in the cluster).
   */
  pooled_connection_count,

  /**
   * The number of <em>stream ids</em> available on the connections to this node (exposed as a
   * {@link Gauge Gauge&lt;Integer&gt;}.
   *
   * <p>Stream ids are used to multiplex requests on each connection, so this is an indication of
   * how many more requests the node could handle concurrently before becoming saturated (note that
   * this is a driver-side only consideration, there might be other limitations on the server that
   * prevent reaching that theoretical limit).
   */
  available_stream_count,

  /** How often this node triggered a retry (exposed as a {@link Meter}). */
  retries,
  ;
}
