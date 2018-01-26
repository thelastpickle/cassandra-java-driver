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

import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;

public enum CoreSessionMetric implements SessionMetric {

  // Implementation note: this enum deliberately breaks the convention of upper-case constant names,
  // because these names are used directly as metric names.

  /**
   * The throughput and latency percentiles of CQL requests (exposed as a {@link Timer}).
   *
   * <p>This corresponds to the overall duration of the {@link CqlSession#execute(Statement)} call,
   * including any retry.
   */
  cql_requests,
  ;
}
