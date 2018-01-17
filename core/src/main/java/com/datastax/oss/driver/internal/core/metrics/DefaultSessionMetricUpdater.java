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
package com.datastax.oss.driver.internal.core.metrics;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DefaultSessionMetricUpdater implements SessionMetricUpdater {

  private final MetricRegistry metricRegistry;
  private final String metricNamePrefix;
  private final Set<SessionMetric> enabledMetrics;

  public DefaultSessionMetricUpdater(
      Set<SessionMetric> enabledMetrics, InternalDriverContext context) {
    this.enabledMetrics = enabledMetrics;
    this.metricRegistry = context.metricRegistry();
    this.metricNamePrefix = context.sessionName() + ".";
  }

  @Override
  public void updateTimer(SessionMetric metric, long duration, TimeUnit unit) {
    if (enabledMetrics.contains(metric)) {
      // TODO plug in an HDR-based histogram recorder with a configurable period
      metricRegistry.timer(metricNamePrefix + metric.name()).update(duration, unit);
    }
  }
}
