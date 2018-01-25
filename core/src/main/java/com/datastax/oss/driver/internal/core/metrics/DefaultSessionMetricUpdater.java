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
import com.codahale.metrics.Timer;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metrics.CoreSessionMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSessionMetricUpdater implements SessionMetricUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSessionMetricUpdater.class);

  private final MetricRegistry metricRegistry;
  private final String metricNamePrefix;
  private final Set<SessionMetric> enabledMetrics;

  public DefaultSessionMetricUpdater(
      Set<SessionMetric> enabledMetrics, InternalDriverContext context) {
    this.enabledMetrics = enabledMetrics;
    this.metricRegistry = context.metricRegistry();
    this.metricNamePrefix = context.sessionName() + ".";

    String logPrefix = context.sessionName();
    DriverConfigProfile config = context.config().getDefaultProfile();

    if (enabledMetrics.contains(CoreSessionMetric.cql_requests)) {
      initializeCqlRequestsTimer(config, logPrefix);
    }
  }

  @Override
  public void updateTimer(SessionMetric metric, long duration, TimeUnit unit) {
    if (enabledMetrics.contains(metric)) {
      metricRegistry.timer(metricNamePrefix + metric.name()).update(duration, unit);
    }
  }

  private void initializeCqlRequestsTimer(DriverConfigProfile config, String logPrefix) {
    Duration highestLatency =
        config.getDuration(CoreDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST);
    final int significantDigits;
    int d = config.getInt(CoreDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS);
    if (d >= 0 && d <= 5) {
      significantDigits = d;
    } else {
      LOG.warn(
          "[{}] Configuration option {} is out of range (expected between 0 and 5, found {}); "
              + "using 3 instead.",
          logPrefix,
          CoreDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS,
          d);
      significantDigits = 3;
    }
    Duration refreshInterval =
        config.getDuration(CoreDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL);

    // Initialize eagerly
    metricRegistry.timer(
        metricNamePrefix + CoreSessionMetric.cql_requests,
        () ->
            new Timer(
                new HdrReservoir(
                    highestLatency,
                    significantDigits,
                    refreshInterval,
                    logPrefix + "." + CoreSessionMetric.cql_requests)));
  }
}
