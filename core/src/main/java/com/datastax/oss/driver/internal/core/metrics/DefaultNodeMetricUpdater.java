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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.CoreNodeMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;

public class DefaultNodeMetricUpdater implements NodeMetricUpdater {

  private final String metricNamePrefix;
  private final MetricRegistry metricRegistry;
  private final Set<NodeMetric> enabledMetrics;

  public DefaultNodeMetricUpdater(
      Node node, Set<NodeMetric> enabledMetrics, InternalDriverContext context) {
    this.metricNamePrefix = buildPrefix(context.sessionName(), node.getConnectAddress());
    this.enabledMetrics = enabledMetrics;
    this.metricRegistry = context.metricRegistry();

    if (enabledMetrics.contains(CoreNodeMetric.pooled_connection_count)) {
      metricRegistry.register(
          metricNamePrefix + CoreNodeMetric.pooled_connection_count.name(),
          (Gauge<Integer>)
              () -> {
                ChannelPool pool = context.poolManager().getPools().get(node);
                return (pool == null) ? 0 : pool.size();
              });
    }
    if (enabledMetrics.contains(CoreNodeMetric.available_stream_count)) {
      metricRegistry.register(
          metricNamePrefix + CoreNodeMetric.available_stream_count,
          (Gauge<Integer>)
              () -> {
                ChannelPool pool = context.poolManager().getPools().get(node);
                return (pool == null) ? 0 : pool.getAvailableIds();
              });
    }
  }

  @Override
  public void markMeter(NodeMetric metric) {
    if (enabledMetrics.contains(metric)) {
      metricRegistry.meter(metricNamePrefix + metric.name()).mark();
    }
  }

  private String buildPrefix(String sessionName, InetSocketAddress addressAndPort) {
    StringBuilder prefix = new StringBuilder(sessionName).append(".nodes.");
    InetAddress address = addressAndPort.getAddress();
    int port = addressAndPort.getPort();
    if (address instanceof Inet4Address) {
      // Metrics use '.' as a delimiter, replace so that the IP is a single path component
      // (127.0.0.1 => 127_0_0_1)
      prefix.append(address.getHostAddress().replace('.', '_'));
    } else {
      assert address instanceof Inet6Address;
      // IPv6 only uses '%' and ':' as separators, so no replacement needed
      prefix.append(address.getHostAddress());
    }
    // Append the port in anticipation of when C* will support nodes on different ports
    return prefix.append('_').append(port).append('.').toString();
  }
}
