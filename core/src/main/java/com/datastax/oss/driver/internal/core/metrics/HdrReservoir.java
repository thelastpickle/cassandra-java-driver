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

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reservoir implementation backed by the HdrHistogram library.
 *
 * <p>It uses a {@link Recorder} to capture snapshots at a configurable interval: calls to {@link
 * #update(long)} are recorded in a "live" histogram, while {@link #getSnapshot()} is based on a
 * "cached", read-only histogram. Each time the cached histogram becomes older than the interval,
 * the two histograms are switched (therefore statistics won't be available during the first
 * interval after initialization, since we don't have a cached histogram yet).
 *
 * @see <a href="http://hdrhistogram.github.io/HdrHistogram/">HdrHistogram</a>
 */
public class HdrReservoir implements Reservoir {

  private static final Logger LOG = LoggerFactory.getLogger(HdrReservoir.class);

  private final String logPrefix;
  private final Recorder recorder;
  private final long refreshIntervalNanos;

  // The lock only orchestrates `getSnapshot()` calls; `update()` is fed directly to the recorder,
  // which is lock-free. Read operations are comparatively rare, so locking is not a bottleneck.
  private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
  private Histogram cachedHistogram; // Guarded by cacheLock
  private long cachedHistogramTimestampNanos; // Guarded by cacheLock
  private Snapshot cachedSnapshot; // Guarded by cacheLock

  public HdrReservoir(
      Duration highestTrackableLatency,
      int numberOfSignificantValueDigits,
      Duration refreshInterval,
      String logPrefix) {
    this.logPrefix = logPrefix;
    this.recorder = new Recorder(highestTrackableLatency.toNanos(), numberOfSignificantValueDigits);
    this.refreshIntervalNanos = refreshInterval.toNanos();
    this.cachedHistogramTimestampNanos = System.nanoTime();
    this.cachedSnapshot = EMPTY_SNAPSHOT;
  }

  @Override
  public void update(long value) {
    try {
      recorder.recordValue(value);
    } catch (ArrayIndexOutOfBoundsException e) {
      LOG.warn("[{}] Recorded value ({}) is out of bounds, discarding", logPrefix, value);
    }
  }

  /**
   * <em>Not implemented</em>.
   *
   * <p>This reservoir implementation is intended for use with a {@link
   * com.codahale.metrics.Histogram}, which doesn't use this method.
   */
  @Override
  public int size() {
    throw new UnsupportedOperationException("HdrReservoir does not implement size()");
  }

  @Override
  public Snapshot getSnapshot() {
    long now = System.nanoTime();

    cacheLock.readLock().lock();
    try {
      if (now < cachedHistogramTimestampNanos + refreshIntervalNanos) {
        return cachedSnapshot;
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    cacheLock.writeLock().lock();
    try {
      // Might have raced with another writer => re-check the timestamp
      if (now >= cachedHistogramTimestampNanos + refreshIntervalNanos) {
        LOG.debug("Cached snapshot is too old, refreshing");
        cachedHistogram = recorder.getIntervalHistogram(cachedHistogram);
        cachedSnapshot = new HdrSnapshot(cachedHistogram);
        cachedHistogramTimestampNanos = now;
      }
      return cachedSnapshot;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  private class HdrSnapshot extends Snapshot {

    private final Histogram histogram;

    public HdrSnapshot(Histogram histogram) {
      this.histogram = histogram;
    }

    @Override
    public double getValue(double quantile) {
      return histogram.getValueAtPercentile(quantile);
    }

    @Override
    public long[] getValues() {
      int size = size();
      long[] result = new long[size];
      int i = 0;
      for (HistogramIterationValue entry : histogram.recordedValues()) {
        long value = entry.getValueIteratedTo();
        for (int j = 0; j < entry.getCountAtValueIteratedTo(); j++) {
          result[i] = value;
          i += 1;
        }
        // If size() overflowed, the iterator might have too many values
        if (i == size) {
          break;
        }
      }
      return result;
    }

    @Override
    public int size() {
      long longSize = histogram.getTotalCount();
      // The Metrics API requires an int. It's very unlikely that we get an overflow here, unless
      // the refresh interval is ridiculously high (at 10k requests/s, it would have to be more than
      // 59 hours). However handle gracefully just in case.
      int size;
      if (longSize > Integer.MAX_VALUE) {
        LOG.warn("[{}] Too many recorded values, truncating", logPrefix);
        size = Integer.MAX_VALUE;
      } else {
        size = (int) longSize;
      }
      return size;
    }

    @Override
    public long getMax() {
      return histogram.getMaxValue();
    }

    @Override
    public double getMean() {
      return histogram.getMean();
    }

    @Override
    public long getMin() {
      return histogram.getMinValue();
    }

    @Override
    public double getStdDev() {
      return histogram.getStdDeviation();
    }

    @Override
    public void dump(OutputStream output) {
      try (PrintWriter out =
          new PrintWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8))) {
        for (long value : getValues()) {
          out.printf("%d%n", value);
        }
      }
    }
  }

  private static final Snapshot EMPTY_SNAPSHOT =
      new Snapshot() {
        @Override
        public double getValue(double quantile) {
          return 0;
        }

        @Override
        public long[] getValues() {
          return new long[0];
        }

        @Override
        public int size() {
          return 0;
        }

        @Override
        public long getMax() {
          return 0;
        }

        @Override
        public double getMean() {
          return 0;
        }

        @Override
        public long getMin() {
          return 0;
        }

        @Override
        public double getStdDev() {
          return 0;
        }

        @Override
        public void dump(OutputStream output) {
          // nothing to do
        }
      };
}
