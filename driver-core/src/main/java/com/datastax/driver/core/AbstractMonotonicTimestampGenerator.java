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

import com.google.common.annotations.VisibleForTesting;

/**
 * Base implementation for monotonic timestamp generators.
 * <p/>
 * The accuracy of the generated timestamps is largely dependent on the
 * granularity of the underlying operating system's clock.
 * <p/>
 * Generally speaking, this granularity is millisecond, and
 * the sub-millisecond part is simply a counter that gets incremented
 * until the next clock tick, as provided by {@link System#currentTimeMillis()}.
 * <p/>
 * On some systems, however, it is possible to have a better granularity by using a JNR
 * call to {@code gettimeofday}. The driver will use this system call automatically whenever
 * available, unless the system property {@code com.datastax.driver.USE_NATIVE_CLOCK} is
 * explicitly set to {@code false}.
 * <p/>
 * Beware that to guarantee monotonicity, if more than one call to {@link #next()}
 * is made within the same microsecond, or in the event of a system clock skew, this generator might
 * return timestamps that drift out in the future.
 *  Whe this happens, {@link #onDrift(long, long)} is invoked.
 */
public abstract class AbstractMonotonicTimestampGenerator implements TimestampGenerator {

    @VisibleForTesting
    volatile Clock clock = ClockFactory.newInstance();

    /**
     * Compute the next timestamp, given the last timestamp previously generated.
     * <p/>
     * To guarantee monotonicity, the next timestamp should be strictly greater than the last one.
     * If the underlying clock fails to generate monotonically increasing timestamps, the generator will simply
     * increment the previous timestamp, and {@link #onDrift(long, long)} will be invoked.
     * <p/>
     * This implementation is inspired by {@code org.apache.cassandra.service.ClientState#getTimestamp()}.
     *
     * @param last the last timestamp generated by this generator, in microseconds.
     * @return the next timestamp to use, in microseconds.
     */
    protected long computeNext(long last) {
        long currentTick = clock.currentTimeMicros();
        if (last >= currentTick) {
            onDrift(currentTick, last);
            return last + 1;
        }
        return currentTick;
    }

    /**
     * Called when generated timestamps drift into the future compared to the underlying clock (in other words, if
     * {@code lastTimestamp >= currentTick}).
     * <p/>
     * This could happen if timestamps are requested faster than the clock granularity, or on a clock skew (for example
     * because of a leap second).
     *
     * @param currentTick   the current clock tick, in microseconds.
     * @param lastTimestamp the last timestamp that was generated, in microseconds.
     */
    protected abstract void onDrift(long currentTick, long lastTimestamp);
}
