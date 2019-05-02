/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache.wan;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableSet;

import java.util.HashSet;
import java.util.Set;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.Statistics;
import org.apache.geode.annotations.Immutable;

public class GatewayReceiverMeters {
  @Immutable
  private static final Set<String> meterWhitelist;

  static {
    Set<String> whitelist = new HashSet<>();
    whitelist.add("cache.gatewayreceiver.events.received");
    meterWhitelist = unmodifiableSet(whitelist);
  }

  private final Counter eventsReceivedCounter;

  public GatewayReceiverMeters(MeterRegistry meterRegistry, GatewayReceiverStats stats) {
    eventsReceivedCounter = createIntCounter(meterRegistry, "cache.gatewayreceiver.events.received",
        stats.getStats(), stats.getEventsReceivedId());
  }

  public static Set<String> whitelist() {
    return meterWhitelist;
  }

  private Counter createIntCounter(MeterRegistry registry, String meterName, Statistics stats,
      int statId) {
    if (!meterWhitelist.contains(meterName)) {
      throw new IllegalStateException(
          format("Meter name '%s' is not whitelisted in %s", meterName, getClass().getSimpleName()));
    }
    Counter counter = Counter.builder(meterName)
        .description(stats.getType().getDescription())
        .register(registry);

    // NOTE: This will convert the double count to an int, which will roll from MAX_INT to MIN_INT.
    stats.setIntSupplier(statId, () -> (int) counter.count());
    return counter;
  }

  public Counter eventsReceivedCounter() {
    return eventsReceivedCounter;
  }

}
