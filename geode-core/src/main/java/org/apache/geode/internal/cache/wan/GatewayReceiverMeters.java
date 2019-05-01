package org.apache.geode.internal.cache.wan;

import static java.lang.String.format;

import java.util.HashSet;
import java.util.Set;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.Statistics;

public class GatewayReceiverMeters {
  private static final Set<String> meterWhitelist = new HashSet<String>() {
    {
      add("cache.gatewayreceiver.events.received");
    }
  };

  private final Counter eventsReceivedCounter;

  public GatewayReceiverMeters(MeterRegistry meterRegistry, GatewayReceiverStats stats) {
    eventsReceivedCounter = createIntCounter(meterRegistry, "cache.gatewayreceiver.events.received",
        stats.getStats(), stats.getEventsReceivedId());
  }

  public static Set<String> whitelist() {
    return meterWhitelist;
  }

  private Counter createIntCounter(MeterRegistry registry, String name, Statistics stats,
      int statId) {
    if (!meterWhitelist.contains(name)) {
      throw new IllegalStateException(
          format("Meter name '%s' is not whitelisted in %s", name, getClass().getSimpleName()));
    }
    Counter counter = Counter.builder(name)
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
