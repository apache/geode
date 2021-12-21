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
package org.apache.geode.distributed.internal;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.membership.api.MembershipLocatorStatistics;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

/**
 * This class maintains statistics for the locator
 *
 * @since GemFire 5.7
 */
public class LocatorStats implements MembershipLocatorStatistics {
  @Immutable
  private static final StatisticsType type;

  private static final String KNOWN_LOCATORS = LOCATORS; // gauge
  private static final String REQUESTS_TO_LOCATOR = "locatorRequests"; // counter
  private static final String RESPONSES_FROM_LOCATOR = "locatorResponses"; // counter
  private static final String ENDPOINTS_KNOWN = "servers"; // gauge
  private static final String REQUESTS_IN_PROGRESS = "requestsInProgress"; // gauge
  private static final String REQUEST_TIME = "requestProcessingTime"; // counter
  private static final String RESPONSE_TIME = "responseProcessingTime"; // counter
  private static final String SERVER_LOAD_UPDATES = "serverLoadUpdates"; // counter

  private final AtomicInteger known_locators = new AtomicInteger();
  private final AtomicLong requests_to_locator = new AtomicLong();
  private final AtomicLong requestTime = new AtomicLong();
  private final AtomicLong responseTime = new AtomicLong();
  private final AtomicLong responses_from_locator = new AtomicLong();
  private final AtomicInteger endpoints_known = new AtomicInteger();
  private final AtomicInteger requestsInProgress = new AtomicInteger();
  private final AtomicLong serverLoadUpdates = new AtomicLong();


  private static final int _KNOWN_LOCATORS;
  private static final int _REQUESTS_TO_LOCATOR;
  private static final int _RESPONSES_FROM_LOCATOR;
  private static final int _ENDPOINTS_KNOWN;
  private static final int _REQUESTS_IN_PROGRESS;
  private static final int _REQUEST_TIME;
  private static final int _RESPONSE_TIME;
  private static final int _SERVER_LOAD_UPDATES;


  private Statistics _stats = null;

  static {
    String statName = "LocatorStats";
    String statDescription = "Statistics on the gemfire locator.";
    String serverThreadsDesc =
        "The number of location requests currently being processed by the thread pool.";
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    type = f.createType(statName, statDescription, new StatisticDescriptor[] {
        f.createIntGauge(KNOWN_LOCATORS, "Number of locators known to this locator", LOCATORS),
        f.createLongCounter(REQUESTS_TO_LOCATOR,
            "Number of requests this locator has received from clients", "requests"),
        f.createLongCounter(RESPONSES_FROM_LOCATOR,
            "Number of responses this locator has sent to clients", "responses"),
        f.createIntGauge(ENDPOINTS_KNOWN, "Number of servers this locator knows about", "servers"),
        f.createIntGauge(REQUESTS_IN_PROGRESS, serverThreadsDesc, "requests"),
        f.createLongCounter(REQUEST_TIME, "Time spent processing server location requests",
            "nanoseconds"),
        f.createLongCounter(RESPONSE_TIME, "Time spent sending location responses to clients",
            "nanoseconds"),
        f.createLongCounter(SERVER_LOAD_UPDATES,
            "Total number of times a server load update has been received.", "updates"),});

    _REQUESTS_IN_PROGRESS = type.nameToId(REQUESTS_IN_PROGRESS);
    _KNOWN_LOCATORS = type.nameToId(KNOWN_LOCATORS);
    _REQUESTS_TO_LOCATOR = type.nameToId(REQUESTS_TO_LOCATOR);
    _RESPONSES_FROM_LOCATOR = type.nameToId(RESPONSES_FROM_LOCATOR);
    _ENDPOINTS_KNOWN = type.nameToId(ENDPOINTS_KNOWN);
    _REQUEST_TIME = type.nameToId(REQUEST_TIME);
    _RESPONSE_TIME = type.nameToId(RESPONSE_TIME);
    _SERVER_LOAD_UPDATES = type.nameToId(SERVER_LOAD_UPDATES);
  }

  /**
   * Creates a new <code>LocatorStats</code> and registers itself with the given statistics factory.
   */
  public LocatorStats() {}

  /**
   * Called when the DS comes online so we can hookup the stats
   */
  public void hookupStats(StatisticsFactory f, String name) {
    if (_stats == null) {
      _stats = f.createAtomicStatistics(type, name);
      setLocatorCount(known_locators.get());
      setServerCount(endpoints_known.get());
      setLocatorRequests(requests_to_locator.get());
      setLocatorResponses(responses_from_locator.get());
      setServerLoadUpdates(serverLoadUpdates.get());
    }
  }

  @Override
  public long getStatTime() {
    return DistributionStats.getStatTime();
  }


  public void setServerCount(int sc) {
    if (_stats == null) {
      endpoints_known.set(sc);
    } else {
      _stats.setInt(_ENDPOINTS_KNOWN, sc);
    }
  }

  public void setLocatorCount(int lc) {
    if (_stats == null) {
      known_locators.set(lc);
    } else {
      _stats.setInt(_KNOWN_LOCATORS, lc);
    }
  }

  @Override
  public void endLocatorRequest(long startTime) {
    long took = DistributionStats.getStatTime() - startTime;
    if (_stats == null) {
      requests_to_locator.incrementAndGet();
      if (took > 0) {
        requestTime.getAndAdd(took);
      }
    } else {
      _stats.incLong(_REQUESTS_TO_LOCATOR, 1);
      if (took > 0) {
        _stats.incLong(_REQUEST_TIME, took);
      }
    }
  }

  @Override
  public void endLocatorResponse(long startTime) {
    long took = DistributionStats.getStatTime() - startTime;
    if (_stats == null) {
      responses_from_locator.incrementAndGet();
      if (took > 0) {
        responseTime.getAndAdd(took);
      }
    } else {
      _stats.incLong(_RESPONSES_FROM_LOCATOR, 1);
      if (took > 0) {
        _stats.incLong(_RESPONSE_TIME, took);
      }
    }
  }



  public void setLocatorRequests(long rl) {
    if (_stats == null) {
      requests_to_locator.set(rl);
    } else {
      _stats.setLong(_REQUESTS_TO_LOCATOR, rl);
    }
  }

  public void setLocatorResponses(long rl) {
    if (_stats == null) {
      responses_from_locator.set(rl);
    } else {
      _stats.setLong(_RESPONSES_FROM_LOCATOR, rl);
    }
  }

  public void setServerLoadUpdates(long v) {
    if (_stats == null) {
      serverLoadUpdates.set(v);
    } else {
      _stats.setLong(_SERVER_LOAD_UPDATES, v);
    }
  }

  public void incServerLoadUpdates() {
    if (_stats == null) {
      serverLoadUpdates.incrementAndGet();
    } else {
      _stats.incLong(_SERVER_LOAD_UPDATES, 1);
    }
  }

  public void incRequestInProgress(int threads) {
    if (_stats != null) {
      _stats.incInt(_REQUESTS_IN_PROGRESS, threads);
    } else {
      requestsInProgress.getAndAdd(threads);
    }
  }

  public void close() {
    if (_stats != null) {
      _stats.close();
    }
  }
}
