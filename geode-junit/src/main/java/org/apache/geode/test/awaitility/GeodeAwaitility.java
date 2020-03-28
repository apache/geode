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
package org.apache.geode.test.awaitility;

import static java.lang.Long.getLong;

import java.time.Duration;

import org.awaitility.core.ConditionFactory;
import org.mockito.Mockito;

/**
 * Utility to set consistent defaults for {@link org.awaitility.Awaitility} calls for all Geode
 * tests.
 */
public class GeodeAwaitility {

  /**
   * System property with value in seconds which will override the await timeout.
   */
  public static final String TIMEOUT_SECONDS_PROPERTY = "GEODE_AWAITILITY_TIMEOUT_SECONDS";

  private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration POLL_INTERVAL = Duration.ofMillis(50);
  private static final Duration POLL_DELAY = Duration.ofMillis(100);

  /**
   * Start building an await statement using Geode's default testing timeout.
   *
   * @return a {@link ConditionFactory} that is a builder for the await
   *
   * @see org.awaitility.Awaitility#await()
   */
  public static ConditionFactory await() {
    return await(null);
  }

  /**
   * Start building an await statement using Geode's default test timeout.
   *
   * @param alias a name for this await, if you test has multiple await statements
   *
   * @return a {@link ConditionFactory} that is a builder for the await
   *
   * @see org.awaitility.Awaitility#await(String)
   */
  public static ConditionFactory await(String alias) {
    return org.awaitility.Awaitility.await(alias)
        .atMost(getTimeout())
        .pollDelay(POLL_DELAY)
        .pollInterval(POLL_INTERVAL);
  }

  /**
   * Gets the timeout value as a {@link Duration}.
   *
   * <p>
   * One use of this is with {@link Mockito#timeout(long)}:
   *
   * <pre>
   * import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
   *
   * private static final long TIMEOUT = getTimeout().toMillis();
   *
   * {@literal @}Test
   * public void test() {
   * ...
   * ArgumentCaptor<AlertDetails> alertDetailsCaptor = ArgumentCaptor.forClass(AlertDetails.class);
   * verify(messageListener, timeout(TIMEOUT)).created(alertDetailsCaptor.capture());
   * }
   *
   * <pre>
   *
   * @return the current timeout value as a {@code Duration}
   */
  public static Duration getTimeout() {
    return Duration.ofSeconds(getLong(TIMEOUT_SECONDS_PROPERTY, DEFAULT_TIMEOUT.getSeconds()));
  }
}
