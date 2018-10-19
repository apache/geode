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


import java.util.concurrent.TimeUnit;

import org.awaitility.Duration;
import org.awaitility.core.ConditionFactory;

/**
 * Utility to set consistent defaults for {@link org.awaitility.Awaitility} calls for all geode
 * tests
 */
public class GeodeAwaitility {

  public static final Integer TIMEOUT_SECONDS =
      Integer.getInteger("GEODE_AWAITILITY_TIMEOUT_SECONDS", 300);
  public static final Duration TIMEOUT = new Duration(TIMEOUT_SECONDS, TimeUnit.SECONDS);
  public static final Duration POLL_INTERVAL = new Duration(50, TimeUnit.MILLISECONDS);
  public static final Duration POLL_DELAY = Duration.ONE_HUNDRED_MILLISECONDS;

  /**
   * Start building an await statement using Geode's default test timeout
   *
   * @return a {@link ConditionFactory} that is a builder for the await
   * @see org.awaitility.Awaitility#await()
   */
  public static ConditionFactory await() {
    return await(null);
  }

  /**
   * Start building an await statement using Geode's default test timeout
   *
   * @param alias A name for this await, if you test has multiple await statements
   *
   * @return a {@link ConditionFactory} that is a builder for the await
   * @see org.awaitility.Awaitility#await(String)
   */
  public static ConditionFactory await(String alias) {
    return org.awaitility.Awaitility.await(alias)
        .atMost(TIMEOUT)
        .pollDelay(POLL_DELAY)
        .pollInterval(POLL_INTERVAL);
  }
}
