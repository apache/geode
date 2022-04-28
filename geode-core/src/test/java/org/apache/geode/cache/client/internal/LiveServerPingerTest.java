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
package org.apache.geode.cache.client.internal;


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.geode.util.internal.GeodeGlossary;

public class LiveServerPingerTest {

  private InternalPool pool;

  private LiveServerPinger lsp;

  private static long PING_INTERVAL = 10L;
  private static long DEFAULT_PING_INTERVAL_NANOS = 5000000L;

  private static long CONFIG_PING_INTERVAL_NANOS = 1000000L;


  @BeforeEach
  public void init() throws Exception {
    System.setProperty(
        GeodeGlossary.GEMFIRE_PREFIX + "LiveServerPinger.INITIAL_DELAY_MULTIPLIER_IN_MILLISECONDS",
        "1");

    pool = mock(InternalPool.class);
    when(pool.getPingInterval()).thenReturn(PING_INTERVAL);

    lsp = new LiveServerPinger(pool);
  }

  @Test
  public void testInitialDelay() throws Exception {

    assertThat(lsp.calculateInitialDelay()).isEqualTo(DEFAULT_PING_INTERVAL_NANOS);
    assertThat(lsp.calculateInitialDelay())
        .isEqualTo(DEFAULT_PING_INTERVAL_NANOS + CONFIG_PING_INTERVAL_NANOS);
    assertThat(lsp.calculateInitialDelay())
        .isEqualTo(DEFAULT_PING_INTERVAL_NANOS + (2 * CONFIG_PING_INTERVAL_NANOS));
    assertThat(lsp.calculateInitialDelay())
        .isEqualTo(DEFAULT_PING_INTERVAL_NANOS + (3 * CONFIG_PING_INTERVAL_NANOS));

  }

  @Test
  public void testInitialDelayWithReset() throws Exception {

    assertThat(lsp.calculateInitialDelay()).isEqualTo(DEFAULT_PING_INTERVAL_NANOS);
    assertThat(lsp.calculateInitialDelay())
        .isEqualTo(DEFAULT_PING_INTERVAL_NANOS + CONFIG_PING_INTERVAL_NANOS);
    assertThat(lsp.calculateInitialDelay())
        .isEqualTo(DEFAULT_PING_INTERVAL_NANOS + (2 * CONFIG_PING_INTERVAL_NANOS));
    lsp.resetInitialDelay();
    assertThat(lsp.calculateInitialDelay())
        .isEqualTo(DEFAULT_PING_INTERVAL_NANOS);

  }

}
