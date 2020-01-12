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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.CONSERVE_SOCKETS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.Repeat;
import org.apache.geode.test.junit.rules.RepeatRule;

/**
 * A test of 46438 - missing response to an update attributes message
 *
 * see bugs #50785 and #46438
 */

public class ConnectDisconnectDUnitTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();

  private static int count;

  @Rule
  public RepeatRule repeat = new RepeatRule();

  @BeforeClass
  public static void beforeClass() {
    count = 0;
  }

  @Before
  public void before() {
    count++;
  }

  @After
  public void after() {
    disconnectAllFromDS();

  }

  @AfterClass
  public static void afterClass() {
    assertThat(count).isEqualTo(20);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(CONSERVE_SOCKETS, "false");
    return props;
  }

  /**
   * This test creates 4 vms and starts a cache in each VM. If that doesn't hang, it destroys the DS
   * in all vms and recreates the cache.
   */
  @Test
  @Repeat(20)
  public void testManyConnectsAndDisconnects() throws Exception {
    logger.info("Test run: {}", count);

    int numVMs = 4;
    VM[] vms = new VM[numVMs];

    for (int i = 0; i < numVMs; i++) {
      vms[i] = Host.getHost(0).getVM(i);
    }

    AsyncInvocation[] asyncs = new AsyncInvocation[numVMs];
    for (int i = 0; i < numVMs; i++) {
      asyncs[i] = vms[i].invokeAsync(new SerializableRunnable("Create a cache") {
        @Override
        public void run() {
          getCache();
        }
      });
    }

    for (int i = 0; i < numVMs; i++) {
      asyncs[i].await();
    }
  }

}
