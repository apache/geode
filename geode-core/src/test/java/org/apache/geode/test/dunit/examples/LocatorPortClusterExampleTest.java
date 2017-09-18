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
package org.apache.geode.test.dunit.examples;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.dunit.DistributedTestUtils.getDUnitLocatorPort;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedDisconnectRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class LocatorPortClusterExampleTest implements Serializable {

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public DistributedDisconnectRule disconnectRule =
      new DistributedDisconnectRule.Builder().disconnectAfter(true).build();

  private static InternalCache cache;

  private Properties config;

  @Before
  public void setUp() throws Exception {
    config = new Properties();
    config.put(LOCATORS, Host.getHost(0).getHostName() + "[" + getDUnitLocatorPort() + "]");

    cache = (InternalCache) new CacheFactory(config).create();

    for (VM vm : Host.getHost(0).getAllVMs()) {
      vm.invoke(() -> {
        cache = (InternalCache) new CacheFactory(config).create();
      });
    }
  }

  @Test
  public void clusterHasSixVMsByDefault() throws Exception {
    assertThat(cache.getDistributionManager().getViewMembers()).hasSize(6);
    assertThat(Host.getHost(0).getVMCount()).isEqualTo(4);
    assertThat(Host.getHost(0).getAllVMs()).hasSize(4);
  }
}
