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
import static org.apache.geode.test.dunit.DistributedTestUtils.getLocatorPort;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class LocatorPortClusterExampleTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  private static InternalCache cache;

  private Properties config;

  @Before
  public void setUp() throws Exception {
    config = new Properties();
    config.put(LOCATORS, getHostName() + "[" + getLocatorPort() + "]");

    cache = (InternalCache) new CacheFactory(config).create();

    for (VM vm : getAllVMs()) {
      vm.invoke(() -> {
        cache = (InternalCache) new CacheFactory(config).create();
      });
    }
  }

  @After
  public void tearDown() throws Exception {
    cache = null;
    for (VM vm : getAllVMs()) {
      vm.invoke(() -> cache = null);
    }
  }

  @Test
  public void clusterHasDUnitVMCountPlusTwoByDefault() {
    int dunitVMCount = getVMCount();
    assertThat(cache.getDistributionManager().getViewMembers()).hasSize(dunitVMCount + 2);
  }
}
