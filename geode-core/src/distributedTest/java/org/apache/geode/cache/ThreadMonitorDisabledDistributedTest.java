/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache;

import static java.util.Arrays.asList;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.THREAD_MONITOR_ENABLED;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedRule.getLocators;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedReference;

@SuppressWarnings("serial")
public class ThreadMonitorDisabledDistributedTest implements Serializable {
  public static final String REGION_NAME = "testRegion";

  @Rule
  public DistributedReference<Cache> cache = new DistributedReference<>();

  @Before
  public void setUp() {
    Properties configProperties = new Properties();
    configProperties.setProperty(LOCATORS, getLocators());
    configProperties.setProperty(THREAD_MONITOR_ENABLED, "false");

    for (VM vm : asList(getVM(0), getVM(1))) {
      vm.invoke(() -> {
        cache.set(new CacheFactory(configProperties).create());
        RegionFactory<Object, Object> factory =
            cache.get().createRegionFactory(RegionShortcut.REPLICATE);
        factory.create(REGION_NAME);
      });
    }
  }

  /**
   * With thread-monitor-enabled==false the p2p reader died with
   * a NullPointerException. This test verifies that p2p messaging
   * works with thread monitoring disabled.
   */
  @Test
  public void regionPutWorksWithThreadMonitorDisabled() {
    for (VM vm : asList(getVM(0))) {
      vm.invoke(() -> {
        Region<Object, Object> region = cache.get().getRegion(REGION_NAME);
        region.put("key", "value");
      });
    }
    for (VM vm : asList(getVM(0), getVM(1))) {
      vm.invoke(() -> {
        Region<Object, Object> region = cache.get().getRegion(REGION_NAME);
        assertThat(region.get("key")).isEqualTo("value");
      });
    }
  }
}
