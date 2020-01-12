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
package org.apache.geode.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;

public class ClientAccessToInternalStatsTest {

  @Test
  public void canGetLongCounterAsInteger() {
    try (ClientCache cache = new ClientCacheFactory().create()) {
      Region region = cache.createClientRegionFactory(ClientRegionShortcut.LOCAL).create("local");
      region.put("key", "value");
      Statistics[] stats = cache.getDistributedSystem().findStatisticsByTextId("cachePerfStats");
      assertThat(stats).hasSize(1);
      assertThat(stats[0].getInt("puts")).isEqualTo(1);
    }
  }
}
