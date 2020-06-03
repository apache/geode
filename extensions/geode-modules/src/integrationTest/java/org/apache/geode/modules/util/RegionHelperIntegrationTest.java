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
package org.apache.geode.modules.util;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RegionHelperIntegrationTest {
  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withNoCacheServer().withRegion(RegionShortcut.REPLICATE, "test");

  @Test
  public void generateXml() throws Exception {
    Region region = server.getCache().getRegion(SEPARATOR + "test");
    region.put("key", "value");
    String cacheXml = RegionHelper.generateCacheXml(server.getCache());

    // make sure the generated cache.xml skips region entries
    assertThat(cacheXml).doesNotContain("<entry>")
        .doesNotContain("<string>key</string>")
        .doesNotContain("<string>value</string>");
  }
}
