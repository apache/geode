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
 *
 */

package org.apache.geode.tools.pulse;

import static org.apache.geode.test.junit.rules.HttpResponseAssert.assertResponse;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.rules.GeodeHttpClientRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({PulseTest.class})
public class PulseDataExportTest {

  @Rule
  public ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withHttpService()
          .withRegion(RegionShortcut.REPLICATE, "regionA");

  @Rule
  public GeodeHttpClientRule client = new GeodeHttpClientRule(server::getHttpPort);

  @Before
  public void before() throws Exception {
    Region region = server.getCache().getRegion("regionA");
    region.put("key1", "value1");
    region.put("key2", "value2");
    region.put("key3", "value3");
  }

  @Test
  public void dataBrowserExportWorksAsExpected() throws Exception {
    client.loginToPulseAndVerify("admin", "admin");

    assertResponse(
        client.get("/pulse/dataBrowserExport", "query", "select * from /regionA a order by a"))
            .hasStatusCode(200)
            .hasResponseBody()
            .isEqualToIgnoringWhitespace(
                "{\"result\":[[\"java.lang.String\",\"value1\"],[\"java.lang.String\",\"value2\"],[\"java.lang.String\",\"value3\"]]}");
  }
}
