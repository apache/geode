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
package org.apache.geode.modules;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import example.test.pojo.model.InnerPojo;
import example.test.pojo.model.TestPojo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class BasicOperationsAcceptanceTest extends AbstractDockerizedAcceptanceTest {

  public BasicOperationsAcceptanceTest(String launchCommand)
      throws IOException, InterruptedException {
    launch(launchCommand);
  }

  @Before
  public void setup() {
    GfshScript.of(getLocatorGFSHConnectionString(),
        "create region --name=testRegion --type=PARTITION").execute(gfshRule);
  }

  @After
  public void teardown() {
    GfshScript.of(getLocatorGFSHConnectionString(), "destroy region --name=testRegion")
        .execute(gfshRule);
  }

  @Test
  public void testSimplePut() {
    ClientCache clientCache =
        new ClientCacheFactory().set("mcast-port", "0").addPoolServer(host, serverPort)
            .create();
    Region<Object, Object> partitionedRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("testRegion");
    partitionedRegion.put("First", "Entry");

    assertThat(partitionedRegion.keySetOnServer().size()).isEqualTo(1);

    clientCache.close();
  }

  @Test
  public void testSimpleGet() {
    ClientCache clientCache =
        new ClientCacheFactory().set("mcast-port", "0").addPoolServer(host, serverPort)
            .create();
    Region<Object, Object> partitionedRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create("testRegion");
    partitionedRegion.put("First", "Entry");

    assertThat(partitionedRegion.get("First")).isNotNull().isEqualTo("Entry");
    assertThat(partitionedRegion.get("UnknownKey")).isNull();

    clientCache.close();
  }

  @Test
  public void testSimpleDestroy() {
    ClientCache clientCache =
        new ClientCacheFactory().set("mcast-port", "0").addPoolServer(host, serverPort)
            .create();
    Region<Object, Object> partitionedRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create("testRegion");
    partitionedRegion.put("First", "Entry");

    assertThat(partitionedRegion.get("First")).isNotNull().isEqualTo("Entry");
    partitionedRegion.destroy("First");
    assertThat(partitionedRegion.get("First")).isNull();

    clientCache.close();
  }

  @Test
  public void testPojoPutAndGet() {
    ClientCache clientCache =
        new ClientCacheFactory().set("mcast-port", "0").addPoolServer(host, serverPort)
            .setPdxSerializer(
                new ReflectionBasedAutoSerializer("example.test.pojo.model.*"))
            .create();
    Region<Object, Object> partitionedRegion =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create("testRegion");
    partitionedRegion.put(1, new TestPojo("Dude", "Whatsup", new InnerPojo(23)));

    assertThat(partitionedRegion.keySetOnServer().size()).isEqualTo(1);
    assertThat(partitionedRegion.get(1)).isNotNull()
        .isEqualTo(new TestPojo("Dude", "Whatsup", new InnerPojo(23)));

    clientCache.close();
  }
}
