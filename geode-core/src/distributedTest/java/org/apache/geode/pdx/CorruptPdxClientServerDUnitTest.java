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
package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getClientCache;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOError;
import java.io.IOException;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.ExportLocalDataFunction;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.pdx.internal.PdxWriterImpl;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class CorruptPdxClientServerDUnitTest {

  public static final String REGION_NAME = "testSimplePdx";
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  public CorruptPdxClientServerDUnitTest() {
    super();
  }

  @Test
  public void testSimplePut() throws Exception {
    final MemberVM locator = cluster.startLocatorVM(0);
    final int locatorPort = locator.getPort();

    Properties properties = new Properties();
    properties.put(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.**");
    properties.put(LOCATORS, String.format("localhost[%d]", locatorPort));

    final MemberVM server1 = cluster.startServerVM(1, properties);
    final MemberVM server2 = cluster.startServerVM(2, properties);
    final ClientVM client = cluster.startClientVM(3,
        cf -> cf.withLocatorConnection(locatorPort));

    final SerializableRunnableIF createServerRegion = () -> {
      getCache().createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).create(REGION_NAME);
    };
    server1.invoke(createServerRegion);
    server2.invoke(createServerRegion);
    server1.invoke(() -> {
      getCache().getQueryService().createIndex("index1", "myInt", Region.SEPARATOR + REGION_NAME);
    });

    client.invoke(() -> {
      final ClientCache cache = getClientCache();
      final Region<Object, Object> region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("testSimplePdx");
      region.put(1, new SimpleClass(1, (byte) 1));
      region.put(2, new SimpleClass(2, (byte) 2));
      PdxWriterImpl.breakIt = true;
      region.put(3, new SimpleClass(3, (byte) 3));
      region.put(4, new SimpleClass(4, (byte) 4));
      PdxWriterImpl.breakIt = false;
      region.put(5, new SimpleClass(5, (byte) 5));
      region.put(6, new SimpleClass(6, (byte) 6));
    });

    final SerializableRunnableIF checkValue = () -> {
      final Region<Object, Object> region = getServerRegion();
      assertThat(region.get(1)).isEqualTo(new SimpleClass(1, (byte) 1));
      assertThat(region.get(2)).isEqualTo(new SimpleClass(2, (byte) 2));
      assertThatThrownBy(() -> region.get(3)).hasRootCauseInstanceOf(IOException.class).hasRootCauseMessage("Unknown header byte 0");
      assertThatThrownBy(() -> region.get(4)).hasRootCauseInstanceOf(IOException.class).hasRootCauseMessage("Unknown header byte 0");
    };

    server1.invoke(checkValue);
    server2.invoke(checkValue);

    client.invoke(() -> {
      final Region<Object, Object> region = getClientCache().getRegion(REGION_NAME);
      ResultCollector resultCollector = FunctionService.onRegion(region)
          .setArguments("/dev/null")
          .execute(new ExportLocalDataFunction());
      resultCollector.getResult();

//      region.remove(3);
//      region.destroy(4);
    });

  }

  private static Region<Object, Object> getServerRegion() {
    return getCache().getRegion(REGION_NAME);
  }

}
