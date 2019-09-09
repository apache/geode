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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import util.TestException;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PdxTypeGenerationDUnitTest {

  private MemberVM locator, server1, server2;
  private static final int numOfTypes = 15;
  private static final int numOfEnums = 10;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void before() {
    Properties props = new Properties();
    props.setProperty("log-level", "WARN");

    locator = cluster.startLocatorVM(0, props);

    int locatorPort1 = locator.getPort();
    server1 = cluster.startServerVM(1,
        x -> x.withProperties(props).withConnectionToLocator(locatorPort1));

    int locatorPort2 = locator.getPort();
    server2 = cluster.startServerVM(2,
        x -> x.withProperties(props).withConnectionToLocator(locatorPort2));
  }

  @Test
  public void testLocalMapsRecoveredAfterServerRestart() {
    createPdxOnServer(server1, numOfTypes, numOfEnums);

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      PeerTypeRegistration registration =
          (PeerTypeRegistration) (cache.getPdxRegistry().getTypeRegistration());

      assertThat(registration.getLocalSize()).isEqualTo(numOfTypes + numOfEnums);
      assertThat(registration.getTypeToIdSize()).isEqualTo(0);
      assertThat(registration.getEnumToIdSize()).isEqualTo(0);

    });

    server2.stop(false);
    Properties props = new Properties();
    props.setProperty("log-level", "WARN");
    int locatorPort1 = locator.getPort();
    server2 = cluster.startServerVM(2,
        x -> x.withProperties(props).withConnectionToLocator(locatorPort1));

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      PeerTypeRegistration registration =
          (PeerTypeRegistration) (cache.getPdxRegistry().getTypeRegistration());

      assertThat(registration.getLocalSize()).isEqualTo(numOfTypes + numOfEnums);
      assertThat(registration.getTypeToIdSize()).isEqualTo(numOfTypes);
      assertThat(registration.getEnumToIdSize()).isEqualTo(numOfEnums);
    });
  }

  @Test
  public void definingNewTypeUpdatesLocalMaps() {
    createPdxOnServer(server1, numOfTypes, numOfEnums);

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      PeerTypeRegistration registration =
          (PeerTypeRegistration) (cache.getPdxRegistry().getTypeRegistration());

      assertThat(registration.getLocalSize()).isEqualTo(numOfTypes + numOfEnums);
      assertThat(registration.getTypeToIdSize()).isEqualTo(0);
      assertThat(registration.getEnumToIdSize()).isEqualTo(0);

      // Creating a new PdxType to trigger the pending local maps to be flushed
      JSONFormatter.fromJSON("{\"fieldName\": \"value\"}");

      assertThat(registration.getLocalSize()).isEqualTo(numOfTypes + numOfEnums + 1);
      assertThat(registration.getTypeToIdSize()).isEqualTo(numOfTypes + 1);
      assertThat(registration.getEnumToIdSize()).isEqualTo(numOfEnums);
    });
  }

  @Test
  public void testNoConflictsWhenGeneratingPdxTypesFromJSONOnMultipleServers() {
    int repeats = 10000;

    AsyncInvocation invocation1 = server1.invokeAsync(() -> {
      for (int i = 0; i < repeats; ++i) {
        JSONFormatter.fromJSON("{\"counter" + i + "\": " + i + "}");
      }
    });
    AsyncInvocation invocation2 = server2.invokeAsync(() -> {
      for (int i = 0; i < repeats; ++i) {
        JSONFormatter.fromJSON("{\"counter" + i + "\": " + i + "}");
      }
    });

    try {
      invocation1.await();
      invocation2.await();
    } catch (Exception ex) {
      throw new TestException("Exception while awaiting async invocation: " + ex);
    }

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      int numberOfTypesInRegion = cache.getPdxRegistry().getTypeRegistration().getLocalSize();
      int numberOfTypesInLocalMap =
          ((PeerTypeRegistration) cache.getPdxRegistry().getTypeRegistration()).getTypeToIdSize();

      assertThat(numberOfTypesInRegion)
          .withFailMessage("Expected number of PdxTypes in region to be %s but was %s",
              repeats, numberOfTypesInRegion)
          .isEqualTo(repeats);

      assertThat(numberOfTypesInLocalMap)
          .withFailMessage("Expected number of PdxTypes in local map to be %s but was %s",
              repeats, numberOfTypesInLocalMap)
          .isEqualTo(repeats);
    });

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      int numberOfTypesInRegion = cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH).size();
      int numberOfTypesInLocalMap =
          ((PeerTypeRegistration) cache.getPdxRegistry().getTypeRegistration()).getTypeToIdSize();

      assertThat(numberOfTypesInRegion)
          .withFailMessage("Expected number of PdxTypes in region to be %s but was %s",
              repeats, numberOfTypesInRegion)
          .isEqualTo(repeats);

      assertThat(numberOfTypesInLocalMap)
          .withFailMessage("Expected number of PdxTypes in local map to be %s but was %s",
              repeats, numberOfTypesInLocalMap)
          .isEqualTo(repeats);
    });
  }

  @Test
  public void testEnumsAndPdxTypesCreatedOnClientAreEnteredIntoTypeRegistry() throws Exception {
    final String regionName = "regionName";
    server1.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory().setDataPolicy(
          DataPolicy.REPLICATE).create(regionName);
    });
    server2.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory().setDataPolicy(
          DataPolicy.REPLICATE).create(regionName);
    });
    int port = locator.getPort();

    Properties props = new Properties();
    props.setProperty("log-level", "WARN");
    ClientVM client = cluster.startClientVM(3,
        cf -> cf.withLocatorConnection(port).withPoolSubscription(true).withProperties(props));

    client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(regionName);

      for (int i = 0; i < numOfTypes; ++i) {
        JSONFormatter.fromJSON("{\"counter" + i + "\": " + i + "}");
      }
      for (int i = 0; i < numOfEnums; ++i) {
        cache.createPdxEnum("ClassName", "EnumName" + i, i);
      }
    });

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      int numberOfTypesInRegion = cache.getPdxRegistry().getTypeRegistration().getLocalSize();

      assertThat(numberOfTypesInRegion)
          .withFailMessage("Expected number of PdxTypes and Enums in region to be %s but was %s",
              numOfEnums, numberOfTypesInRegion)
          .isEqualTo(numOfTypes + numOfEnums);
    });
  }

  private void createPdxOnServer(MemberVM server, int numOfTypes, int numOfEnums) {
    server.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();

      for (int i = 0; i < numOfTypes; ++i) {
        JSONFormatter.fromJSON("{\"counter" + i + "\": " + i + "}");
      }
      for (int i = 0; i < numOfEnums; ++i) {
        cache.createPdxEnum("ClassName", "EnumName" + i, i);
      }
      PeerTypeRegistration registration =
          (PeerTypeRegistration) (cache.getPdxRegistry().getTypeRegistration());

      assertThat(registration.getLocalSize()).isEqualTo(numOfTypes + numOfEnums);
      assertThat(registration.getTypeToIdSize()).isEqualTo(numOfTypes);
      assertThat(registration.getEnumToIdSize()).isEqualTo(numOfEnums);
    });
  }

}
