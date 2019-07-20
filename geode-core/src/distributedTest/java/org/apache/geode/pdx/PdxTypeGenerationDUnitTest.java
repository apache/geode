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

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import util.TestException;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PdxTypeGenerationDUnitTest {

  private static final float TARGET_CONSISTENCY_FACTOR = 2f;

  private static final String FIELD_NAME_TO_REPLACE = "maxUrlLength";

  private MemberVM locator, server1, server2;

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
  public void testSortJSONFieldNamesDoesNotImpactPerformance() {
    final String fileName = "/org/apache/geode/pdx/jsonStrings/testJSON.txt";
    String jsonString = loadJSONFileAsString(fileName);

    server1.invoke(() -> {
      int repeats = 1000;
      long totalUnsortedTime = 0;
      long totalSortedTime = 0;

      // The first JSON document used to create a PdxInstance takes significantly longer than
      // successive ones, so a dummy document is used to trigger this first, longer creation process
      JSONFormatter.fromJSON("{\"testFieldName\": \"test\"}");
      for (int i = 0; i < repeats; ++i) {
        System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "false");

        String fieldOne = "counter" + 2 * i;

        long startTime = System.currentTimeMillis();
        JSONFormatter.fromJSON(jsonString.replace(FIELD_NAME_TO_REPLACE, fieldOne));
        totalUnsortedTime += (System.currentTimeMillis() - startTime);

        System.setProperty(JSONFormatter.SORT_JSON_FIELD_NAMES_PROPERTY, "true");

        String fieldTwo = "counter" + 2 * i + 1;

        startTime = System.currentTimeMillis();
        JSONFormatter.fromJSON(jsonString.replace(FIELD_NAME_TO_REPLACE, fieldTwo));
        totalSortedTime += (System.currentTimeMillis() - startTime);
      }
      float consistencyFactor = (float) totalUnsortedTime / (float) totalSortedTime;
      assertThat(consistencyFactor).withFailMessage(
          "Expected unsorted JSON to take no longer than %f times as long as sorted JSON. Was actually %f times.",
          TARGET_CONSISTENCY_FACTOR, consistencyFactor)
          .isLessThan(TARGET_CONSISTENCY_FACTOR);

      assertThat(1 / consistencyFactor).withFailMessage(
          "Expected sorted JSON to take no longer than %f times as long as unsorted JSON. Was actually %f times.",
          TARGET_CONSISTENCY_FACTOR, 1 / consistencyFactor)
          .isLessThan(TARGET_CONSISTENCY_FACTOR);
    });
  }

  @Test
  public void testLocalPdxTypeMapRecoveredAfterServerRestart() {
    final String fileName = "/org/apache/geode/pdx/jsonStrings/testJSON.txt";
    String jsonString = loadJSONFileAsString(fileName);
    final int addedTypes = 10;

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();

      // Creating a PdxType from the unmodified JSON document so that the same document can be used
      // later to confirm that the PdxType associated with it is still registered
      JSONFormatter.fromJSON(jsonString);

      // Creating more PdxTypes to allow us to confirm that they are all recovered when we attempt
      // to create a PdxType after restarting the server
      for (int i = 0; i < addedTypes; ++i) {
        String replacementField = "counter" + i;
        String modifiedJSON = jsonString.replace(FIELD_NAME_TO_REPLACE, replacementField);
        JSONFormatter.fromJSON(modifiedJSON);
      }
      PeerTypeRegistration registration =
          (PeerTypeRegistration) (cache.getPdxRegistry().getTypeRegistration());

      assertThat(registration.getLocalSize()).isEqualTo(addedTypes + 1);
      assertThat(registration.getTypeToIdSize()).isEqualTo(addedTypes + 1);
    });

    server1.stop(false);
    Properties props = new Properties();
    props.setProperty("log-level", "WARN");
    int locatorPort1 = locator.getPort();
    server1 = cluster.startServerVM(1,
        x -> x.withProperties(props).withConnectionToLocator(locatorPort1));

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();

      PeerTypeRegistration registration =
          (PeerTypeRegistration) (cache.getPdxRegistry().getTypeRegistration());

      assertThat(registration.getLocalSize()).isEqualTo(addedTypes + 1);
      assertThat(registration.getTypeToIdSize()).isEqualTo(0);

      // Attempting to create a PdxType that already exists in the /PdxTypes region in order to
      // trigger rebuilding of the TypeToId map in PeerTypeRegistration
      JSONFormatter.fromJSON(jsonString);

      assertThat(registration.getLocalSize()).isEqualTo(addedTypes + 1);
      assertThat(registration.getTypeToIdSize()).isEqualTo(addedTypes + 1);
    });
  }

  @Test
  public void testNoConflictsWhenGeneratingPdxTypesFromJSONOnMultipleServers() {
    int repeats = 1000;
    AsyncInvocation invocation1 = null;
    AsyncInvocation invocation2 = null;

    invocation1 = server1.invokeAsync(() -> {
      for (int i = 0; i < repeats; ++i) {
        JSONFormatter.fromJSON("{\"fieldName" + i + "\": \"value\"}");
      }
    });
    invocation2 = server2.invokeAsync(() -> {
      for (int i = 0; i < repeats; ++i) {
        JSONFormatter.fromJSON("{\"fieldName" + i + "\": \"value\"}");
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
      assertThat(cache).isNotNull();
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

    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
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
  public void testPdxTypesGeneratedFromJSONOnClientAreEnteredIntoTypeRegistry() throws Exception {
    server1.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory().setDataPolicy(
          DataPolicy.REPLICATE).create("Test");
    });
    server2.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory().setDataPolicy(
          DataPolicy.REPLICATE).create("Test");
    });
    int port = locator.getPort();
    int serverPort = server1.getPort();

    Properties props = new Properties();
    props.setProperty("log-level", "WARN");
    ClientVM client = cluster.startClientVM(3,
        cf -> cf.withLocatorConnection(port).withPoolSubscription(true).withProperties(props));

    int entries = 10;
    client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      Region region =
          cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create("Test");
      String field;
      String jsonString;

      for (int i = 0; i < entries; ++i) {
        field = "\"counter" + i + "\": " + i;

        jsonString = "{" + field + "}";

        JSONFormatter.fromJSON(jsonString);
      }
    });

    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      int numberOfTypesInRegion = cache.getRegion(PeerTypeRegistration.REGION_FULL_PATH).size();
      int numberOfTypesInLocalMap =
          ((PeerTypeRegistration) cache.getPdxRegistry().getTypeRegistration()).getTypeToIdSize();

      assertThat(numberOfTypesInRegion)
          .withFailMessage("Expected number of PdxTypes in region to be %s but was %s",
              entries, numberOfTypesInRegion)
          .isEqualTo(entries);

      assertThat(numberOfTypesInLocalMap)
          .withFailMessage("Expected number of PdxTypes in local map to be %s but was %s",
              entries, numberOfTypesInLocalMap)
          .isEqualTo(entries);
    });
  }

  private String loadJSONFileAsString(String fileName) {
    Path filePath = loadTestResourcePath(fileName);
    String jsonString;
    try {
      jsonString = new String(Files.readAllBytes(filePath));
    } catch (IOException ex) {
      throw new TestException(ex.getMessage());
    }
    return jsonString;
  }

  private List<String> loadJSONFileAsList(String fileName) {
    Path filePath = loadTestResourcePath(fileName);
    List<String> jsonLines;
    try {
      jsonLines = new ArrayList<>(Files.readAllLines(filePath));
    } catch (IOException ex) {
      throw new TestException(ex.getMessage());
    }
    return jsonLines;
  }

  private static String buildJSONString(List<String> jsonLines) {
    StringBuilder jsonString = new StringBuilder();
    for (int i = 0; i < jsonLines.size(); ++i) {
      String line = jsonLines.get(i);
      jsonString.append(line + "\n");
    }
    return jsonString.toString();
  }

  private Path loadTestResourcePath(String fileName) {
    Path filePath = createTempFileFromResource(getClass(), fileName).toPath();
    assertThat(filePath).isNotNull();

    return filePath;
  }

}
