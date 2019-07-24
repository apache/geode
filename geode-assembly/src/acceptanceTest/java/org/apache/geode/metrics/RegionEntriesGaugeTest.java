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

package org.apache.geode.metrics;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.PROXY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import io.micrometer.core.instrument.Gauge;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.metrics.rules.MetricsPublishingServiceJarRule;
import org.apache.geode.metrics.rules.SingleFunctionJarRule;
import org.apache.geode.test.junit.categories.MetricsTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@Category(MetricsTest.class)
public class RegionEntriesGaugeTest {

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  // TODO: refactor to ServiceJarRule
  @Rule
  public MetricsPublishingServiceJarRule metricsPublishingServiceJarRule =
      new MetricsPublishingServiceJarRule("metrics-publishing-service.jar",
          SimpleMetricsPublishingService.class);

  // TODO: Inline the code that builds this jar
  @Rule
  public SingleFunctionJarRule functionJarRule =
      new SingleFunctionJarRule("function.jar", GetMemberRegionEntriesGaugeFunction.class);

  private static final String SPACE = " ";

  private final List<File> serverFolders = new ArrayList<>();
  private File folderForLocator;
  private ClientCache clientCache;
  private String connectToLocatorCommand;
  private String locatorString;

  @Before
  public void startMembers() throws Exception {
    int[] availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(5);
    int locatorPort = availablePorts[0];
    int locatorHttpPort = availablePorts[1];
    int locatorJmxPort = availablePorts[2];
    int serverPort1 = availablePorts[3];
    int serverPort2 = availablePorts[4];

    locatorString = "localhost[" + locatorPort + "]";

    folderForLocator = temporaryFolder.newFolder("locator");
    File folderForServer1 = temporaryFolder.newFolder("server1");
    File folderForServer2 = temporaryFolder.newFolder("server2");
    serverFolders.add(folderForServer1);
    serverFolders.add(folderForServer2);

    String startLocatorCommand = String.join(SPACE,
        "start locator",
        "--name=" + "locator",
        "--dir=" + folderForLocator.getAbsolutePath(),
        "--port=" + locatorPort,
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-http-port=" + locatorHttpPort,
        "--J=-Dgemfire.jmx-manager-port=" + locatorJmxPort);

    String startServer1Command = startServerCommand("server1", serverPort1, folderForServer1);
    String startServer2Command = startServerCommand("server2", serverPort2, folderForServer2);

    gfshRule.execute(startLocatorCommand, startServer1Command, startServer2Command);

    connectToLocatorCommand = "connect --locator=" + locatorString;

    String deployCommand = functionJarRule.deployCommand();
    String listFunctionsCommand = "list functions";

    gfshRule.execute(connectToLocatorCommand, deployCommand, listFunctionsCommand);

    clientCache = new ClientCacheFactory().addPoolLocator("localhost", locatorPort).create();
  }

  @After
  public void stopMembers() {
    clientCache.close();

    List<String> commands = new ArrayList<>();
    commands.add(connectToLocatorCommand);

    serverFolders.stream()
        .map(RegionEntriesGaugeTest::stopServerCommand)
        .forEach(commands::add);

    commands.add("stop locator --dir=" + folderForLocator.getAbsolutePath());

    gfshRule.execute(commands.toArray(new String[0]));
  }

  private static String stopServerCommand(File folder) {
    return "stop server --dir=" + folder.getAbsolutePath();
  }

  @Test
  public void regionEntriesGaugeShowsCountOfReplicateRegionValuesInServer() {
    Region<String, String> otherRegion = createRegion(REPLICATE.name(), "otherRegionName");
    otherRegion.put("other-region-key", "other-region-value");

    String regionName = "replicateRegion";
    Region<String, String> regionOfInterest = createRegion(REPLICATE.name(), regionName);

    List<String> keys = asList("a", "b", "c", "d", "e", "f", "g", "h");
    for (String key : keys) {
      regionOfInterest.put(key, key);
    }
    regionOfInterest.destroy(keys.get(0));
    int expectedNumberOfEntries = keys.size() - 1;

    String getGaugeValueCommand = memberRegionEntryGaugeValueCommand(regionName);

    await().untilAsserted(() -> {
      GfshExecution execution = gfshRule.execute(connectToLocatorCommand, getGaugeValueCommand);
      List<Integer> entryCounts = linesOf(execution.getOutputText())
          .filter(s -> s.startsWith("server"))
          .map(RegionEntriesGaugeTest::extractEntryCount)
          .collect(toList());

      assertThat(entryCounts)
          .as("Number of entries reported by each server")
          .allMatch(i -> i == expectedNumberOfEntries, "equals " + expectedNumberOfEntries);
    });
  }

  @Test
  public void regionEntriesGaugeShowsCountOfPartitionedRegionValuesInEachServer() {
    String regionName = "partitionedRegion";

    Region<String, String> region = createRegion(PARTITION.name(), regionName);

    List<String> keys = asList("a", "b", "c", "d", "e", "f", "g", "h");
    for (String key : keys) {
      region.put(key, key);
    }

    region.destroy(keys.get(0));

    int expectedNumberOfEntries = keys.size() - 1;

    String getGaugeValueCommand = memberRegionEntryGaugeValueCommand(regionName);

    await().untilAsserted(() -> {
      GfshExecution execution = gfshRule.execute(connectToLocatorCommand, getGaugeValueCommand);
      int sum = linesOf(execution.getOutputText())
          .filter(s -> s.startsWith("server"))
          .mapToInt(RegionEntriesGaugeTest::extractEntryCount)
          .sum();

      assertThat(sum)
          .as("total number of entries on all servers")
          .isEqualTo(expectedNumberOfEntries);
    });
  }

  @Test
  public void regionEntriesGaugeShowsCountOfPartitionedRedundantRegionValuesInEachServer()
      throws IOException {
    int numberOfRedundantCopies = 1;
    String regionName = "partitionedRegion";

    int server3Port = AvailablePortHelper.getRandomAvailableTCPPort();

    File folderForServer3 = temporaryFolder.newFolder("server3");
    serverFolders.add(folderForServer3);

    String startServer3Command = startServerCommand("server3", server3Port, folderForServer3);
    gfshRule.execute(connectToLocatorCommand, startServer3Command);

    Region<String, String> region =
        createPartitionedRegionWithRedundancy(regionName, numberOfRedundantCopies);

    List<String> keys = asList("a", "b", "c", "d", "e", "f", "g", "h");
    for (String key : keys) {
      region.put(key, key);
    }

    region.destroy(keys.get(0));

    String getGaugeValueCommand = memberRegionEntryGaugeValueCommand(regionName);

    int numberOfEntries = keys.size() - 1;
    int totalNumberOfCopies = numberOfRedundantCopies + 1;
    int expectedNumberOfEntries = numberOfEntries * totalNumberOfCopies;

    await()
        .atMost(10, TimeUnit.SECONDS)
        .untilAsserted(() -> {
          GfshExecution execution =
              gfshRule.execute(connectToLocatorCommand, getGaugeValueCommand);
          int totalEntryCount = linesOf(execution.getOutputText())
              .filter(s -> s.startsWith("server"))
              .mapToInt(RegionEntriesGaugeTest::extractEntryCount)
              .sum();

          assertThat(totalEntryCount)
              .as("total number of entries on all servers")
              .isEqualTo(expectedNumberOfEntries);
        });
  }

  private static int extractEntryCount(String serverEntryCountLine) {
    String entryCountExtractor = ".*\\[(\\d+).*].*";
    Pattern entryCountPattern = Pattern.compile(entryCountExtractor);
    Matcher matcher = entryCountPattern.matcher(serverEntryCountLine);
    assertThat(matcher.matches())
        .as(serverEntryCountLine)
        .withFailMessage("does not match " + entryCountExtractor)
        .isTrue();
    String matchedGroup = matcher.group(1);
    return Integer.valueOf(matchedGroup);
  }

  private static Stream<String> linesOf(String text) {
    return new BufferedReader(new StringReader(text)).lines();
  }

  private Region<String, String> createPartitionedRegionWithRedundancy(String regionName,
      int numberOfRedundantCopies) {

    String createRegionCommand = String.join(SPACE,
        "create region",
        "--name=" + regionName,
        "--redundant-copies=" + numberOfRedundantCopies,
        "--type=" + PARTITION_REDUNDANT.name());

    gfshRule.execute(connectToLocatorCommand, createRegionCommand);

    return clientCache.<String, String>createClientRegionFactory(PROXY).create(regionName);
  }

  private Region<String, String> createRegion(String regionType, String regionName) {

    String createRegionCommand = String.join(SPACE,
        "create region",
        "--name=" + regionName,
        "--type=" + regionType);

    gfshRule.execute(connectToLocatorCommand, createRegionCommand);

    return clientCache.<String, String>createClientRegionFactory(PROXY).create(regionName);
  }

  private String startServerCommand(String serverName, int serverPort, File folderForServer) {
    return String.join(SPACE,
        "start server",
        "--name=" + serverName,
        "--dir=" + folderForServer.getAbsolutePath(),
        "--server-port=" + serverPort,
        "--locators=" + locatorString,
        "--classpath=" + metricsPublishingServiceJarRule.absolutePath());
  }

  private static String memberRegionEntryGaugeValueCommand(String regionName) {
    return String.join(SPACE,
        "execute function",
        "--id=" + GetMemberRegionEntriesGaugeFunction.ID,
        "--arguments=" + regionName);
  }

  public static class GetMemberRegionEntriesGaugeFunction implements Function<String[]> {

    private static final String ID = "GetMemberRegionEntriesGaugeFunction";

    @Override
    public void execute(FunctionContext<String[]> context) {
      String regionName = context.getArguments()[0];

      Gauge memberRegionEntriesGauge = SimpleMetricsPublishingService.getRegistry()
          .find("member.region.entries")
          .tag("region.name", regionName)
          .gauge();

      Object result = memberRegionEntriesGauge == null
          ? "Meter not found."
          : memberRegionEntriesGauge.value();

      context.getResultSender().lastResult(result);
    }

    @Override
    public String getId() {
      return ID;
    }
  }
}
