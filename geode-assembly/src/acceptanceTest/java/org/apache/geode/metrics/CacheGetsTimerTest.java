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

import static java.io.File.pathSeparatorChar;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.geode.cache.execute.FunctionService.onServer;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import io.micrometer.core.instrument.Timer;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.security.AuthInitialize;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class CacheGetsTimerTest {
  private int locatorPort;
  private ClientCache clientCache;
  private Region<Object, Object> replicateRegion;
  private Region<Object, Object> partitionRegion;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  @After
  public void tearDown() {
    if (clientCache != null) {
      clientCache.close();
    }

    if (locatorPort != 0) {
      String connectToLocatorCommand = "connect --locator=localhost[" + locatorPort + "]";
      String shutdownCommand = "shutdown --include-locators";
      gfshRule.execute(connectToLocatorCommand, shutdownCommand);
    }
  }

  @Test
  public void hitTimerRecordsCountAndTotalTime_ifGetsPerformedOnReplicateRegionWithExistingKey()
      throws IOException {
    startWithTimeStatisticsEnabled();

    for (int i = 0; i < 5; i++) {
      replicateRegion.put(i, i);
      replicateRegion.get(i);
    }

    TimerValue hitTimerValue = hitTimerValueForRegion(replicateRegion);

    assertThat(hitTimerValue.region)
        .as("Cache gets hit timer region tag value")
        .isEqualTo(replicateRegion.getName());

    assertThat(hitTimerValue.result)
        .as("Cache gets hit timer result tag value")
        .isEqualTo("hit");

    assertThat(hitTimerValue.count)
        .as("Cache gets hit timer count for region " + replicateRegion.getName())
        .isEqualTo(5);

    assertThat(hitTimerValue.totalTime)
        .as("Cache gets hit timer total time for region " + replicateRegion.getName())
        .isGreaterThan(0);
  }

  @Test
  public void missTimerRecordsCountAndTotalTime_ifGetsPerformedOnReplicateRegionWithMissingKey()
      throws IOException {
    startWithTimeStatisticsEnabled();

    for (int i = 0; i < 5; i++) {
      replicateRegion.get(i);
    }

    TimerValue missTimerValue = missTimerValueForRegion(replicateRegion);

    assertThat(missTimerValue.region)
        .as("Cache gets miss timer region tag value")
        .isEqualTo(replicateRegion.getName());

    assertThat(missTimerValue.result)
        .as("Cache gets miss timer result tag value")
        .isEqualTo("miss");

    assertThat(missTimerValue.count)
        .as("Cache gets miss timer count for region " + replicateRegion.getName())
        .isEqualTo(5);

    assertThat(missTimerValue.totalTime)
        .as("Cache gets miss timer total time for region " + replicateRegion.getName())
        .isGreaterThan(0);
  }

  @Test
  public void hitTimerRecordsCountAndTotalTime_ifGetsPerformedOnPartitionRegionWithExistingKey()
      throws IOException {
    startWithTimeStatisticsEnabled();

    for (int i = 0; i < 5; i++) {
      partitionRegion.put(i, i);
      partitionRegion.get(i);
    }

    TimerValue hitTimerValue = hitTimerValueForRegion(partitionRegion);

    assertThat(hitTimerValue.region)
        .as("Cache gets hit timer region tag value")
        .isEqualTo(partitionRegion.getName());

    assertThat(hitTimerValue.result)
        .as("Cache gets hit timer result tag value")
        .isEqualTo("hit");

    assertThat(hitTimerValue.count)
        .as("Cache gets hit timer count for region " + partitionRegion.getName())
        .isEqualTo(5);

    assertThat(hitTimerValue.totalTime)
        .as("Cache gets hit timer total time for region " + partitionRegion.getName())
        .isGreaterThan(0);
  }

  @Test
  public void missTimerRecordsCountAndTotalTime_ifGetsPerformedOnPartitionRegionWithMissingKey()
      throws IOException {
    startWithTimeStatisticsEnabled();

    for (int i = 0; i < 5; i++) {
      partitionRegion.get(i);
    }

    TimerValue missTimerValue = missTimerValueForRegion(partitionRegion);

    assertThat(missTimerValue.region)
        .as("Cache gets miss timer region tag value")
        .isEqualTo(partitionRegion.getName());

    assertThat(missTimerValue.result)
        .as("Cache gets miss timer result tag value")
        .isEqualTo("miss");

    assertThat(missTimerValue.count)
        .as("Cache gets miss timer count for region " + partitionRegion.getName())
        .isEqualTo(5);

    assertThat(missTimerValue.totalTime)
        .as("Cache gets miss timer total time for region " + partitionRegion.getName())
        .isGreaterThan(0);
  }

  @Test
  public void timersExistWithInitialValues_ifNoGetsPerformedOnReplicateRegion() throws IOException {
    startWithTimeStatisticsEnabled();

    assertThat(allTimerValuesForRegion(replicateRegion))
        .as("All timer values for region " + replicateRegion.getName())
        .hasSize(2)
        .anyMatch(tv -> tv.result.equals("hit"))
        .anyMatch(tv -> tv.result.equals("miss"))
        .allMatch(tv -> tv.count == 0, "All timers have count of zero")
        .allMatch(tv -> tv.totalTime == 0, "All timers have total time of zero");
  }

  @Test
  public void allTimersRemoved_ifReplicateRegionDestroyed() throws IOException {
    startWithTimeStatisticsEnabled();

    assertThat(allTimerValuesForRegion(replicateRegion))
        .as("All timer values before destroying region " + replicateRegion.getName())
        .hasSize(2);

    replicateRegion.destroyRegion();

    assertThat(allTimerValuesForRegion(replicateRegion))
        .as("All timer values after destroying region " + replicateRegion.getName())
        .isEmpty();
  }

  @Test
  public void timersExistWithInitialValues_ifNoGetsPerformedOnPartitionRegion() throws IOException {
    startWithTimeStatisticsEnabled();

    assertThat(allTimerValuesForRegion(partitionRegion))
        .as("All timer values for region " + partitionRegion.getName())
        .hasSize(2)
        .anyMatch(tv -> tv.result.equals("hit"))
        .anyMatch(tv -> tv.result.equals("miss"))
        .allMatch(tv -> tv.count == 0, "All timers have count of zero")
        .allMatch(tv -> tv.totalTime == 0, "All timers have total time of zero");
  }

  @Test
  public void allTimersRemoved_ifPartitionRegionDestroyed() throws IOException {
    startWithTimeStatisticsEnabled();

    assertThat(allTimerValuesForRegion(partitionRegion))
        .as("All timer values before destroying region " + partitionRegion.getName())
        .hasSize(2);

    partitionRegion.destroyRegion();

    assertThat(allTimerValuesForRegion(partitionRegion))
        .as("All timer values after destroying region " + partitionRegion.getName())
        .isEmpty();
  }

  @Test
  public void timersRecordCountForReplicateRegion_ifTimeStatisticsDisabled() throws IOException {
    startWithTimeStatisticsDisabled();

    replicateRegion.put("existing-key", "existing-value");
    replicateRegion.get("existing-key");
    replicateRegion.get("missing-key");

    assertThat(allTimerValuesForRegion(replicateRegion))
        .as("All timer values for region " + replicateRegion.getName())
        .hasSize(2)
        .anyMatch(tv -> tv.result.equals("hit"))
        .anyMatch(tv -> tv.result.equals("miss"))
        .allMatch(tv -> tv.count == 1, "All timers have count of one")
        .allMatch(tv -> tv.totalTime == 0, "All timers have total time of zero");
  }

  @Test
  public void timersRecordCountForPartitionRegion_ifTimeStatisticsDisabled() throws IOException {
    startWithTimeStatisticsDisabled();

    partitionRegion.put("existing-key", "existing-value");
    partitionRegion.get("existing-key");
    partitionRegion.get("missing-key");

    assertThat(allTimerValuesForRegion(partitionRegion))
        .as("All timer values for region " + partitionRegion.getName())
        .hasSize(2)
        .anyMatch(tv -> tv.result.equals("hit"))
        .anyMatch(tv -> tv.result.equals("miss"))
        .allMatch(tv -> tv.count == 1, "All timers have count of one")
        .allMatch(tv -> tv.totalTime == 0, "All timers have total time of zero");
  }

  @Test
  public void timersDoNotRecord_ifNotAuthorized() throws IOException {
    startWithSecurityEnabled();

    Throwable thrown =
        catchThrowable(() -> replicateRegion.get(DenyUnauthorizedKey.UNAUTHORIZED_KEY));

    assertThat(thrown)
        .as("Exception from unauthorized GET operation")
        .isNotNull();

    assertThat(allTimerValuesForRegion(replicateRegion))
        .as("All timer values for region " + replicateRegion.getName())
        .hasSize(2)
        .allMatch(tv -> tv.count == 0, "All timers have count of zero");
  }

  private void startWithTimeStatisticsEnabled() throws IOException {
    startCluster(true, false);
  }

  private void startWithTimeStatisticsDisabled() throws IOException {
    startCluster(false, false);
  }

  private void startWithSecurityEnabled() throws IOException {
    startCluster(true, true);
  }

  private void startCluster(boolean enableTimeStatistics, boolean enableSecurity)
      throws IOException {
    int[] availablePorts = getRandomAvailableTCPPorts(2);

    locatorPort = availablePorts[0];
    int serverPort = availablePorts[1];

    Path serviceJarPath = serviceJarRule.createJarFor("metrics-publishing-service.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);

    Path helpersJarPath = temporaryFolder.getRoot().toPath()
        .resolve("helpers.jar").toAbsolutePath();
    writeJarFromClasses(helpersJarPath.toFile(), TimerValue.class,
        FetchCacheGetsTimerValues.class, DenyUnauthorizedKey.class, ClientCacheAuthInit.class);

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + "locator",
        "--dir=" + temporaryFolder.newFolder("locator").getAbsolutePath(),
        "--port=" + locatorPort,
        "--classpath=" + helpersJarPath);

    String serverName = "server";
    String startServerCommand = String.join(" ",
        "start server",
        "--name=" + serverName,
        "--dir=" + temporaryFolder.newFolder(serverName).getAbsolutePath(),
        "--server-port=" + serverPort,
        "--locators=localhost[" + locatorPort + "]",
        "--classpath=" + serviceJarPath + pathSeparatorChar + helpersJarPath);

    if (enableTimeStatistics) {
      startServerCommand += " --enable-time-statistics";
    }

    if (enableSecurity) {
      File securityPropertiesFile = createSecurityPropertiesFile();
      String securityPropertiesFileOption =
          " --security-properties-file=" + securityPropertiesFile.getAbsolutePath();
      startLocatorCommand += securityPropertiesFileOption;
      startServerCommand += securityPropertiesFileOption;
    }

    String replicateRegionName = "ReplicateRegion";
    String createReplicateRegionCommand = String.join(" ",
        "create region",
        "--type=REPLICATE",
        "--name=" + replicateRegionName);

    String partitionRegionName = "PartitionRegion";
    String createPartitionRegionCommand = String.join(" ",
        "create region",
        "--type=PARTITION",
        "--name=" + partitionRegionName);

    gfshRule.execute(startLocatorCommand, startServerCommand, createReplicateRegionCommand,
        createPartitionRegionCommand);

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
    if (enableSecurity) {
      clientCacheFactory.set("security-client-auth-init", ClientCacheAuthInit.class.getName());
    }

    clientCache = clientCacheFactory
        .addPoolLocator("localhost", locatorPort)
        .create();

    replicateRegion = clientCache
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(replicateRegionName);

    partitionRegion = clientCache
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(partitionRegionName);
  }

  private File createSecurityPropertiesFile() throws IOException {
    Properties securityProperties = new Properties();
    securityProperties.setProperty(SECURITY_MANAGER, DenyUnauthorizedKey.class.getName());
    securityProperties.setProperty("security-username", DenyUnauthorizedKey.AUTHENTICATED_USER);
    securityProperties.setProperty("security-password", DenyUnauthorizedKey.AUTHENTICATED_PASSWORD);

    File securityPropertiesFile = gfshRule.getTemporaryFolder().newFile("security.properties");
    securityProperties.store(new FileOutputStream(securityPropertiesFile), null);
    return securityPropertiesFile;
  }

  private TimerValue hitTimerValueForRegion(Region<?, ?> region) {
    return timerValueForRegionAndResult(region, "hit");
  }

  private TimerValue missTimerValueForRegion(Region<?, ?> region) {
    return timerValueForRegionAndResult(region, "miss");
  }

  private TimerValue timerValueForRegionAndResult(Region<?, ?> region, String resultTagValue) {
    List<TimerValue> cacheGetsTimerValues = allTimerValuesForRegion(region).stream()
        .filter(tv -> tv.result.equals(resultTagValue))
        .collect(toList());

    assertThat(cacheGetsTimerValues)
        .as("Timers for region " + region.getName() + " with result " + resultTagValue)
        .hasSize(1);

    return cacheGetsTimerValues.get(0);
  }

  private List<TimerValue> allTimerValuesForRegion(Region<?, ?> region) {
    @SuppressWarnings("unchecked")
    List<List<TimerValue>> timerValuesFromAllServers =
        (List<List<TimerValue>>) onServer(clientCache)
            .execute(new FetchCacheGetsTimerValues())
            .getResult();

    assertThat(timerValuesFromAllServers)
        .hasSize(1);

    return timerValuesFromAllServers.get(0).stream()
        .filter(tv -> tv.region.equals(region.getName()))
        .collect(toList());
  }

  static class TimerValue implements Serializable {
    final long count;
    final double totalTime;
    final String region;
    final String result;

    TimerValue(long count, double totalTime, String region, String result) {
      this.count = count;
      this.totalTime = totalTime;
      this.region = region;
      this.result = result;
    }

    @Override
    public String toString() {
      return "TimerValue{" +
          "count=" + count +
          ", totalTime=" + totalTime +
          ", region='" + region + '\'' +
          ", result='" + result + '\'' +
          '}';
    }
  }

  static class FetchCacheGetsTimerValues implements Function<Void> {
    private static final String ID = "FetchCacheGetsTimerValues";

    @Override
    public void execute(FunctionContext<Void> context) {
      Collection<Timer> timers = SimpleMetricsPublishingService.getRegistry()
          .find("geode.cache.gets")
          .timers();

      List<TimerValue> result = timers.stream()
          .map(FetchCacheGetsTimerValues::toTimerValues)
          .collect(toList());

      context.getResultSender().lastResult(result);
    }

    @Override
    public String getId() {
      return ID;
    }

    private static TimerValue toTimerValues(Timer t) {
      String region = t.getId().getTag("region");
      String result = t.getId().getTag("result");

      return new TimerValue(
          t.count(),
          t.totalTime(NANOSECONDS),
          region,
          result);
    }
  }

  public static class DenyUnauthorizedKey implements org.apache.geode.security.SecurityManager {
    static final String AUTHENTICATED_USER = "user";
    static final String AUTHENTICATED_PASSWORD = "password";
    static final String UNAUTHORIZED_KEY = "unauthorized-key";

    @Override
    public Object authenticate(Properties credentials) throws AuthenticationFailedException {
      return AUTHENTICATED_USER;
    }

    @Override
    public boolean authorize(Object principal, ResourcePermission permission) {
      return !UNAUTHORIZED_KEY.equals(permission.getKey());
    }
  }

  public static class ClientCacheAuthInit implements AuthInitialize {
    @Override
    public Properties getCredentials(Properties securityProps, DistributedMember server,
        boolean isPeer) throws AuthenticationFailedException {
      Properties properties = new Properties();
      properties.setProperty("security-username", DenyUnauthorizedKey.AUTHENTICATED_USER);
      properties.setProperty("security-password", DenyUnauthorizedKey.AUTHENTICATED_PASSWORD);
      return properties;
    }
  }
}
