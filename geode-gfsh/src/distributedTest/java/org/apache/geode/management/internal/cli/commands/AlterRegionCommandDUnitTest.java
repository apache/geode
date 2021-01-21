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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.lang.Identifiable.find;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({RegionsTest.class})
public class AlterRegionCommandDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static MemberVM locator, server1, server2, server3;

  @BeforeClass
  public static void beforeClass() throws Exception {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startServerVM(1, "group1", locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());
    server3 = cluster.startServerVM(3, locator.getPort());

    gfsh.connectAndVerify(locator);

    deployJarFilesForRegionAlter();
  }

  @Before
  public void before() throws Exception {
    // make sure all tests started with no region defined
    gfsh.executeAndAssertThat("list regions").statusIsSuccess().containsOutput("No Regions Found");
  }

  @Test
  public void alterRegionResetCacheListeners() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=PARTITION")
        .statusIsSuccess();

    String listenerABC =
        "com.cadrdunit.RegionAlterCacheListenerA,com.cadrdunit.RegionAlterCacheListenerB,com.cadrdunit.RegionAlterCacheListenerC";
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --cache-listener=" + listenerABC)
        .statusIsSuccess().tableHasRowCount(3);

    VMProvider.invokeInEveryMember(() -> {
      RegionAttributes<Object, Object> attributes =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName)
              .getAttributes();
      assertEquals(3, attributes.getCacheListeners().length);

      assertThat(Arrays.stream(attributes.getCacheListeners()).map(c -> c.getClass().getName())
          .collect(Collectors.toSet())).containsExactlyInAnyOrder(
              "com.cadrdunit.RegionAlterCacheListenerA", "com.cadrdunit.RegionAlterCacheListenerB",
              "com.cadrdunit.RegionAlterCacheListenerC");
    }, server1, server2, server3);

    // alter region on a group instead of "cluster"
    gfsh.executeAndAssertThat(
        "alter region --group=group1 --name=" + regionName + " --cache-listener=''")
        .statusIsError()
        .hasInfoSection()
        .hasOutput().contains(SEPARATOR + regionName + " does not exist in group group1");

    // since this region exists on "cluster" group, we can only alter it with a "cluster" group
    gfsh.executeAndAssertThat("alter region --name=" + regionName + " --cache-listener=''")
        .statusIsSuccess()
        .hasTableSection().hasRowSize(3)
        .hasRow(0)
        .containsExactly("server-1", "OK", "Region " + regionName + " altered");
    // remove listener on server1
    server1.invoke(() -> {
      RegionAttributes<Object, Object> attributes =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName)
              .getAttributes();
      assertEquals(0, attributes.getCacheListeners().length);
    });
    gfsh.executeAndAssertThat("destroy region --name=" + regionName + " --if-exists")
        .statusIsSuccess();
  }

  @Test
  public void alterEntryIdleTimeExpiration() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName
            + " --type=REPLICATE --entry-idle-time-expiration=10 --enable-statistics")
        .statusIsSuccess();

    server1.invoke(() -> {
      Region<Object, Object> region =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName);
      ExpirationAttributes expiry = region.getAttributes().getEntryIdleTimeout();
      assertThat(expiry.getTimeout()).isEqualTo(10);
      assertThat(expiry.getAction()).isEqualTo(ExpirationAction.INVALIDATE);
    });

    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --entry-idle-time-expiration-action=DESTROY")
        .statusIsSuccess();
    server1.invoke(() -> {
      Region<Object, Object> region =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName);
      ExpirationAttributes expiry = region.getAttributes().getEntryIdleTimeout();
      assertThat(expiry.getTimeout()).isEqualTo(10);
      assertThat(expiry.getAction()).isEqualTo(ExpirationAction.DESTROY);
    });

    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --entry-idle-time-expiration=5")
        .statusIsSuccess();
    server1.invoke(() -> {
      Region<Object, Object> region =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName);
      ExpirationAttributes expiry = region.getAttributes().getEntryIdleTimeout();
      assertThat(expiry.getTimeout()).isEqualTo(5);
      assertThat(expiry.getAction()).isEqualTo(ExpirationAction.DESTROY);
    });
    gfsh.executeAndAssertThat("destroy region --name=" + regionName + " --if-exists")
        .statusIsSuccess();
  }


  @Test
  public void alterEntryIdleTimeExpirationAction() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName
            + " --type=REPLICATE --entry-idle-time-expiration-action=destroy --enable-statistics")
        .statusIsSuccess();

    server1.invoke(() -> {
      Region<Object, Object> region =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName);
      ExpirationAttributes expiry = region.getAttributes().getEntryIdleTimeout();
      assertThat(expiry.getTimeout()).isEqualTo(0);
      assertThat(expiry.getAction()).isEqualTo(ExpirationAction.DESTROY);
    });

    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --entry-idle-time-expiration-action=invalidate")
        .statusIsSuccess();
    server1.invoke(() -> {
      Region<Object, Object> region =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName);
      ExpirationAttributes expiry = region.getAttributes().getEntryIdleTimeout();
      assertThat(expiry.getTimeout()).isEqualTo(0);
      assertThat(expiry.getAction()).isEqualTo(ExpirationAction.INVALIDATE);
    });

    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --entry-idle-time-expiration=5")
        .statusIsSuccess();
    server1.invoke(() -> {
      Region<Object, Object> region =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName);
      ExpirationAttributes expiry = region.getAttributes().getEntryIdleTimeout();
      assertThat(expiry.getTimeout()).isEqualTo(5);
      assertThat(expiry.getAction()).isEqualTo(ExpirationAction.INVALIDATE);
    });
    gfsh.executeAndAssertThat("destroy region --name=" + regionName + " --if-exists")
        .statusIsSuccess();
  }

  @Test
  public void alterRegionStatisticsNotEnabled() {
    String regionName = testName.getMethodName();
    IgnoredException.addIgnoredException(
        "java.lang.IllegalStateException: Cannot set idle timeout when statistics are disabled");
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --entry-idle-time-expiration-action=invalidate")
        .statusIsError()
        .containsOutput("Cannot set idle timeout when statistics are disabled.");

    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName
            + " --entry-idle-time-custom-expiry=com.cadrdunit.RegionAlterCustomExpiry")
        .statusIsError()
        .containsOutput("Cannot set idle timeout when statistics are disabled.");
    gfsh.executeAndAssertThat("destroy region --name=" + regionName + " --if-exists")
        .statusIsSuccess();
  }

  @Test
  public void alterExpirationAttributesWithStatisticsEnabled() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --enable-statistics")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --entry-idle-time-expiration-action=invalidate")
        .statusIsSuccess();

    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName
            + " --entry-idle-time-custom-expiry=com.cadrdunit.RegionAlterCustomExpiry")
        .statusIsSuccess();

    server1.invoke(() -> {
      Region<Object, Object> region =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName);
      ExpirationAttributes expiry = region.getAttributes().getEntryIdleTimeout();
      assertThat(expiry.getTimeout()).isEqualTo(0);
      assertThat(expiry.getAction()).isEqualTo(ExpirationAction.INVALIDATE);
      assertThat(region.getAttributes().getCustomEntryIdleTimeout().getClass().getName())
          .isEqualTo("com.cadrdunit.RegionAlterCustomExpiry");
    });

    gfsh.executeAndAssertThat(
        "alter region --name=" + regionName + " --entry-idle-time-custom-expiry=''")
        .statusIsSuccess();

    server1.invoke(() -> {
      Region<Object, Object> region =
          Objects.requireNonNull(ClusterStartupRule.getCache()).getRegion(regionName);
      ExpirationAttributes expiry = region.getAttributes().getEntryIdleTimeout();
      assertThat(expiry.getTimeout()).isEqualTo(0);
      assertThat(expiry.getAction()).isEqualTo(ExpirationAction.INVALIDATE);
      assertThat(region.getAttributes().getCustomEntryIdleTimeout()).isNull();
    });
    gfsh.executeAndAssertThat("destroy region --name=" + regionName + " --if-exists")
        .statusIsSuccess();
  }

  @Test
  public void alterEvictionMaxOnRegionWithoutEvictionAttributesHasNoEffect() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 3);

    gfsh.executeAndAssertThat("alter region --name=" + regionName + " --eviction-max=20")
        .statusIsSuccess();

    locator.invoke(() -> {
      CacheConfig config = Objects.requireNonNull(ClusterStartupRule.getLocator())
          .getConfigurationPersistenceService()
          .getCacheConfig("cluster");
      RegionConfig regionConfig = find(config.getRegions(), regionName);
      RegionAttributesType.EvictionAttributes evictionAttributes =
          regionConfig.getRegionAttributes().getEvictionAttributes();
      assertThat(evictionAttributes).isNull();
    });

    server1.invoke(() -> {
      Region<Object, Object> region = Objects.requireNonNull(ClusterStartupRule.getCache())
          .getRegion(SEPARATOR + regionName);
      EvictionAttributes evictionAttributes = region.getAttributes().getEvictionAttributes();
      assertThat(evictionAttributes.getAlgorithm()).isEqualTo(EvictionAlgorithm.NONE);
    });

    gfsh.executeAndAssertThat("destroy region --name=" + regionName + " --if-exists")
        .statusIsSuccess();
  }

  @Test
  public void alterRegionWithEvictionMaxOnRegionWithEviction() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName
            + " --type=REPLICATE --eviction-entry-count=20 --eviction-action=local-destroy")
        .statusIsSuccess();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + regionName, 3);

    gfsh.executeAndAssertThat("alter region --name=" + regionName + " --eviction-max=30")
        .statusIsSuccess();

    locator.invoke(() -> {
      CacheConfig config = Objects.requireNonNull(ClusterStartupRule.getLocator())
          .getConfigurationPersistenceService()
          .getCacheConfig("cluster");
      RegionConfig regionConfig = find(config.getRegions(), regionName);
      RegionAttributesType.EvictionAttributes evictionAttributes =
          regionConfig.getRegionAttributes().getEvictionAttributes();
      assertThat(evictionAttributes.getLruEntryCount().getMaximum()).isEqualTo("30");
      assertThat(evictionAttributes.getLruEntryCount().getAction()).isEqualTo(
          EnumActionDestroyOverflow.LOCAL_DESTROY);
    });

    gfsh.executeAndAssertThat("destroy region --name=" + regionName + " --if-exists")
        .statusIsSuccess();
  }

  private static void deployJarFilesForRegionAlter() throws IOException {
    ClassBuilder classBuilder = new ClassBuilder();
    final File jarFile1 = new File(temporaryFolder.getRoot(), "testAlterRegion1.jar");
    final File jarFile2 = new File(temporaryFolder.getRoot(), "testAlterRegion2.jar");
    final File jarFile3 = new File(temporaryFolder.getRoot(), "testAlterRegion3.jar");
    final File jarFile4 = new File(temporaryFolder.getRoot(), "testAlterRegion4.jar");
    final File jarFile5 = new File(temporaryFolder.getRoot(), "testAlterRegion5.jar");
    final File jarFile6 = new File(temporaryFolder.getRoot(), "testAlterRegion6.jar");


    byte[] jarBytes =
        classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheListenerA",
            "package com.cadrdunit;" + "import org.apache.geode.cache.util.CacheListenerAdapter;"
                + "public class RegionAlterCacheListenerA extends CacheListenerAdapter {}");
    writeJarBytesToFile(jarFile1, jarBytes);
    gfsh.executeAndAssertThat("deploy --jar=" + jarFile1.getAbsolutePath()).statusIsSuccess();

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheListenerB",
        "package com.cadrdunit;" + "import org.apache.geode.cache.util.CacheListenerAdapter;"
            + "public class RegionAlterCacheListenerB extends CacheListenerAdapter {}");
    writeJarBytesToFile(jarFile2, jarBytes);
    gfsh.executeAndAssertThat("deploy --jar=" + jarFile2.getAbsolutePath()).statusIsSuccess();

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheListenerC",
        "package com.cadrdunit;" + "import org.apache.geode.cache.util.CacheListenerAdapter;"
            + "public class RegionAlterCacheListenerC extends CacheListenerAdapter {}");
    writeJarBytesToFile(jarFile3, jarBytes);
    gfsh.executeAndAssertThat("deploy --jar=" + jarFile3.getAbsolutePath()).statusIsSuccess();

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheLoader",
        "package com.cadrdunit;" + "import org.apache.geode.cache.CacheLoader;"
            + "import org.apache.geode.cache.CacheLoaderException;"
            + "import org.apache.geode.cache.LoaderHelper;"
            + "public class RegionAlterCacheLoader implements CacheLoader {"
            + "public void close() {}"
            + "public Object load(LoaderHelper helper) throws CacheLoaderException {return null;}}");
    writeJarBytesToFile(jarFile4, jarBytes);
    gfsh.executeAndAssertThat("deploy --jar=" + jarFile4.getAbsolutePath()).statusIsSuccess();

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheWriter",
        "package com.cadrdunit;" + "import org.apache.geode.cache.util.CacheWriterAdapter;"
            + "public class RegionAlterCacheWriter extends CacheWriterAdapter {}");
    writeJarBytesToFile(jarFile5, jarBytes);
    gfsh.executeAndAssertThat("deploy --jar=" + jarFile5.getAbsolutePath()).statusIsSuccess();

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCustomExpiry",
        "package com.cadrdunit;" + "import org.apache.geode.cache.CustomExpiry;"
            + "import org.apache.geode.cache.Region.Entry;"
            + "import org.apache.geode.cache.ExpirationAttributes;"
            + "public class RegionAlterCustomExpiry implements CustomExpiry {"
            + "public void close() {}"
            + "public ExpirationAttributes getExpiry(Entry entry) {return null;}" + "}");
    writeJarBytesToFile(jarFile6, jarBytes);
    gfsh.executeAndAssertThat("deploy --jar=" + jarFile6.getAbsolutePath()).statusIsSuccess();
  }

  private static void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.flush();
    outStream.close();
  }
}
