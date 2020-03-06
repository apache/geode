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

import static org.apache.geode.lang.Identifiable.find;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({RegionsTest.class})
public class CreateRegionCommandDUnitTest {

  private static MemberVM locator, server1, server2;

  public static class TestCacheListener<K, V> extends CacheListenerAdapter<K, V>
      implements Serializable {
  }

  public static class AnotherTestCacheListener<K, V> extends CacheListenerAdapter<K, V>
      implements Serializable {
  }

  public static class DummyAEQListener implements AsyncEventListener, Declarable {
    @Override
    @SuppressWarnings("rawtypes")
    public boolean processEvents(List<AsyncEvent> events) {
      return false;
    }
  }

  public static class DummyPartitionResolver<K, V> implements PartitionResolver<K, V>, Declarable {
    @Override
    public Object getRoutingObject(EntryOperation<K, V> opDetails) {
      return null;
    }

    @Override
    public String getName() {
      return "dummy";
    }
  }

  public static class DummyCompressor implements Compressor, Declarable {
    @Override
    public byte[] compress(byte[] input) {
      return new byte[0];
    }

    @Override
    public byte[] decompress(byte[] input) {
      return new byte[0];
    }
  }

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @BeforeClass
  public static void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    server2 = lsRule.startServerVM(2, "group2", locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void multipleTemplateRegionTypes() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --group=group1")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE_PROXY --group=group2")
        .statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=failed --template-region=" + regionName)
        .statusIsError()
        .hasInfoSection().hasOutput()
        .contains("Multiple types of template region /multipleTemplateRegionTypes exist.");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testCreateRegionWithGoodCompressor() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName
        + " --type=REPLICATE --compressor=" + RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER)
        .statusIsSuccess();

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<?, ?> region = cache.getRegion(regionName);
      assertThat(region).isNotNull();
      assertThat(region.getAttributes().getCompressor())
          .isEqualTo(SnappyCompressor.getDefaultInstance());
    });

    gfsh.executeAndAssertThat("put --key='foo' --value='125' --region=" + regionName)
        .statusIsSuccess();
    gfsh.executeAndAssertThat("get --key='foo' --region=" + regionName)
        .statusIsSuccess()
        .containsKeyValuePair("Value", "\"125\"");
  }

  @Test
  public void testCreateRegionWithBadCompressor() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --type=REPLICATE --compressor=BAD_COMPRESSOR")
        .statusIsError();

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<?, ?> region = cache.getRegion(regionName);
      assertThat(region).isNull();
    });
  }

  @Test
  public void testCreateRegionWithNoCompressor() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<?, ?> region = cache.getRegion(regionName);
      assertThat(region).isNotNull();
      assertThat(region.getAttributes().getCompressor()).isNull();
    });
  }

  @Test
  public void testCreateRegionWithSubregion() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();
    String subRegionName = "subregion";
    String subRegionPath = regionName + "/" + subRegionName;

    gfsh.executeAndAssertThat("create region --name=" + subRegionPath +
        " --type=REPLICATE")
        .statusIsSuccess();

    String subSubRegionName = "subregion2";
    String subSubRegionPath = regionName + "/" + subRegionName + "/" + subSubRegionName;
    gfsh.executeAndAssertThat("create region --name=" + subSubRegionPath +
        " --type=REPLICATE")
        .statusIsSuccess();

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      Region<?, ?> region = cache.getRegion(regionName);
      assertThat(region).isNotNull();

      Region<?, ?> subRegion = cache.getRegion(subRegionPath);
      assertThat(subRegion).isNotNull();

      Region<?, ?> subSubRegion = cache.getRegion(subSubRegionPath);
      assertThat(subSubRegion).isNotNull();
    });

    locator.invoke(() -> {
      CacheConfig config = ClusterStartupRule.getLocator().getConfigurationPersistenceService()
          .getCacheConfig("cluster");
      assertThat(find(config.getRegions(), regionName)).isNotNull();

      RegionConfig parentConfig = find(config.getRegions(), regionName);
      assertThat(find(parentConfig.getRegions(), subRegionName)).isNotNull();

      RegionConfig subRegionConfig = find(parentConfig.getRegions(),
          subRegionName);
      assertThat(find(subRegionConfig.getRegions(), subSubRegionName))
          .isNotNull();
    });
  }

  @Test
  public void testCreateRegionWithPartitionResolver() throws Exception {
    String regionName = testName.getMethodName();
    String PR_STRING = "package io.pivotal; "
        + "public class TestPartitionResolver implements org.apache.geode.cache.PartitionResolver { "
        + "   @Override" + "   public void close() {" + "   }" + "   @Override"
        + "   public Object getRoutingObject(org.apache.geode.cache.EntryOperation opDetails) { "
        + "    return null; " + "   }" + "   @Override" + "   public String getName() { "
        + "    return \"TestPartitionResolver\";" + "   }" + " }";
    final File prJarFile = new File(tmpDir.getRoot(), "myPartitionResolver.jar");
    new JarBuilder().buildJar(prJarFile, PR_STRING);

    gfsh.executeAndAssertThat("deploy --jar=" + prJarFile.getAbsolutePath()).statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=" + regionName
        + " --type=PARTITION --partition-resolver=io.pivotal.TestPartitionResolver")
        .statusIsSuccess();

    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
      PartitionResolver<?, ?> resolver = region.getPartitionAttributes().getPartitionResolver();
      assertThat(resolver).isNotNull();
      assertThat(resolver.getName()).isEqualTo("TestPartitionResolver");
    });
  }

  @Test
  public void testCreateRegionWithInvalidPartitionResolver() {
    gfsh.executeAndAssertThat("create region --name=" + testName.getMethodName()
        + " --type=PARTITION --partition-resolver=InvalidPartitionResolver")
        .statusIsError()
        .containsOutput("Error instantiating class");
  }

  @Test
  public void testCreateRegionWithInvalidCustomExpiry() {
    gfsh.executeAndAssertThat("create region --name=" + testName.getMethodName()
        + " --type=REPLICATE --entry-time-to-live-custom-expiry=InvalidCustomExpiry" +
        " --enable-statistics=true")
        .statusIsError()
        .containsOutput("Error instantiating class");

    gfsh.executeAndAssertThat("create region --name=" + testName.getMethodName()
        + " --type=REPLICATE --entry-idle-time-custom-expiry=InvalidCustomExpiry" +
        " --enable-statistics=true")
        .statusIsError()
        .containsOutput("Error instantiating class");
  }

  @Test
  public void testCreateRegionWithInvalidCacheLoader() {
    gfsh.executeAndAssertThat("create region --name=" + testName.getMethodName()
        + " --type=REPLICATE --cache-loader=InvalidCacheLoader")
        .statusIsError()
        .containsOutput("Error instantiating class");
  }

  @Test
  public void testCreateRegionWithInvalidCacheWriter() {
    gfsh.executeAndAssertThat("create region --name=" + testName.getMethodName()
        + " --type=REPLICATE --cache-writer=InvalidCacheWriter")
        .statusIsError()
        .containsOutput("Error instantiating class");
  }

  @Test
  public void testCreateRegionWithInvalidCacheListeners() {
    gfsh.executeAndAssertThat("create region --name=" + testName.getMethodName()
        + " --type=REPLICATE --cache-listener=" + TestCacheListener.class.getName()
        + ",InvalidCacheListener")
        .statusIsError()
        .containsOutput("Error instantiating class")
        .doesNotContainOutput("TestCacheListener");
  }

  @Test
  public void testCreateRegionForReplicatedRegionWithPartitionResolver() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName
        + " --type=REPLICATE --partition-resolver=InvalidPartitionResolver")
        .containsOutput("can be used only for creating a Partitioned Region").statusIsError();
  }

  @Test
  public void testCreateRegionFromTemplateFailsIfTemplateDoesNotExist() {
    gfsh.executeAndAssertThat("create region --template-region=/TEMPLATE --name=/TEST"
        + TestCacheListener.class.getName())
        .statusIsError()
        .containsOutput("Template region /TEMPLATE does not exist");
  }

  @Test
  public void overrideListenerFromTemplate() {
    gfsh.executeAndAssertThat("create region --name=/TEMPLATE --type=PARTITION_REDUNDANT"
        + " --cache-listener=" + TestCacheListener.class.getName()).statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=/COPY --template-region=/TEMPLATE"
        + " --cache-listener=" + AnotherTestCacheListener.class.getName()).statusIsSuccess();

    server1.getVM().invoke(() -> {
      Region<?, ?> copy = ClusterStartupRule.getCache().getRegion("/COPY");

      assertThat(Arrays.stream(copy.getAttributes().getCacheListeners())
          .map(c -> c.getClass().getName()).collect(Collectors.toSet()))
              .containsExactly(AnotherTestCacheListener.class.getName());
    });

    gfsh.executeAndAssertThat("destroy region --name=/COPY").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy region --name=/TEMPLATE").statusIsSuccess();
  }


  @Test
  public void cannotSetRegionExpirationForPartitionedTemplate() {
    gfsh.executeAndAssertThat("create region --name=/TEMPLATE --type=PARTITION")
        .statusIsSuccess();

    gfsh.executeAndAssertThat(
        "create region --name=/COPY --template-region=/TEMPLATE --enable-statistics=true --region-idle-time-expiration=1 --region-time-to-live-expiration=1")
        .statusIsError()
        .containsOutput(
            "ExpirationAction INVALIDATE or LOCAL_INVALIDATE for region is not supported for Partitioned Region");

    gfsh.executeAndAssertThat("destroy region --name=/TEMPLATE").statusIsSuccess();
  }

  @Test
  public void cannotCreateTemplateWithInconsistentPersistence() {
    gfsh.executeAndAssertThat("create region --name=/TEMPLATE --type=PARTITION_PERSISTENT")
        .statusIsSuccess();

    gfsh.executeAndAssertThat(
        "create region --name=/COPY --template-region=/TEMPLATE --enable-statistics=true --region-idle-time-expiration=1 --region-time-to-live-expiration=1")
        .statusIsError()
        .containsOutput(
            "ExpirationAction INVALIDATE or LOCAL_INVALIDATE for region is not supported for Partitioned Region");

    gfsh.executeAndAssertThat("destroy region --name=/TEMPLATE").statusIsSuccess();
  }

  @Test
  public void ensureOverridingCallbacksFromTemplateDoNotRequireClassesOnLocator() throws Exception {
    final File prJarFile = new File(tmpDir.getRoot(), "myCacheListener.jar");
    new JarBuilder().buildJar(prJarFile, getUniversalClassCode("MyCallback"),
        getUniversalClassCode("MyNewCallback"));
    gfsh.executeAndAssertThat("deploy --jar=" + prJarFile.getAbsolutePath()).statusIsSuccess();

    String myCallback = "io.pivotal.MyCallback";
    gfsh.executeAndAssertThat(
        String
            .format(
                "create region --name=/TEMPLATE --type=PARTITION_REDUNDANT "
                    + "--cache-listener=%1$s " + "--cache-loader=%1$s " + "--cache-writer=%1$s "
                    + "--compressor=%1$s " + "--key-constraint=%1$s " + "--value-constraint=%1$s",
                myCallback))
        .statusIsSuccess();

    String myNewCallback = "io.pivotal.MyNewCallback";
    gfsh.executeAndAssertThat(String
        .format("create region --name=/COPY --template-region=/TEMPLATE " + "--cache-listener=%1$s "
            + "--cache-loader=%1$s " + "--cache-writer=%1$s " + "--compressor=%1$s "
            + "--key-constraint=%1$s " + "--value-constraint=%1$s", myNewCallback))
        .statusIsSuccess();

    server1.getVM().invoke(() -> {
      Region<?, ?> copy = ClusterStartupRule.getCache().getRegion("/COPY");

      assertThat(Arrays.stream(copy.getAttributes().getCacheListeners())
          .map(c -> c.getClass().getName()).collect(Collectors.toSet()))
              .containsExactly(myNewCallback);

      assertThat(copy.getAttributes().getCacheLoader().getClass().getName())
          .isEqualTo(myNewCallback);

      assertThat(copy.getAttributes().getCacheWriter().getClass().getName())
          .isEqualTo(myNewCallback);

      assertThat(copy.getAttributes().getCompressor().getClass().getName())
          .isEqualTo(myNewCallback);

      assertThat(copy.getAttributes().getKeyConstraint().getName()).isEqualTo(myNewCallback);

      assertThat(copy.getAttributes().getValueConstraint().getName()).isEqualTo(myNewCallback);
    });

    gfsh.executeAndAssertThat("destroy region --name=/COPY").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy region --name=/TEMPLATE").statusIsSuccess();
  }

  @Test
  public void ensureNewCallbacksFromTemplateDoNotRequireClassesOnLocator() throws Exception {
    final File prJarFile = new File(tmpDir.getRoot(), "myCacheListener.jar");
    new JarBuilder().buildJar(prJarFile, getUniversalClassCode("MyNewCallback"));
    gfsh.executeAndAssertThat("deploy --jar=" + prJarFile.getAbsolutePath()).statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=/TEMPLATE --type=PARTITION_REDUNDANT")
        .statusIsSuccess();

    String myNewCallback = "io.pivotal.MyNewCallback";
    gfsh.executeAndAssertThat(String
        .format("create region --name=/COPY --template-region=/TEMPLATE " + "--cache-listener=%1$s "
            + "--cache-loader=%1$s " + "--cache-writer=%1$s " + "--compressor=%1$s "
            + "--key-constraint=%1$s " + "--value-constraint=%1$s", myNewCallback))
        .statusIsSuccess();

    server1.getVM().invoke(() -> {
      Region<?, ?> copy = ClusterStartupRule.getCache().getRegion("/COPY");

      assertThat(Arrays.stream(copy.getAttributes().getCacheListeners())
          .map(c -> c.getClass().getName()).collect(Collectors.toSet()))
              .containsExactly(myNewCallback);

      assertThat(copy.getAttributes().getCacheLoader().getClass().getName())
          .isEqualTo(myNewCallback);

      assertThat(copy.getAttributes().getCacheWriter().getClass().getName())
          .isEqualTo(myNewCallback);

      assertThat(copy.getAttributes().getCompressor().getClass().getName())
          .isEqualTo(myNewCallback);

      assertThat(copy.getAttributes().getKeyConstraint().getName()).isEqualTo(myNewCallback);

      assertThat(copy.getAttributes().getValueConstraint().getName()).isEqualTo(myNewCallback);
    });

    gfsh.executeAndAssertThat("destroy region --name=/COPY").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy region --name=/TEMPLATE").statusIsSuccess();
  }

  @Test
  public void ensureOriginalCallbacksFromTemplateDoNotRequireClassesOnLocator() throws Exception {
    final File prJarFile = new File(tmpDir.getRoot(), "myCacheListener.jar");
    new JarBuilder().buildJar(prJarFile, getUniversalClassCode("MyCallback"));
    gfsh.executeAndAssertThat("deploy --jar=" + prJarFile.getAbsolutePath()).statusIsSuccess();

    String myCallback = "io.pivotal.MyCallback";
    gfsh.executeAndAssertThat(
        String
            .format(
                "create region --name=/TEMPLATE --type=PARTITION_REDUNDANT "
                    + "--cache-listener=%1$s " + "--cache-loader=%1$s " + "--cache-writer=%1$s "
                    + "--compressor=%1$s " + "--key-constraint=%1$s " + "--value-constraint=%1$s",
                myCallback))
        .statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=/COPY --template-region=/TEMPLATE")
        .statusIsSuccess();

    server1.getVM().invoke(() -> {
      Region<?, ?> copy = ClusterStartupRule.getCache().getRegion("/COPY");

      assertThat(Arrays.stream(copy.getAttributes().getCacheListeners())
          .map(c -> c.getClass().getName()).collect(Collectors.toSet()))
              .containsExactly(myCallback);

      assertThat(copy.getAttributes().getCacheLoader().getClass().getName()).isEqualTo(myCallback);

      assertThat(copy.getAttributes().getCacheWriter().getClass().getName()).isEqualTo(myCallback);

      assertThat(copy.getAttributes().getCompressor().getClass().getName()).isEqualTo(myCallback);

      assertThat(copy.getAttributes().getKeyConstraint().getName()).isEqualTo(myCallback);

      assertThat(copy.getAttributes().getValueConstraint().getName()).isEqualTo(myCallback);
    });

    gfsh.executeAndAssertThat("destroy region --name=/COPY").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy region --name=/TEMPLATE").statusIsSuccess();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void startWithNonProxyRegion() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --type=REPLICATE --group=group1 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --type=REPLICATE --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("Region /" + regionName + " already exists on the cluster");

    gfsh.executeAndAssertThat("create region --type=PARTITION --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("Region /" + regionName + " already exists on the cluster");

    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE_PROXY --group=group2 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-2");

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + regionName, 2);

    gfsh.executeAndAssertThat(
        "create region --type=PARTITION_PROXY --group=group2 --name=" + regionName).statusIsError()
        .containsOutput("The existing region is not a partitioned region");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void startWithReplicateProxyRegion() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE_PROXY --group=group1 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --type=REPLICATE --group=group2 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-2");

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + regionName, 2);
    // the following two should fail with name check on locator, not on server
    gfsh.executeAndAssertThat("create region --type=PARTITION --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("Region /" + regionName + " already exists on the cluster");
    gfsh.executeAndAssertThat(
        "create region --type=PARTITION_PROXY --group=group2 --name=" + regionName).statusIsError()
        .containsOutput("The existing region is not a partitioned region");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void startWithReplicateProxyThenPartitionRegion() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE_PROXY --group=group1 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("create region --type=PARTITION --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("The existing region is not a partitioned region");
    gfsh.executeAndAssertThat(
        "create region --type=PARTITION_PROXY --group=group2 --name=" + regionName).statusIsError()
        .containsOutput("The existing region is not a partitioned region");
    gfsh.executeAndAssertThat("create region --type=LOCAL --group=group2 --name=" + regionName)
        .statusIsError();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void startWithPartitionProxyThenReplicate() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --type=PARTITION_PROXY --group=group1 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --type=REPLICATE --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("The existing region is not a replicate region");
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE_PROXY --group=group2 --name=" + regionName).statusIsError()
        .containsOutput("The existing region is not a replicate region");
    gfsh.executeAndAssertThat("create region --type=LOCAL --group=group2 --name=" + regionName)
        .statusIsError();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void startWithLocalPersistent() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --type=LOCAL_PERSISTENT --group=group1 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --type=REPLICATE --group=group2 --name=" + regionName)
        .statusIsError()
        .containsOutput("Region /startWithLocalPersistent already exists on the cluster");
    gfsh.executeAndAssertThat("create region --type=LOCAL --group=group2 --name=" + regionName)
        .statusIsError()
        .containsOutput("Region /startWithLocalPersistent already exists on the cluster");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void startWithReplicateHeapLRU() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE_HEAP_LRU --group=group1 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --type=REPLICATE --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("already exists on the cluster");
    gfsh.executeAndAssertThat("create region --type=LOCAL --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("already exists on the cluster");
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE_PROXY --group=group2 --name=" + regionName)
        .statusIsSuccess();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void startWithReplicateThenPartition() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --type=REPLICATE --group=group1 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --type=PARTITION --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("already exists on the cluster");
    gfsh.executeAndAssertThat(
        "create region --type=PARTITION_PROXY --group=group2 --name=" + regionName).statusIsError()
        .containsOutput("The existing region is not a partitioned region");
    gfsh.executeAndAssertThat("create region --type=LOCAL --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("already exists on the cluster");
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE_PROXY --group=group1 --name=" + regionName).statusIsError()
        .containsOutput("already exists on these members: server-1");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void startWithPartitionThenReplicate() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --type=PARTITION --group=group1 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --type=REPLICATE --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("already exists on the cluster");
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE_PROXY --group=group2 --name=" + regionName).statusIsError()
        .containsOutput("The existing region is not a replicate region");
    gfsh.executeAndAssertThat("create region --type=LOCAL --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("already exists on the cluster");
    gfsh.executeAndAssertThat(
        "create region --type=PARTITION_PROXY --group=group1 --name=" + regionName).statusIsError()
        .containsOutput("already exists on these members: server-1");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void startWithPartitionProxyRegion() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --type=PARTITION_PROXY --group=group1 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-1");
    gfsh.executeAndAssertThat("create region --type=PARTITION --group=group1 --name=" + regionName)
        .statusIsError().containsOutput("already exists on these members: server-1");
    gfsh.executeAndAssertThat("create region --type=PARTITION --group=group2 --name=" + regionName)
        .statusIsSuccess().tableHasRowWithValues("Member", "server-2");

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + regionName, 2);
    gfsh.executeAndAssertThat("create region --type=PARTITION --group=group2 --name=" + regionName)
        .statusIsError().containsOutput("Region /" + regionName + " already exists on the cluster");
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE_PROXY --group=group2 --name=" + regionName).statusIsError()
        .containsOutput("The existing region is not a replicate region");
    gfsh.executeAndAssertThat(
        "create region --type=PARTITION_PROXY --group=group1 --name=" + regionName).statusIsError()
        .containsOutput("already exists on these members: server-1");
  }

  @Test
  public void startWithLocalRegion() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --type=LOCAL --group=group1 --name=" + regionName)
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region --type=REPLICATE --name=" + regionName).statusIsError()
        .containsOutput("Region /startWithLocalRegion already exists on the cluster.");
    gfsh.executeAndAssertThat("create region --type=PARTITION --group=group2 --name=" + regionName)
        .statusIsError()
        .containsOutput("Region /startWithLocalRegion already exists on the cluster.");
    gfsh.executeAndAssertThat(
        "create region --type=PARTITION_PROXY --group=group2 --name=" + regionName).statusIsError()
        .containsOutput("Region /startWithLocalRegion already exists on the cluster");
  }

  @Test
  public void cannotDisableCloningWhenCompressorIsSet() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=REPLICATE"
        + " --compressor=" + DummyCompressor.class.getName()
        + " --enable-cloning=false").statusIsError();
  }

  @Test
  public void cannotColocateWithNonexistentRegion() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=PARTITION"
        + " --colocated-with=/nonexistent").statusIsError()
        .containsOutput("Specify a valid region path for colocated-with")
        .containsOutput("nonexistent not found");
  }

  @Test
  public void cannotColocateWithNonPartitionedRegion() {
    gfsh.executeAndAssertThat("create region --type=REPLICATE --name=/nonpartitioned")
        .statusIsSuccess();

    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=PARTITION"
        + " --colocated-with=/nonpartitioned").statusIsError()
        .containsOutput("\"/nonpartitioned\" is not a Partitioned Region");
  }

  @Test
  public void cannotCreateSubregionOnNonExistentParentRegion() {
    gfsh.executeAndAssertThat("create region --type=REPLICATE --name=/nonexistentparent/child")
        .statusIsError()
        .containsOutput("Parent region for \"/nonexistentparent/child\" does not exist");
  }

  @Test
  public void cannotCreateWithNonexistentGatewaySenders() {
    gfsh.executeAndAssertThat(
        "create region --type=REPLICATE --name=/invalid --gateway-sender-id=nonexistent")
        .statusIsError()
        .containsOutput("There are no GatewaySenders");
  }

  @Test
  public void testCreateRegionFromTemplateWithAsyncEventListeners() {
    String queueId = "queue1";
    gfsh.executeAndAssertThat(
        "create async-event-queue --id=" + queueId
            + " --listener=" + CreateRegionCommandDUnitTest.DummyAEQListener.class.getName())
        .statusIsSuccess();

    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat(
        "create region --name=" + regionName
            + " --type=REPLICATE"
            + " --async-event-queue-id=" + queueId)
        .statusIsSuccess();

    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + "-from-template"
            + " --template-region=" + regionName)
        .statusIsSuccess();

    server1.invoke(() -> {
      Region<?, ?> regionFromTemplate = ClusterStartupRule.getCache()
          .getRegion(regionName + "-from-template");
      assertThat(regionFromTemplate).isNotNull();
      @SuppressWarnings("unchecked")
      final Set<String> asyncEventQueueIds =
          ((InternalRegion) regionFromTemplate).getAsyncEventQueueIds();
      assertThat(asyncEventQueueIds).contains(queueId);
    });
  }

  @Test
  public void testCreateRegionFromTemplateWithPartitionResolver() {
    String regionName = testName.getMethodName();
    String regionFromTemplateName = regionName + "-from-template";

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=PARTITION"
        + " --partition-resolver=" + DummyPartitionResolver.class.getName()).statusIsSuccess();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionFromTemplateName
        + " --template-region=" + regionName).statusIsSuccess();

    server1.invoke(() -> {
      Region<?, ?> regionFromTemplate = ClusterStartupRule.getCache()
          .getRegion(regionName + "-from-template");
      assertThat(regionFromTemplate).isNotNull();
      assertThat(((InternalRegion) regionFromTemplate).getPartitionAttributes()
          .getPartitionResolver())
              .isNotNull();
      assertThat(((InternalRegion) regionFromTemplate).getPartitionAttributes()
          .getPartitionResolver().getName())
              .isEqualTo("dummy");
    });
  }

  @Test
  @SuppressWarnings("deprecation")
  public void createRegionCommandCreateCorrectClusterConfigXml() {
    URL xmlResource =
        CreateRegionCommandDUnitTest.class.getResource("CreateRegionCommandDUnitTest.xml");

    RegionShortcut[] shortcuts = RegionShortcut.values();
    for (RegionShortcut shortcut : shortcuts) {
      gfsh.executeAndAssertThat(
          "create region --name=" + shortcut.name() + " --type=" + shortcut.name());
    }

    locator.invoke(() -> {
      InternalConfigurationPersistenceService persistenceService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig expected = persistenceService.getJaxbService()
          .unMarshall(FileUtils.readFileToString(new File(xmlResource.getFile()), "UTF-8"));

      CacheConfig actual = persistenceService.getCacheConfig("cluster");

      for (RegionShortcut shortcut : shortcuts) {
        assertThat(find(actual.getRegions(), shortcut.name()))
            .isEqualToComparingFieldByFieldRecursively(
                find(expected.getRegions(), shortcut.name()));
      }
    });
  }

  private String getUniversalClassCode(String classname) {
    return "package io.pivotal;" + "import org.apache.geode.cache.CacheLoader;"
        + "import org.apache.geode.cache.CacheLoaderException;"
        + "import org.apache.geode.cache.CacheWriter;"
        + "import org.apache.geode.cache.CacheWriterException;"
        + "import org.apache.geode.cache.EntryEvent;"
        + "import org.apache.geode.cache.LoaderHelper;"
        + "import org.apache.geode.cache.RegionEvent;"
        + "import org.apache.geode.cache.util.CacheListenerAdapter;"
        + "import org.apache.geode.compression.Compressor;" + "public class " + classname
        + " extends CacheListenerAdapter "
        + "implements CacheWriter, CacheLoader, Compressor, java.io.Serializable {"
        + "public void beforeUpdate(EntryEvent event) throws CacheWriterException {}"
        + "public void beforeCreate(EntryEvent event) throws CacheWriterException {}"
        + "public void beforeDestroy(EntryEvent event) throws CacheWriterException {}"
        + "public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {}"
        + "public void beforeRegionClear(RegionEvent event) throws CacheWriterException {}"
        + "public Object load(LoaderHelper helper) throws CacheLoaderException { return null; }"
        + "public byte[] compress(byte[] input) { return new byte[0]; }"
        + "public byte[] decompress(byte[] input) { return new byte[0]; }" + "}";
  }

}
