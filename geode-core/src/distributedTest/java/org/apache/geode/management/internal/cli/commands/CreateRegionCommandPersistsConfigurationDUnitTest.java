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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesScope;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.compression.Compressor;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({RegionsTest.class})
public class CreateRegionCommandPersistsConfigurationDUnitTest {

  private MemberVM locator, server1;

  @Rule
  public ClusterStartupRule clusterRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  public static class DummyCacheListener extends CacheListenerAdapter {
  }

  public static class DummyCustomExpiry implements CustomExpiry, Declarable {
    @Override
    public ExpirationAttributes getExpiry(Region.Entry entry) {
      return null;
    }
  }

  public static class DummyPartitionResolver implements PartitionResolver, Declarable {
    @Override
    public Object getRoutingObject(EntryOperation opDetails) {
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

  public static class DummyCacheLoader implements CacheLoader, Declarable {
    @Override
    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return null;
    }
  }

  public static class DummyCacheWriter extends CacheWriterAdapter {
  }

  @Before
  public void before() throws Exception {
    locator = clusterRule.startLocatorVM(0);
    server1 = clusterRule.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void createRegionPersistsEmptyConfig() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=REPLICATE")
        .statusIsSuccess();

    server1.stop();
    server1 = clusterRule.startServerVM(1, "group1", locator.getPort());

    gfsh.executeAndAssertThat("list regions")
        .statusIsSuccess().containsOutput(regionName);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      RegionConfig regionConfig = regions.get(0);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);
      assertThat(regionConfig.getIndexes()).isEmpty();
      assertThat(regionConfig.getRegions()).isEmpty();
      assertThat(regionConfig.getEntries()).isEmpty();
      assertThat(regionConfig.getCustomRegionElements()).isEmpty();
    });
  }

  @Test
  public void createRegionPersistsConfigParams() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region --name=" + regionName + " --type=PARTITION"
        + " --enable-statistics=true" + " --enable-async-conflation=true"
        + " --entry-idle-time-expiration=100").statusIsSuccess();

    server1.stop();
    server1 = clusterRule.startServerVM(1, "group1", locator.getPort());

    gfsh.executeAndAssertThat("list regions")
        .statusIsSuccess().containsOutput(regionName);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      RegionConfig regionConfig = regions.get(0);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);
      assertThat(regionConfig.getRegionAttributes()).isNotNull();

      RegionAttributesType attr = regionConfig.getRegionAttributes();
      assertThat(attr.isStatisticsEnabled()).isTrue();
      assertThat(attr.isEnableAsyncConflation()).isTrue();

      RegionAttributesType.ExpirationAttributesType entryIdleTimeExp = attr.getEntryIdleTime();
      assertThat(entryIdleTimeExp.getTimeout()).isEqualTo("100");
    });

    server1.invoke(() -> {
      Region<?, ?> region = ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.getAttributes().getStatisticsEnabled())
          .describedAs("Expecting statistics to be enabled")
          .isTrue();
      assertThat(region.getAttributes().getEnableAsyncConflation())
          .describedAs("Expecting async conflation to be enabled")
          .isTrue();
      assertThat(region.getAttributes().getEntryIdleTimeout().getTimeout())
          .describedAs("Expecting entry idle time exp timeout to be 100")
          .isEqualTo(100);
    });
  }

  @Test
  public void createRegionFromTemplateCreatesCorrectConfig() {
    String regionName = testName.getMethodName();
    String templateRegionName = regionName + "_template";
    gfsh.executeAndAssertThat("create region"
        + " --name=" + templateRegionName
        + " --type=PARTITION"
        + " --cache-listener=" + DummyCacheListener.class.getName()
        + " --enable-statistics=true"
        + " --enable-async-conflation=true"
        + " --entry-idle-time-expiration=100").statusIsSuccess();

    gfsh.executeAndAssertThat(
        "create region --name=" + regionName + " --template-region=" + templateRegionName);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      RegionConfig regionConfig = CacheElement.findElement(config.getRegions(), regionName);

      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);
      assertThat(regionConfig.getRegionAttributes()).isNotNull();

      RegionAttributesType attr = regionConfig.getRegionAttributes();
      assertThat(attr.isStatisticsEnabled()).isTrue();
      assertThat(attr.isEnableAsyncConflation()).isTrue();

      RegionAttributesType.ExpirationAttributesType entryIdleTimeExp = attr.getEntryIdleTime();
      assertThat(entryIdleTimeExp.getTimeout()).isEqualTo("100");
    });
  }

  @Test
  public void createRegionAndValidateAllConfigIsPersistedForReplicatedRegion() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=REPLICATE"
        + " --cache-listener=" + DummyCacheListener.class.getName()
        + " --cache-loader=" + DummyCacheLoader.class.getName()
        + " --cache-writer=" + DummyCacheWriter.class.getName()
        + " --compressor=" + DummyCompressor.class.getName()
        + " --enable-async-conflation=false"
        + " --enable-concurrency-checks=false"
        + " --enable-multicast=false"
        + " --concurrency-level=1"
        + " --enable-statistics=true"
        + " --enable-subscription-conflation=true"
        + " --entry-idle-time-expiration=100"
        + " --entry-idle-time-expiration-action=local-destroy"
        + " --entry-time-to-live-expiration=200"
        + " --entry-time-to-live-expiration-action=local-destroy"
        + " --eviction-action=local-destroy"
        + " --eviction-entry-count=1000"
        + " --key-constraint=" + Object.class.getName()
        + " --off-heap=false"
        + " --region-idle-time-expiration=100"
        + " --region-idle-time-expiration-action=local-destroy"
        + " --region-time-to-live-expiration=200"
        + " --region-time-to-live-expiration-action=local-destroy"
        + " --value-constraint=" + Object.class.getName()).statusIsSuccess();

    String regionNameFromTemplate = regionName + "-from-template";
    gfsh.executeAndAssertThat("create region --name=" + regionNameFromTemplate
        + " --template-region=" + regionName)
        .statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(2);

      List<String> regionNames = Arrays.asList(regionName, regionNameFromTemplate);
      regionNames.forEach(name -> {
        RegionConfig regionConfig = CacheElement.findElement(config.getRegions(), name);
        assertThat(regionConfig).isNotNull();
        assertThat(regionConfig.getName()).isEqualTo(name);
        assertThat(regionConfig.getRegionAttributes()).isNotNull();

        RegionAttributesType attr = regionConfig.getRegionAttributes();
        assertThat(attr.getCacheListeners().get(0).toString())
            .describedAs("Expecting one cache listener for region " + name)
            .isEqualTo(DummyCacheListener.class.getName());
        assertThat(attr.getCacheLoader().toString())
            .describedAs("Expecting a DummyCacheLoader for region " + name)
            .isEqualTo(DummyCacheLoader.class.getName());
        assertThat(attr.getCacheWriter().toString())
            .describedAs("Expecting a DummyCacheWriter for region " + name)
            .isEqualTo(DummyCacheWriter.class.getName());
        assertThat(attr.getCompressor().toString())
            .describedAs("Expecting a DummyCompressor for region " + name)
            .isEqualTo(DummyCompressor.class.getName());
        assertThat(attr.isEnableAsyncConflation())
            .describedAs("Expecting async conflation to not be enabled for region "
                + name)
            .isFalse();
        assertThat(attr.isConcurrencyChecksEnabled())
            .describedAs("Expecting concurrency checks not to be enabled for region "
                + name)
            .isFalse();
        assertThat(attr.isMulticastEnabled())
            .describedAs("Expecting multicast is not enabled for region " + name)
            .isFalse();
        assertThat(attr.getConcurrencyLevel())
            .describedAs("Expecting concurrency level to be 1 for region " + name)
            .isEqualTo("1");
        assertThat(attr.isStatisticsEnabled())
            .describedAs("Expecting statistics to be enabled for region " + name)
            .isTrue();
        assertThat(attr.isCloningEnabled())
            .describedAs("Expecting cloning to be enabled for region " + name
                + " since compressor is provided")
            .isTrue();
        assertThat(attr.isEnableSubscriptionConflation())
            .describedAs("Expecting subscription conflation to be enabled for region "
                + name)
            .isTrue();
        assertThat(attr.getEntryIdleTime().getTimeout())
            .describedAs("Entry idle time timeout should be 100 for region " + name)
            .isEqualTo("100");
        assertThat(attr.getEntryIdleTime().getAction())
            .describedAs("Entry idle time expiration action should be local-destroy for region "
                + name)
            .isEqualTo("local-destroy");
        assertThat(attr.getEntryTimeToLive().getTimeout())
            .describedAs("Expecting entry time to live expiration to be 200 for region "
                + name)
            .isEqualTo("200");
        assertThat(attr.getEntryTimeToLive().getAction())
            .describedAs("Entry time to live expiration action should be local-destroy "
                + "for region " + name)
            .isEqualTo("local-destroy");
        assertThat(attr.getEvictionAttributes().getLruEntryCount().getAction().value())
            .describedAs("Eviction action should be local-destroy for region " + name)
            .isEqualTo("local-destroy");
        assertThat(attr.getEvictionAttributes().getLruEntryCount().getMaximum())
            .describedAs("Eviction max should be 1000 for region " + name)
            .isEqualTo("1000");
        assertThat(attr.getKeyConstraint())
            .describedAs("Expected key constraint to be " + Object.class.getName() +
                " for region " + name)
            .isEqualTo(Object.class.getName());
        assertThat(attr.isOffHeap())
            .describedAs("Expected off heap to be false for region " + name)
            .isFalse();
        assertThat(attr.getRegionIdleTime().getTimeout())
            .describedAs("Expecting region idle time expiration to be 100 for region "
                + name)
            .isEqualTo("100");
        assertThat(attr.getRegionIdleTime().getAction())
            .describedAs("Expecting region idle time expiration action to be "
                + "local-destroy for region " + name)
            .isEqualTo("local-destroy");
        assertThat(attr.getRegionTimeToLive().getTimeout())
            .describedAs("Expecting region idle time timeout to be 200 for "
                + "region " + name)
            .isEqualTo("200");
        assertThat(attr.getRegionTimeToLive().getAction())
            .describedAs("Expecting region ttl action to be local-destroy for "
                + "region " + name)
            .isEqualTo("local-destroy");
        assertThat(attr.getValueConstraint())
            .describedAs("Expecting value constraint to be Object.class for "
                + "region " + name)
            .isEqualTo(Object.class.getName());
      });
    });
  }

  @Test
  public void createRegionDoesNotPersistEmptyOrDefaultEvictionAttributes() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=REPLICATE").statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(1);

      List<String> regionNames = Arrays.asList(regionName);
      regionNames.forEach(name -> {
        RegionConfig regionConfig = CacheElement.findElement(config.getRegions(), name);
        assertThat(regionConfig).isNotNull();
        assertThat(regionConfig.getName()).isEqualTo(name);
        assertThat(regionConfig.getRegionAttributes()).isNotNull();

        RegionAttributesType attr = regionConfig.getRegionAttributes();
        assertThat(attr.getEvictionAttributes())
            .describedAs("Eviction attributes should be null for " + name)
            .isNull();
      });
    });
  }

  @Test
  public void createRegionPersistsAEQConfig() {
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

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(1);
      RegionConfig regionConfig = CacheElement.findElement(regions, regionName);
      assertThat(regionConfig.getRegionAttributes().getAsyncEventQueueIds())
          .contains(queueId);
    });
  }

  @Test
  public void createRegionWithColocation() {
    String regionName = testName.getMethodName();
    String colocatedRegionName = regionName + "-colocated";
    String colocatedRegionFromTemplateName = colocatedRegionName + "-from-template";

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=PARTITION_REDUNDANT").statusIsSuccess();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + colocatedRegionName
        + " --colocated-with=" + regionName
        + " --type=PARTITION_REDUNDANT").statusIsSuccess();

    gfsh.executeAndAssertThat("create region"
        + " --name=" + colocatedRegionFromTemplateName
        + " --template-region=" + colocatedRegionName).statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(3);

      RegionConfig colocatedConfig = CacheElement.findElement(regions, colocatedRegionName);
      assertThat(
          colocatedConfig.getRegionAttributes().getPartitionAttributes().getColocatedWith())
              .isEqualTo("/" + regionName);

      RegionConfig colocatedConfigFromTemplate = CacheElement.findElement(regions,
          colocatedRegionFromTemplateName);
      assertThat(
          colocatedConfigFromTemplate.getRegionAttributes().getPartitionAttributes()
              .getColocatedWith())
                  .isEqualTo("/" + regionName);
    });
  }

  @Test
  public void createRegionPersistsDiskstores() throws Exception {
    String regionName = testName.getMethodName();
    String store = "Store1";
    gfsh.executeAndAssertThat("create disk-store"
        + " --name=" + store
        + " --dir=/tmp/foo").statusIsSuccess();

    // Give disk store time to get created
    Thread.sleep(2000);

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=REPLICATE_PERSISTENT"
        + " --disk-store=" + store
        + " --enable-synchronous-disk=true").statusIsSuccess();

    String regionNameFromTemplate = regionName + "-from-template";
    gfsh.executeAndAssertThat("create region --name=" + regionNameFromTemplate
        + " --template-region=" + regionName)
        .statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(2);

      List<String> regionNames = Arrays.asList(regionName, regionNameFromTemplate);
      regionNames.forEach(name -> {
        RegionConfig regionConfig = CacheElement.findElement(config.getRegions(), name);
        assertThat(regionConfig).isNotNull();
        assertThat(regionConfig.getName()).isEqualTo(name);

        RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
        assertThat(regionAttributes.getDiskStoreName())
            .isEqualTo(store);
        assertThat(regionAttributes.isDiskSynchronous())
            .isTrue();
      });
    });
  }

  @Test
  public void createRegionPersistsPartitionAttributes() {
    String regionName = testName.getMethodName();
    String regionFromTemplateName = regionName + "-from-template";

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=PARTITION"
        + " --recovery-delay=1"
        + " --local-max-memory=1000"
        + " --redundant-copies=1"
        + " --startup-recovery-delay=1"
        + " --total-max-memory=100"
        + " --total-num-buckets=1").statusIsSuccess();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionFromTemplateName
        + " --template-region=" + regionName).statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(2);

      List<String> regionNames = Arrays.asList(regionName, regionFromTemplateName);
      regionNames.forEach(name -> {
        RegionConfig regionConfig = CacheElement.findElement(config.getRegions(), name);
        assertThat(regionConfig).isNotNull();
        assertThat(regionConfig.getName()).isEqualTo(name);

        RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
        RegionAttributesType.PartitionAttributes partitionAttributes =
            regionAttributes.getPartitionAttributes();

        assertThat(partitionAttributes.getRecoveryDelay())
            .describedAs("Recovery delay should be 1 for region " + name)
            .isEqualTo("1");
        assertThat(partitionAttributes.getLocalMaxMemory())
            .describedAs("Local max memory should be 1000 for region " + name)
            .isEqualTo("1000");
        assertThat(partitionAttributes.getRedundantCopies())
            .describedAs("Redundant copies should be 1 for region " + name)
            .isEqualTo("1");
        assertThat(partitionAttributes.getStartupRecoveryDelay())
            .describedAs("Startup recovery delay should be 1 for region " + name)
            .isEqualTo("1");
        assertThat(partitionAttributes.getTotalMaxMemory())
            .describedAs("Total max memory should be 100 for region " + name)
            .isEqualTo("100");
        assertThat(partitionAttributes.getTotalNumBuckets())
            .describedAs("Total num buckets should be 1 for region " + name)
            .isEqualTo("1");
      });
    });
  }

  @Test
  public void createRegionPersistsPartitionResolver() {
    String regionName = testName.getMethodName();

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=PARTITION"
        + " --partition-resolver=" + DummyPartitionResolver.class.getName()).statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(1);

      RegionConfig regionConfig = CacheElement.findElement(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);

      RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
      RegionAttributesType.PartitionAttributes partitionAttributes =
          regionAttributes.getPartitionAttributes();

      assertThat(partitionAttributes.getPartitionResolver().getClassName())
          .isEqualTo(DummyPartitionResolver.class.getName());
    });
  }

  @Test
  public void createRegionDoesNotPersistEmptyOrDefaultPartitionAttributes() {
    String regionName = testName.getMethodName();
    String regionFromTemplateName = regionName + "-from-template";

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=REPLICATE").statusIsSuccess();

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionFromTemplateName
        + " --template-region=" + regionName);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(2);

      RegionConfig regionConfig =
          CacheElement.findElement(config.getRegions(), regionFromTemplateName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionFromTemplateName);
      assertThat(regionConfig.getRegionAttributes()).isNotNull();

      RegionAttributesType attr = regionConfig.getRegionAttributes();
      assertThat(attr.getPartitionAttributes())
          .describedAs("Partition attributes should be null for " + regionFromTemplateName)
          .isNull();
    });
  }

  @Test
  public void createRegionDoestNotPersistEmptyOrDefaultExpirationAttributes() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=REPLICATE").statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(1);

      RegionConfig regionConfig =
          CacheElement.findElement(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);
      assertThat(regionConfig.getRegionAttributes()).isNotNull();

      RegionAttributesType attr = regionConfig.getRegionAttributes();
      assertThat(attr.getRegionTimeToLive())
          .describedAs("Expiration attributes should be null for " + regionName)
          .isNull();
      assertThat(attr.getRegionIdleTime())
          .describedAs("Expiration attributes should be null for " + regionName)
          .isNull();
      assertThat(attr.getEntryIdleTime())
          .describedAs("Expiration attributes should be null for " + regionName)
          .isNull();
      assertThat(attr.getEntryTimeToLive())
          .describedAs("Expiration attributes should be null for " + regionName)
          .isNull();
    });
  }

  @Test
  public void createRegionPersistsDisableCloningSetting() {
    String regionName = testName.getMethodName();
    String regionFromTemplateName = regionName + "-from-template";

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=REPLICATE"
        + " --enable-cloning=false").statusIsSuccess();

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionFromTemplateName
        + " --template-region=" + regionName);

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(2);

      List<String> regionNames = Arrays.asList(regionName, regionFromTemplateName);
      regionNames.forEach(name -> {
        RegionConfig regionConfig = CacheElement.findElement(config.getRegions(), name);
        assertThat(regionConfig).isNotNull();
        assertThat(regionConfig.getName()).isEqualTo(name);
        assertThat(regionConfig.getRegionAttributes()).isNotNull();

        RegionAttributesType attr = regionConfig.getRegionAttributes();
        assertThat(attr.isCloningEnabled())
            .describedAs("Cloning should be disabled for " + name)
            .isFalse();
      });
    });
  }

  @Test
  public void createRegionPersistsCustomExpiryClass() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=REPLICATE"
        + " --enable-statistics=true"
        + " --entry-idle-time-custom-expiry=" + DummyCustomExpiry.class.getName())
        .statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(1);

      RegionConfig regionConfig = CacheElement.findElement(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);
      assertThat(regionConfig.getRegionAttributes()).isNotNull();
      RegionAttributesType attr = regionConfig.getRegionAttributes();
      assertThat(attr.getEntryIdleTime().getCustomExpiry().toString())
          .describedAs("Entry expiration custom expiration should be DummyCustomExpiry")
          .isEqualTo(DummyCustomExpiry.class.getName());
    });
  }

  @Test
  public void createRegionPersistsDataPolicy() {
    String regionName = testName.getMethodName();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=PARTITION")
        .statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(1);

      RegionConfig regionConfig = CacheElement.findElement(config.getRegions(), regionName);
      assertThat(regionConfig).isNotNull();
      assertThat(regionConfig.getName()).isEqualTo(regionName);
      assertThat(regionConfig.getRegionAttributes()).isNotNull();

      RegionAttributesType attr = regionConfig.getRegionAttributes();
      assertThat(attr.getDataPolicy())
          .describedAs("Data policy for partitioned region should be persisted correctly")
          .isEqualTo(RegionAttributesDataPolicy.PARTITION);
    });
  }

  @Test
  public void createRegionPersistsScope() {
    String regionName = testName.getMethodName();
    String regionName2 = regionName + "2";
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=PARTITION")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName2
        + " --type=REPLICATE")
        .statusIsSuccess();

    locator.invoke(() -> {
      InternalConfigurationPersistenceService cc =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig config = cc.getCacheConfig("cluster");

      List<RegionConfig> regions = config.getRegions();
      assertThat(regions).isNotEmpty();
      assertThat(regions).hasSize(2);

      RegionConfig regionConfig1 = CacheElement.findElement(config.getRegions(), regionName);
      assertThat(regionConfig1).isNotNull();
      assertThat(regionConfig1.getName()).isEqualTo(regionName);
      assertThat(regionConfig1.getRegionAttributes()).isNotNull();

      RegionAttributesType attr1 = regionConfig1.getRegionAttributes();
      assertThat(attr1.getScope())
          .describedAs("Scope for partitioned region should be null")
          .isNull();

      RegionConfig regionConfig2 = CacheElement.findElement(config.getRegions(), regionName2);
      assertThat(regionConfig2).isNotNull();
      assertThat(regionConfig2.getName()).isEqualTo(regionName2);
      assertThat(regionConfig2.getRegionAttributes()).isNotNull();

      RegionAttributesType attr2 = regionConfig2.getRegionAttributes();
      assertThat(attr2.getScope())
          .describedAs(
              "Scope for replicated region should be persisted as distributed-ack by default")
          .isEqualTo(RegionAttributesScope.DISTRIBUTED_ACK);
    });
  }
}
