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
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({RegionsTest.class})
public class CreateRegionCommandIntegrationTest {

  private static String CREATE_REGION = "create region --type=REPLICATE ";

  public static class TestCacheListener extends CacheListenerAdapter<Object, Object> {
  }

  public static class TestConstraint {
  }

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withRegion(RegionShortcut.REPLICATE, "REPLICATED");

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
  }

  @Test
  public void parentRegionDoesNotExist() {
    gfsh.executeAndAssertThat(CREATE_REGION + "--name=/A/B").statusIsError()
        .containsOutput("Parent region for \"/A/B\" does not exist");
  }

  @Test
  public void groupDoesNotExist() {
    gfsh.executeAndAssertThat(CREATE_REGION + "--name=/FOO --groups=unknown").statusIsError()
        .containsOutput("Group(s) \"unknown\" are invalid");
  }

  @Test
  public void templateRegionDoesNotExist() {
    gfsh.executeAndAssertThat("create region --name=/FOO --template-region=/BAR").statusIsError()
        .containsOutput("Template region /BAR does not exist");
  }

  @Test
  public void conflictingPartitionAttributesWithTemplate() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --template-region=REPLICATED --redundant-copies=2")
        .statusIsError().containsOutput(
            "can be used only for creating a Partitioned Region");
  }

  @Test
  public void conflictingPartitionAttributesWithShortCut() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=REPLICATE --redundant-copies=2")
        .statusIsError().containsOutput("can be used only for creating a Partitioned Region");
  }

  @Test
  public void colocatedWithRegionDoesNotExist() {
    gfsh.executeAndAssertThat("create region --type=PARTITION --name=/FOO --colocated-with=/BAR")
        .statusIsError().containsOutput("Specify a valid region path for colocated-with");
  }

  @Test
  public void colocatedWithRegionIsNotPartitioned() {
    gfsh.executeAndAssertThat(
        "create region --type=PARTITION --name=/FOO --colocated-with=/REPLICATED").statusIsError()
        .containsOutput("colocated-with \"/REPLICATED\" is not a Partitioned Region");
  }

  @Test
  public void negativeLocalMaxMemory() {
    gfsh.executeAndAssertThat("create region --type=PARTITION --name=/FOO --local-max-memory=-1")
        .statusIsError().containsOutput("PartitionAttributes localMaxMemory must not be negative");
  }

  @Test
  public void zeroLocalMaxMemoryIsOK() {
    gfsh.executeAndAssertThat("create region --type=PARTITION --name=/FOO --local-max-memory=0")
        .statusIsSuccess().containsOutput("Region \"/FOO\" created");
    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void negativeTotalMaxMemory() {
    gfsh.executeAndAssertThat("create region --type=PARTITION --name=/FOO --total-max-memory=-1")
        .statusIsError().containsOutput("Total size of partition region must be > 0");
  }

  @Test
  public void zeroTotalMaxMemory() {
    gfsh.executeAndAssertThat("create region --type=PARTITION --name=/FOO --total-max-memory=0")
        .statusIsError().containsOutput("Total size of partition region must be > 0");
  }

  @Test
  public void redundantCopies() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=PARTITION --redundant-copies=2")
        .statusIsSuccess().containsOutput("Region \"/FOO\" created");

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void tooManyredundantCopies() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=PARTITION --redundant-copies=4")
        .statusIsError().containsOutput("redundant-copies \"4\" is not valid");
  }

  @Test
  public void keyConstraint() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=REPLICATE --key-constraint=abc-def")
        .statusIsError().containsOutput("Specify a valid class name for key-constraint");
  }

  @Test
  public void valueConstraint() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --type=REPLICATE --value-constraint=abc-def").statusIsError()
        .containsOutput("Specify a valid class name for value-constraint");
  }

  @Test
  public void ifNotExistsIsIdempotent() {
    gfsh.executeAndAssertThat(
        "create region --if-not-exists --type=PARTITION --name=/FOO --local-max-memory=0")
        .statusIsSuccess().containsOutput("Region \"/FOO\" created");

    gfsh.executeAndAssertThat(
        "create region --skip-if-exists --type=PARTITION --name=/FOO --local-max-memory=0")
        .statusIsSuccess().containsOutput("Region /FOO already exists on these members: server.");

    gfsh.executeAndAssertThat(
        "create region --if-not-exists --type=PARTITION --name=/FOO --local-max-memory=0")
        .statusIsSuccess().containsOutput("Region /FOO already exists on these members: server.");

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void invalidCacheListener() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=REPLICATE --cache-listener=abc-def")
        .statusIsError().containsOutput(
            "java.lang.IllegalArgumentException: Failed to convert 'abc-def' to type ClassName");
  }

  @Test
  public void invalidCacheLoader() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=REPLICATE --cache-loader=abc-def")
        .statusIsError().containsOutput(
            "java.lang.IllegalArgumentException: Failed to convert 'abc-def' to type ClassName");
  }

  @Test
  public void invalidCacheWriter() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=REPLICATE --cache-writer=abc-def")
        .statusIsError().containsOutput(
            "java.lang.IllegalArgumentException: Failed to convert 'abc-def' to type ClassName");
  }

  @Test
  public void invalidGatewaySenders() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --type=REPLICATE --gateway-sender-id=unknown").statusIsError()
        .containsOutput("There are no GatewaySenders defined currently in the system");
  }

  // TODO: Write test for invalid gateway name (gateways already need to exist).

  @Test
  public void invalidConcurrencyLevel() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --template-region=/REPLICATED --concurrency-level=-1")
        .statusIsError().containsOutput("Specify positive integer value for concurrency-level");
  }

  @Test
  public void nonPersistentRegionWithdiskStore() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=REPLICATE --disk-store=unknown")
        .statusIsError()
        .containsOutput("Only regions with persistence or overflow to disk can specify DiskStore");
  }

  @Test
  public void nonPersistentTemplateWithdiskStore() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --template-region=/REPLICATED --disk-store=unknown")
        .statusIsError().containsOutput("template-region region \"/REPLICATED\" is not persistent")
        .containsOutput("Only regions with persistence or overflow to disk can specify DiskStore");
  }

  @Test
  public void nonPersistentReplicateOverflowRegionWithdiskStore() {
    gfsh.executeAndAssertThat(
        "create disk-store --name=DISKSTORE --dir=DISKSTORE --auto-compact=false" +
            " --compaction-threshold=99 --max-oplog-size=1 --allow-force-compaction=true")
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");
    gfsh.executeAndAssertThat("create region --name=/OVERFLOW --type=REPLICATE_OVERFLOW" +
        " --eviction-action=overflow-to-disk --eviction-entry-count=1000 --disk-store=DISKSTORE")
        .statusIsSuccess();

    gfsh.executeAndAssertThat("destroy region --name=/OVERFLOW").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy disk-store --name=DISKSTORE").statusIsSuccess();
  }

  @Test
  public void nonPersistentPartitionOverflowRegionWithdiskStore() {
    gfsh.executeAndAssertThat(
        "create disk-store --name=DISKSTORE --dir=DISKSTORE --auto-compact=false" +
            " --compaction-threshold=99 --max-oplog-size=1 --allow-force-compaction=true")
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");
    gfsh.executeAndAssertThat("create region --name=/OVERFLOW --type=PARTITION_OVERFLOW" +
        " --eviction-action=overflow-to-disk --eviction-entry-count=1000 --disk-store=DISKSTORE")
        .statusIsSuccess();

    gfsh.executeAndAssertThat("destroy region --name=/OVERFLOW").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy disk-store --name=DISKSTORE").statusIsSuccess();
  }

  @Test
  public void nonPersistentTemplateOverflowRegionWithdiskStore() {
    gfsh.executeAndAssertThat(
        "create disk-store --name=DISKSTORE --dir=DISKSTORE --auto-compact=false" +
            " --compaction-threshold=99 --max-oplog-size=1 --allow-force-compaction=true")
        .statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting");
    gfsh.executeAndAssertThat("create region --name=/OVERFLOW --type=PARTITION_OVERFLOW" +
        " --eviction-action=overflow-to-disk --eviction-entry-count=1000 --disk-store=DISKSTORE")
        .statusIsSuccess();

    gfsh.executeAndAssertThat(
        "create region --name=/TEMPLATE --template-region=/OVERFLOW --disk-store=DISKSTORE")
        .statusIsSuccess();

    gfsh.executeAndAssertThat("destroy region --name=/TEMPLATE").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy region --name=/OVERFLOW").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy disk-store --name=DISKSTORE").statusIsSuccess();
  }

  @Test
  public void invalidDiskStore() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --type=REPLICATE_PERSISTENT --disk-store=unknown")
        .statusIsError()
        .containsOutput("Specify valid disk-store. Unknown Disk Store : \"unknown\"");
  }

  @Test
  public void entryIdleTimeWithoutStatisticsEnabled() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --type=REPLICATE --entry-idle-time-expiration=1").statusIsError()
        .containsOutput("Statistics must be enabled for expiration");
  }

  @Test
  public void invalidCompressor() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --type=REPLICATE --compressor=java.lang.String").statusIsError()
        .containsOutput("java.lang.String cannot be cast to ")
        .containsOutput("org.apache.geode.compression.Compressor");
  }

  @Test
  public void validateDefaultExpirationAttributes() {
    gfsh.executeAndAssertThat("create region --name=/A --type=REPLICATE").statusIsSuccess();

    Region region = server.getCache().getRegion("/A");
    RegionAttributes attributes = region.getAttributes();
    ExpirationAttributes entryIdle = attributes.getEntryIdleTimeout();
    ExpirationAttributes entryTTL = attributes.getEntryTimeToLive();
    ExpirationAttributes regionIdle = attributes.getRegionIdleTimeout();
    ExpirationAttributes regionTTL = attributes.getRegionTimeToLive();

    assertThat(entryIdle).isNotNull();
    assertThat(entryIdle.getTimeout()).isEqualTo(0);
    assertThat(entryIdle.getAction()).isEqualTo(ExpirationAction.INVALIDATE);
    assertThat(entryTTL).isNotNull();
    assertThat(entryTTL.getTimeout()).isEqualTo(0);
    assertThat(entryTTL.getAction()).isEqualTo(ExpirationAction.INVALIDATE);
    assertThat(regionIdle).isNotNull();
    assertThat(regionIdle.getTimeout()).isEqualTo(0);
    assertThat(regionIdle.getAction()).isEqualTo(ExpirationAction.INVALIDATE);
    assertThat(regionTTL).isNotNull();
    assertThat(regionTTL.getTimeout()).isEqualTo(0);
    assertThat(regionTTL.getAction()).isEqualTo(ExpirationAction.INVALIDATE);

    gfsh.executeAndAssertThat("destroy region --name=/A").statusIsSuccess();
  }

  @Test
  public void validateNonDefaultBinaryOptions() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=REPLICATE"
        + " --enable-async-conflation" + " --enable-cloning" + " --enable-concurrency-checks=false"
        + " --enable-multicast" + " --enable-statistics" + " --enable-subscription-conflation"
        + " --enable-synchronous-disk=false").statusIsSuccess();

    Region foo = server.getCache().getRegion("/FOO");

    assertThat(foo.getAttributes().getEnableAsyncConflation()).isTrue();
    assertThat(foo.getAttributes().getCloningEnabled()).isTrue();
    assertThat(foo.getAttributes().getConcurrencyChecksEnabled()).isFalse();
    assertThat(foo.getAttributes().getMulticastEnabled()).isTrue();
    assertThat(foo.getAttributes().getStatisticsEnabled()).isTrue();
    assertThat(foo.getAttributes().getEnableSubscriptionConflation()).isTrue();
    assertThat(foo.getAttributes().isDiskSynchronous()).isFalse();

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void validateExpirationOptions() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=REPLICATE" + " --enable-statistics"
        + " --entry-idle-time-expiration=3" + " --entry-idle-time-expiration-action=DESTROY"
        + " --entry-time-to-live-expiration=5" + " --entry-time-to-live-expiration-action=DESTROY"
        + " --region-idle-time-expiration=7" + " --region-idle-time-expiration-action=DESTROY"
        + " --region-time-to-live-expiration=11"
        + " --region-time-to-live-expiration-action=DESTROY").statusIsSuccess();

    Region<?, ?> foo = server.getCache().getRegion("/FOO");

    assertThat(foo.getAttributes().getStatisticsEnabled()).isTrue();
    assertThat(foo.getAttributes().getEntryIdleTimeout().getTimeout()).isEqualTo(3);
    assertThat(foo.getAttributes().getEntryIdleTimeout().getAction())
        .isEqualTo(ExpirationAction.DESTROY);
    assertThat(foo.getAttributes().getEntryTimeToLive().getTimeout()).isEqualTo(5);
    assertThat(foo.getAttributes().getEntryTimeToLive().getAction())
        .isEqualTo(ExpirationAction.DESTROY);
    assertThat(foo.getAttributes().getRegionIdleTimeout().getTimeout()).isEqualTo(7);
    assertThat(foo.getAttributes().getRegionIdleTimeout().getAction())
        .isEqualTo(ExpirationAction.DESTROY);
    assertThat(foo.getAttributes().getRegionTimeToLive().getTimeout()).isEqualTo(11);
    assertThat(foo.getAttributes().getRegionTimeToLive().getAction())
        .isEqualTo(ExpirationAction.DESTROY);

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @SuppressWarnings("deprecation")
  @Test
  public void validatePartitionRegionOptions() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=PARTITION_REDUNDANT"
        + " --local-max-memory=1001" + " --recovery-delay=7" + " --redundant-copies=1"
        + " --startup-recovery-delay=5" + " --total-max-memory=2001" + " --total-num-buckets=11"
        + " --partition-resolver=" + TestPartitionResolver.class.getName()).statusIsSuccess();

    Region<?, ?> foo = server.getCache().getRegion("/FOO");

    assertThat(foo.getAttributes().getPartitionAttributes().getLocalMaxMemory()).isEqualTo(1001);
    assertThat(foo.getAttributes().getPartitionAttributes().getRecoveryDelay()).isEqualTo(7);
    assertThat(foo.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(1);
    assertThat(foo.getAttributes().getPartitionAttributes().getStartupRecoveryDelay()).isEqualTo(5);
    assertThat(foo.getAttributes().getPartitionAttributes().getTotalMaxMemory()).isEqualTo(2001);
    assertThat(foo.getAttributes().getPartitionAttributes().getTotalNumBuckets()).isEqualTo(11);
    assertThat(
        foo.getAttributes().getPartitionAttributes().getPartitionResolver().getClass().getName())
            .isEqualTo(TestPartitionResolver.class.getName());

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void validateCallbackOptions() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --type=PARTITION_REDUNDANT --cache-listener="
            + TestCacheListener.class.getName() + " --cache-loader="
            + TestCacheLoader.class.getName() + " --cache-writer=" + TestCacheWriter.class.getName()
            + " --compressor=" + TestCompressor.class.getName())
        .statusIsSuccess();

    Region<?, ?> foo = server.getCache().getRegion("/FOO");

    assertThat(Arrays.stream(foo.getAttributes().getCacheListeners())
        .map(c -> c.getClass().getName()).collect(Collectors.toSet()))
            .contains(TestCacheListener.class.getName());
    assertThat(foo.getAttributes().getCacheLoader().getClass().getName())
        .isEqualTo(TestCacheLoader.class.getName());
    assertThat(foo.getAttributes().getCacheWriter().getClass().getName())
        .isEqualTo(TestCacheWriter.class.getName());
    assertThat(foo.getAttributes().getCompressor().getClass().getName())
        .isEqualTo(TestCompressor.class.getName());

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void validateConstraints() {
    gfsh.executeAndAssertThat("create region --name=/FOO --type=REPLICATE" + " --key-constraint="
        + TestConstraint.class.getName() + " --value-constraint=" + TestConstraint.class.getName())
        .statusIsSuccess();

    Region<?, ?> foo = server.getCache().getRegion("/FOO");

    assertThat(foo.getAttributes().getKeyConstraint().getName())
        .isEqualTo(TestConstraint.class.getName());
    assertThat(foo.getAttributes().getValueConstraint().getName())
        .isEqualTo(TestConstraint.class.getName());

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void validateEntryIdleTimeExpiration() {
    gfsh.executeAndAssertThat(
        "create region --name=/FOO --type=REPLICATE --entry-idle-time-expiration=7 --enable-statistics")
        .statusIsSuccess();
    Region<?, ?> template = server.getCache().getRegion("/FOO");
    assertThat(template.getAttributes().getEntryIdleTimeout().getTimeout()).isEqualTo(7);

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void validateTemplateRegionAttributesForReplicate() {
    gfsh.executeAndAssertThat("create region --name=/TEMPLATE --type=REPLICATE"
        + " --enable-async-conflation" + " --enable-cloning" + " --enable-concurrency-checks=false"
        + " --enable-multicast" + " --enable-statistics" + " --enable-subscription-conflation"
        + " --enable-synchronous-disk=false" + " --entry-idle-time-expiration=3"
        + " --entry-idle-time-expiration-action=DESTROY" + " --entry-time-to-live-expiration=5"
        + " --entry-time-to-live-expiration-action=DESTROY" + " --region-idle-time-expiration=7"
        + " --region-idle-time-expiration-action=DESTROY" + " --region-time-to-live-expiration=11"
        + " --region-time-to-live-expiration-action=DESTROY" + " --cache-listener="
        + TestCacheListener.class.getName() + " --cache-loader=" + TestCacheLoader.class.getName()
        + " --cache-writer=" + TestCacheWriter.class.getName() + " --compressor="
        + TestCompressor.class.getName() + " --key-constraint=" + TestConstraint.class.getName()
        + " --value-constraint=" + TestConstraint.class.getName()).statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=/COPY --template-region=/TEMPLATE")
        .statusIsSuccess();

    Region<?, ?> copy = server.getCache().getRegion("/COPY");

    assertThat(copy.getAttributes().getStatisticsEnabled()).isTrue();
    assertThat(copy.getAttributes().getEnableAsyncConflation()).isTrue();
    assertThat(copy.getAttributes().getCloningEnabled()).isTrue();
    assertThat(copy.getAttributes().getConcurrencyChecksEnabled()).isFalse();
    assertThat(copy.getAttributes().getMulticastEnabled()).isTrue();
    assertThat(copy.getAttributes().getStatisticsEnabled()).isTrue();
    assertThat(copy.getAttributes().getEnableSubscriptionConflation()).isTrue();
    assertThat(copy.getAttributes().isDiskSynchronous()).isFalse();
    assertThat(copy.getAttributes().getEntryIdleTimeout().getTimeout()).isEqualTo(3);
    assertThat(copy.getAttributes().getEntryIdleTimeout().getAction())
        .isEqualTo(ExpirationAction.DESTROY);
    assertThat(copy.getAttributes().getEntryTimeToLive().getTimeout()).isEqualTo(5);
    assertThat(copy.getAttributes().getEntryTimeToLive().getAction())
        .isEqualTo(ExpirationAction.DESTROY);
    assertThat(copy.getAttributes().getRegionIdleTimeout().getTimeout()).isEqualTo(7);
    assertThat(copy.getAttributes().getRegionIdleTimeout().getAction())
        .isEqualTo(ExpirationAction.DESTROY);
    assertThat(copy.getAttributes().getRegionTimeToLive().getTimeout()).isEqualTo(11);
    assertThat(copy.getAttributes().getRegionTimeToLive().getAction())
        .isEqualTo(ExpirationAction.DESTROY);
    assertThat(Arrays.stream(copy.getAttributes().getCacheListeners())
        .map(c -> c.getClass().getName()).collect(Collectors.toSet()))
            .contains(TestCacheListener.class.getName());
    assertThat(copy.getAttributes().getCacheLoader().getClass().getName())
        .isEqualTo(TestCacheLoader.class.getName());
    assertThat(copy.getAttributes().getCacheWriter().getClass().getName())
        .isEqualTo(TestCacheWriter.class.getName());
    assertThat(copy.getAttributes().getCompressor().getClass().getName())
        .isEqualTo(TestCompressor.class.getName());
    assertThat(copy.getAttributes().getKeyConstraint().getName())
        .isEqualTo(TestConstraint.class.getName());
    assertThat(copy.getAttributes().getValueConstraint().getName())
        .isEqualTo(TestConstraint.class.getName());

    gfsh.executeAndAssertThat("destroy region --name=/COPY").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy region --name=/TEMPLATE").statusIsSuccess();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void validateTemplateRegionAttributesForPartitionRedundant() {
    gfsh.executeAndAssertThat("create region --name=/TEMPLATE --type=PARTITION_REDUNDANT"
        + " --enable-async-conflation" + " --enable-cloning" + " --enable-concurrency-checks=false"
        + " --enable-multicast" + " --enable-statistics" + " --enable-subscription-conflation"
        + " --enable-synchronous-disk=false" + " --cache-listener="
        + TestCacheListener.class.getName() + " --cache-loader=" + TestCacheLoader.class.getName()
        + " --cache-writer=" + TestCacheWriter.class.getName() + " --compressor="
        + TestCompressor.class.getName() + " --key-constraint=" + TestConstraint.class.getName()
        + " --value-constraint=" + TestConstraint.class.getName() + " --local-max-memory=1001"
        + " --recovery-delay=7" + " --redundant-copies=1" + " --startup-recovery-delay=5"
        + " --total-max-memory=2001" + " --total-num-buckets=11" + " --partition-resolver="
        + TestPartitionResolver.class.getName()).statusIsSuccess();

    gfsh.executeAndAssertThat("create region --name=/COPY --template-region=/TEMPLATE")
        .statusIsSuccess();

    Region<?, ?> copy = server.getCache().getRegion("/COPY");

    assertThat(copy.getAttributes().getStatisticsEnabled()).isTrue();
    assertThat(copy.getAttributes().getEnableAsyncConflation()).isTrue();
    assertThat(copy.getAttributes().getCloningEnabled()).isTrue();
    assertThat(copy.getAttributes().getConcurrencyChecksEnabled()).isFalse();
    assertThat(copy.getAttributes().getMulticastEnabled()).isTrue();
    assertThat(copy.getAttributes().getStatisticsEnabled()).isTrue();
    assertThat(copy.getAttributes().getEnableSubscriptionConflation()).isTrue();
    assertThat(copy.getAttributes().isDiskSynchronous()).isFalse();
    assertThat(Arrays.stream(copy.getAttributes().getCacheListeners())
        .map(c -> c.getClass().getName()).collect(Collectors.toSet()))
            .contains(TestCacheListener.class.getName());
    assertThat(copy.getAttributes().getCacheLoader().getClass().getName())
        .isEqualTo(TestCacheLoader.class.getName());
    assertThat(copy.getAttributes().getCacheWriter().getClass().getName())
        .isEqualTo(TestCacheWriter.class.getName());
    assertThat(copy.getAttributes().getCompressor().getClass().getName())
        .isEqualTo(TestCompressor.class.getName());
    assertThat(copy.getAttributes().getKeyConstraint().getName())
        .isEqualTo(TestConstraint.class.getName());
    assertThat(copy.getAttributes().getValueConstraint().getName())
        .isEqualTo(TestConstraint.class.getName());
    assertThat(copy.getAttributes().getPartitionAttributes().getLocalMaxMemory()).isEqualTo(1001);
    assertThat(copy.getAttributes().getPartitionAttributes().getRecoveryDelay()).isEqualTo(7);
    assertThat(copy.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(1);
    assertThat(copy.getAttributes().getPartitionAttributes().getStartupRecoveryDelay())
        .isEqualTo(5);
    assertThat(copy.getAttributes().getPartitionAttributes().getTotalMaxMemory()).isEqualTo(2001);
    assertThat(copy.getAttributes().getPartitionAttributes().getTotalNumBuckets()).isEqualTo(11);
    assertThat(
        copy.getAttributes().getPartitionAttributes().getPartitionResolver().getClass().getName())
            .isEqualTo(TestPartitionResolver.class.getName());

    gfsh.executeAndAssertThat("destroy region --name=/COPY").statusIsSuccess();
    gfsh.executeAndAssertThat("destroy region --name=/TEMPLATE").statusIsSuccess();
  }

  @Test
  public void cannotSetRegionExpirationForPartitionedRegion() {
    gfsh.executeAndAssertThat(
        "create region --enable-statistics=true --name=/FOO --type=PARTITION " +
            "--region-idle-time-expiration=1 --region-time-to-live-expiration=1")
        .statusIsError()
        .containsOutput(
            "ExpirationAction INVALIDATE or LOCAL_INVALIDATE for region is not supported for Partitioned Region");
  }

  @Test
  public void testEvictionAttributesForLRUHeap() {
    gfsh.executeAndAssertThat(
        "create region --name=FOO --type=REPLICATE --eviction-action=local-destroy")
        .statusIsSuccess();

    Region<?, ?> foo = server.getCache().getRegion("/FOO");
    assertThat(foo.getAttributes().getEvictionAttributes().getAction())
        .isEqualTo(EvictionAction.LOCAL_DESTROY);
    assertThat(foo.getAttributes().getEvictionAttributes().getAlgorithm())
        .isEqualTo(EvictionAlgorithm.LRU_HEAP);

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void testEvictionAttributesForLRUHeapWithObjectSizer() {
    gfsh.executeAndAssertThat(
        "create region --name=FOO --type=REPLICATE --eviction-action=local-destroy --eviction-object-sizer="
            + TestObjectSizer.class.getName())
        .statusIsSuccess();

    Region<?, ?> foo = server.getCache().getRegion("/FOO");
    assertThat(foo.getAttributes().getEvictionAttributes().getAction())
        .isEqualTo(EvictionAction.LOCAL_DESTROY);
    assertThat(foo.getAttributes().getEvictionAttributes().getAlgorithm())
        .isEqualTo(EvictionAlgorithm.LRU_HEAP);
    assertThat(foo.getAttributes().getEvictionAttributes().getObjectSizer().getClass().getName())
        .isEqualTo(TestObjectSizer.class.getName());

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void testEvictionAttributesForLRUEntry() {
    gfsh.executeAndAssertThat(
        "create region --name=FOO --type=REPLICATE --eviction-entry-count=1001 " +
            "--eviction-action=overflow-to-disk")
        .statusIsSuccess();

    Region<?, ?> foo = server.getCache().getRegion("/FOO");
    assertThat(foo.getAttributes().getEvictionAttributes().getAction())
        .isEqualTo(EvictionAction.OVERFLOW_TO_DISK);
    assertThat(foo.getAttributes().getEvictionAttributes().getAlgorithm())
        .isEqualTo(EvictionAlgorithm.LRU_ENTRY);
    assertThat(foo.getAttributes().getEvictionAttributes().getMaximum()).isEqualTo(1001);

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void testEvictionAttributesForLRUMemory() {
    gfsh.executeAndAssertThat(
        "create region --name=FOO --type=REPLICATE --eviction-max-memory=1001 " +
            "--eviction-action=overflow-to-disk")
        .statusIsSuccess();

    Region<?, ?> foo = server.getCache().getRegion("/FOO");
    assertThat(foo.getAttributes().getEvictionAttributes().getAction())
        .isEqualTo(EvictionAction.OVERFLOW_TO_DISK);
    assertThat(foo.getAttributes().getEvictionAttributes().getAlgorithm())
        .isEqualTo(EvictionAlgorithm.LRU_MEMORY);
    assertThat(foo.getAttributes().getEvictionAttributes().getMaximum()).isEqualTo(1001);

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void testEvictionAttributesForObjectSizer() {
    gfsh.executeAndAssertThat(
        "create region --name=FOO --type=REPLICATE --eviction-max-memory=1001 " +
            "--eviction-action=overflow-to-disk --eviction-object-sizer="
            + TestObjectSizer.class.getName())
        .statusIsSuccess();

    Region<?, ?> foo = server.getCache().getRegion("/FOO");
    EvictionAttributes attrs = foo.getAttributes().getEvictionAttributes();
    assertThat(attrs.getAction()).isEqualTo(EvictionAction.OVERFLOW_TO_DISK);
    assertThat(attrs.getAlgorithm()).isEqualTo(EvictionAlgorithm.LRU_MEMORY);
    assertThat(attrs.getMaximum()).isEqualTo(1001);
    assertThat(attrs.getObjectSizer().getClass().getName())
        .isEqualTo(TestObjectSizer.class.getName());

    gfsh.executeAndAssertThat("destroy region --name=/FOO").statusIsSuccess();
  }

  @Test
  public void testEvictionAttributesForNonDeclarableObjectSizer() {
    gfsh.executeAndAssertThat(
        "create region --name=FOO --type=REPLICATE --eviction-max-memory=1001 " +
            "--eviction-action=overflow-to-disk --eviction-object-sizer="
            + TestObjectSizerNotDeclarable.class.getName())
        .statusIsError().containsOutput(
            "eviction-object-sizer must implement both ObjectSizer and Declarable interfaces");
  }

  @Test
  public void createRegionWithCacheListenerWithInvalidJson() {
    gfsh.executeAndAssertThat("create region --name=FOO --type=REPLICATE --cache-listener=abc{abc}")
        .statusIsError().containsOutput("Invalid JSON: {abc}");
  }

  @Test
  public void createSubRegion() {
    gfsh.executeAndAssertThat("create region --name=region --type=REPLICATE").statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=region/region1 --type=REPLICATE")
        .statusIsSuccess();

    Region<?, ?> subregion = server.getCache().getRegion("/region/region1");
    assertThat(subregion).isNotNull();

    gfsh.executeAndAssertThat("destroy region --name=/region").statusIsSuccess();
  }
}
