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

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(IntegrationTest.class)
public class CreateRegionCommandIntegrationTest {

  private static String CREATE_REGION = "create region --type=REPLICATE ";

  public static class TestCacheListener extends CacheListenerAdapter {
  }

  public static class TestConstraint {
  }

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withRegion(RegionShortcut.REPLICATE, "REPLICATED");

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshShellConnectionRule.PortType.jmxManager);
  }

  @Test
  public void parentRegionDoesNotExist() throws Exception {
    gfsh.executeAndVerifyCommandError(CREATE_REGION + "--name=/A/B",
        "Parent region for \"/A/B\" doesnt exist");
  }

  @Test
  public void groupDoesNotExist() throws Exception {
    gfsh.executeAndVerifyCommandError(CREATE_REGION + "--name=/FOO --groups=unknown",
        "Group\\(s\\) .* are invalid");
  }

  @Test
  public void templateRegionDoesNotExist() throws Exception {
    gfsh.executeAndVerifyCommandError("create region --name=/FOO --template-region=/BAR",
        "Specify a valid region path for template-region");
  }

  @Test
  public void conflictingPartitionAttributesWithTemplate() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --template-region=REPLICATED --redundant-copies=2",
        "Parameter\\(s\\) \"\\[redundant-copies\\]\" can be used only for creating a Partitioned Region");
  }

  @Test
  public void conflictingPartitionAttributesWithShortCut() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --redundant-copies=2",
        "Parameter\\(s\\) \"\\[redundant-copies\\]\" can be used only for creating a Partitioned Region");
  }

  @Test
  public void colocatedWithRegionDoesNotExist() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --type=PARTITION --name=/FOO --colocated-with=/BAR",
        "Specify a valid region path for colocated-with");
  }

  @Test
  public void colocatedWithRegionIsNotPartitioned() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --type=PARTITION --name=/FOO --colocated-with=/REPLICATED",
        "colocated-with \"/REPLICATED\" is not a Partitioned Region");
  }

  @Test
  public void negativeLocalMaxMemory() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --type=PARTITION --name=/FOO --local-max-memory=-1",
        "PartitionAttributes localMaxMemory must not be negative");
  }

  @Test
  public void zeroLocalMaxMemoryIsOK() throws Exception {
    gfsh.executeAndVerifyCommand("create region --type=PARTITION --name=/FOO --local-max-memory=0",
        "Region \"/FOO\" created");
    gfsh.executeAndVerifyCommand("destroy region --name=/FOO");
  }

  @Test
  public void negativeTotalMaxMemory() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --type=PARTITION --name=/FOO --total-max-memory=-1",
        "Total size of partition region must be > 0");
  }

  @Test
  public void zeroTotalMaxMemory() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --type=PARTITION --name=/FOO --total-max-memory=0",
        "Total size of partition region must be > 0");
  }

  @Test
  public void redundantCopies() throws Exception {
    gfsh.executeAndVerifyCommand("create region --name=/FOO --type=PARTITION --redundant-copies=2",
        "Region \"/FOO\" created");
    gfsh.executeAndVerifyCommand("destroy region --name=/FOO");
  }

  @Test
  public void tooManyredundantCopies() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=PARTITION --redundant-copies=4",
        "redundant-copies \"4\" is not valid");
  }

  @Test
  public void keyConstraint() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --key-constraint=abc-def",
        "Specify a valid class name for key-constraint");
  }

  @Test
  public void valueConstraint() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --value-constraint=abc-def",
        "Specify a valid class name for value-constraint");
  }

  @Test
  public void invalidCacheListener() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --cache-listener=abc-def",
        "Specify a valid class name for cache-listener");
  }

  @Test
  public void invalidCacheLoader() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --cache-loader=abc-def",
        "Specify a valid class name for cache-loader");
  }

  @Test
  public void invalidCacheWriter() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --cache-writer=abc-def",
        "Specify a valid class name for cache-writer");
  }

  @Test
  public void invalidGatewaySenders() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --gateway-sender-id=unknown",
        "There are no GatewaySenders defined currently in the system");
  }

  // TODO: Write test for invalid gateway name (gateways already need to exist).

  @Test
  public void invalidConcurrencyLevel() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --template-region=/REPLICATED --concurrency-level=-1",
        "Specify positive integer value for concurrency-level");
  }

  @Test
  public void nonPersistentRegionWithdiskStore() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --disk-store=unknown",
        "Only regions with persistence or overflow to disk can specify DiskStore");
  }

  @Test
  public void nonPersistentTemplateWithdiskStore() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --template-region=/REPLICATED --disk-store=unknown",
        "Only regions with persistence or overflow to disk can specify DiskStore",
        "template-region region \"/REPLICATED\" is not persistent");
  }


  @Test
  public void invalidDiskStore() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE_PERSISTENT --disk-store=unknown",
        "Specify valid disk-store. Unknown Disk Store : \"unknown\"");
  }

  @Test
  public void entryIdleTimeWithoutStatisticsEnabled() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --entry-idle-time-expiration=1",
        "Statistics must be enabled for expiration");
  }

  @Test
  public void invalidCompressor() throws Exception {
    gfsh.executeAndVerifyCommandError(
        "create region --name=/FOO --type=REPLICATE --compressor=java.lang.String",
        "java.lang.String cannot be cast to org.apache.geode.compression.Compressor");
  }

  @Test
  public void validateDefaultExpirationAttributes() throws Exception {
    gfsh.executeAndVerifyCommand("create region --name=/A --type=REPLICATE");

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

    gfsh.executeAndVerifyCommand("destroy region --name=/A");
  }

  @Test
  public void validateNonDefaultBinaryOptions() throws Exception {
    gfsh.executeAndVerifyCommand("create region --name=/FOO --type=REPLICATE"
        + " --enable-async-conflation" + " --enable-cloning" + " --enable-concurrency-checks=false"
        + " --enable-multicast" + " --enable-statistics" + " --enable-subscription-conflation"
        + " --enable-synchronous-disk=false");

    Region foo = server.getCache().getRegion("/FOO");

    assertThat(foo.getAttributes().getEnableAsyncConflation()).isTrue();
    assertThat(foo.getAttributes().getCloningEnabled()).isTrue();
    assertThat(foo.getAttributes().getConcurrencyChecksEnabled()).isFalse();
    assertThat(foo.getAttributes().getMulticastEnabled()).isTrue();
    assertThat(foo.getAttributes().getStatisticsEnabled()).isTrue();
    assertThat(foo.getAttributes().getEnableSubscriptionConflation()).isTrue();
    assertThat(foo.getAttributes().isDiskSynchronous()).isFalse();

    gfsh.executeAndVerifyCommand("destroy region --name=/FOO");
  }

  @Test
  public void validateExpirationOptions() throws Exception {
    gfsh.executeAndVerifyCommand("create region --name=/FOO --type=REPLICATE"
        + " --enable-statistics" + " --entry-idle-time-expiration=3"
        + " --entry-idle-time-expiration-action=DESTROY" + " --entry-time-to-live-expiration=5"
        + " --entry-time-to-live-expiration-action=DESTROY" + " --region-idle-time-expiration=7"
        + " --region-idle-time-expiration-action=DESTROY" + " --region-time-to-live-expiration=11"
        + " --region-time-to-live-expiration-action=DESTROY");

    Region foo = server.getCache().getRegion("/FOO");

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

    gfsh.executeAndVerifyCommand("destroy region --name=/FOO");
  }

  @Test
  public void validatePartitionRegionOptions() throws Exception {
    gfsh.executeAndVerifyCommand("create region --name=/FOO --type=PARTITION_REDUNDANT"
        + " --local-max-memory=1001" + " --recovery-delay=7" + " --redundant-copies=1"
        + " --startup-recovery-delay=5" + " --total-max-memory=2001" + " --total-num-buckets=11"
        + " --partition-resolver=" + TestPartitionResolver.class.getName());

    Region foo = server.getCache().getRegion("/FOO");

    assertThat(foo.getAttributes().getPartitionAttributes().getLocalMaxMemory()).isEqualTo(1001);
    assertThat(foo.getAttributes().getPartitionAttributes().getRecoveryDelay()).isEqualTo(7);
    assertThat(foo.getAttributes().getPartitionAttributes().getRedundantCopies()).isEqualTo(1);
    assertThat(foo.getAttributes().getPartitionAttributes().getStartupRecoveryDelay()).isEqualTo(5);
    assertThat(foo.getAttributes().getPartitionAttributes().getTotalMaxMemory()).isEqualTo(2001);
    assertThat(foo.getAttributes().getPartitionAttributes().getTotalNumBuckets()).isEqualTo(11);
    assertThat(
        foo.getAttributes().getPartitionAttributes().getPartitionResolver().getClass().getName())
            .isEqualTo(TestPartitionResolver.class.getName());

    gfsh.executeAndVerifyCommand("destroy region --name=/FOO");
  }

  @Test
  public void validateCallbackOptions() throws Exception {
    gfsh.executeAndVerifyCommand("create region --name=/FOO --type=PARTITION_REDUNDANT"
        + " --cache-listener=" + TestCacheListener.class.getName() + " --cache-loader="
        + TestCacheLoader.class.getName() + " --cache-writer=" + TestCacheWriter.class.getName()
        + " --compressor=" + TestCompressor.class.getName());

    Region foo = server.getCache().getRegion("/FOO");

    assertThat(Arrays.stream(foo.getAttributes().getCacheListeners())
        .map(c -> c.getClass().getName()).collect(Collectors.toSet()))
            .contains(TestCacheListener.class.getName());
    assertThat(foo.getAttributes().getCacheLoader().getClass().getName())
        .isEqualTo(TestCacheLoader.class.getName());
    assertThat(foo.getAttributes().getCacheWriter().getClass().getName())
        .isEqualTo(TestCacheWriter.class.getName());
    assertThat(foo.getAttributes().getCompressor().getClass().getName())
        .isEqualTo(TestCompressor.class.getName());

    gfsh.executeAndVerifyCommand("destroy region --name=/FOO");
  }

  @Test
  public void validateConstraints() throws Exception {
    gfsh.executeAndVerifyCommand("create region --name=/FOO --type=REPLICATE" + " --key-constraint="
        + TestConstraint.class.getName() + " --value-constraint=" + TestConstraint.class.getName());

    Region foo = server.getCache().getRegion("/FOO");

    assertThat(foo.getAttributes().getKeyConstraint().getName())
        .isEqualTo(TestConstraint.class.getName());
    assertThat(foo.getAttributes().getValueConstraint().getName())
        .isEqualTo(TestConstraint.class.getName());

    gfsh.executeAndVerifyCommand("destroy region --name=/FOO");
  }

  @Test
  public void validateEntryIdleTimeExpiration() throws Exception {
    gfsh.executeAndVerifyCommand(
        "create region --name=/FOO --type=REPLICATE --entry-idle-time-expiration=7 --enable-statistics");
    Region template = server.getCache().getRegion("/FOO");
    assertThat(template.getAttributes().getEntryIdleTimeout().getTimeout()).isEqualTo(7);

    gfsh.executeAndVerifyCommand("destroy region --name=/FOO");
  }

  @Test
  public void validateTemplateRegionAttributesForReplicate() throws Exception {
    gfsh.executeAndVerifyCommand("create region --name=/TEMPLATE --type=REPLICATE"
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
        + " --value-constraint=" + TestConstraint.class.getName());

    gfsh.executeAndVerifyCommand("create region --name=/COPY --template-region=/TEMPLATE");

    Region copy = server.getCache().getRegion("/COPY");

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


    gfsh.executeAndVerifyCommand("destroy region --name=/COPY");
    gfsh.executeAndVerifyCommand("destroy region --name=/TEMPLATE");
  }

  @Test
  public void validateTemplateRegionAttributesForPartitionRedundant() throws Exception {
    gfsh.executeAndVerifyCommand("create region --name=/TEMPLATE --type=PARTITION_REDUNDANT"
        + " --enable-async-conflation" + " --enable-cloning" + " --enable-concurrency-checks=false"
        + " --enable-multicast" + " --enable-statistics" + " --enable-subscription-conflation"
        + " --enable-synchronous-disk=false" + " --cache-listener="
        + TestCacheListener.class.getName() + " --cache-loader=" + TestCacheLoader.class.getName()
        + " --cache-writer=" + TestCacheWriter.class.getName() + " --compressor="
        + TestCompressor.class.getName() + " --key-constraint=" + TestConstraint.class.getName()
        + " --value-constraint=" + TestConstraint.class.getName() + " --local-max-memory=1001"
        + " --recovery-delay=7" + " --redundant-copies=1" + " --startup-recovery-delay=5"
        + " --total-max-memory=2001" + " --total-num-buckets=11" + " --partition-resolver="
        + TestPartitionResolver.class.getName());

    gfsh.executeAndVerifyCommand("create region --name=/COPY --template-region=/TEMPLATE");

    Region copy = server.getCache().getRegion("/COPY");

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

    gfsh.executeAndVerifyCommand("destroy region --name=/COPY");
    gfsh.executeAndVerifyCommand("destroy region --name=/TEMPLATE");
  }
}
