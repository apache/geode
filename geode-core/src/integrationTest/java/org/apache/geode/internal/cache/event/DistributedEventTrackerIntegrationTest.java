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
package org.apache.geode.internal.cache.event;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.cache.event.DistributedEventTracker.EVENT_HAS_PREVIOUSLY_BEEN_SEEN_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({RegionsTest.class})
@RunWith(GeodeParamsRunner.class)
public class DistributedEventTrackerIntegrationTest {

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private Cache cache;

  private File logFile;

  @Before
  public void setUp() throws Exception {
    logFile = temporaryFolder.newFile(testName.getMethodName() + ".log");
    Properties properties = new Properties();
    properties.setProperty(LOCATORS, "");
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOG_FILE, logFile.getAbsolutePath());
    cache = new CacheFactory(properties).create();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  @Test
  @Parameters(method = "getPossibleDuplicates")
  public void testHasSeenEventReplicatedRegion(boolean possibleDuplicate) throws IOException {
    // Create the replicated region
    DistributedRegion region =
        (DistributedRegion) cache.createRegionFactory(REPLICATE).create(testName.getMethodName());

    // Invoke hasSeenEvent and verify results
    invokeHasSeenEventAndVerifyResults(region, possibleDuplicate, !possibleDuplicate);
  }

  @Test
  @Parameters(method = "getPossibleDuplicates")
  public void testHasSeenEventPartitionedRegion(boolean possibleDuplicate) throws IOException {
    // Create the partitioned region
    PartitionedRegion region =
        (PartitionedRegion) cache.createRegionFactory(PARTITION).create(testName.getMethodName());

    // Assign buckets so that the BucketRegions and their EventTrackers are created
    PartitionRegionHelper.assignBucketsToPartitions(region);

    // Get a BucketRegion
    BucketRegion br = region.getBucketRegion(0);

    // Invoke hasSeenEvent and verify results
    invokeHasSeenEventAndVerifyResults(br, possibleDuplicate, !possibleDuplicate);
  }

  @Test
  public void testHasSeenEventNullEvent() throws IOException {
    // Create the region
    DistributedRegion region =
        (DistributedRegion) cache.createRegionFactory(REPLICATE).create(testName.getMethodName());

    // Record an event with a high sequence number
    byte[] memberId = new byte[0];
    long threadId = 1L;
    recordHighSequenceNumberEvent(region, memberId, threadId);

    // Invoke hasSeenEvent with event id (null event)
    boolean hasSeenEvent =
        region.getEventTracker().hasSeenEvent(new EventID(memberId, threadId, 0L));

    // Verify results
    verifyResults(region, hasSeenEvent, true);
  }

  @Test
  public void testHasSeenEventPartitionedRegionLowRedundancy() throws IOException {
    // Create the partitioned region
    PartitionedRegion region = (PartitionedRegion) cache.createRegionFactory(PARTITION_REDUNDANT)
        .create(testName.getMethodName());

    // Assign buckets so that the BucketRegions and their EventTrackers are created
    PartitionRegionHelper.assignBucketsToPartitions(region);

    // Get a BucketRegion
    BucketRegion br = region.getBucketRegion(0);

    // Invoke hasSeenEvent and verify results
    invokeHasSeenEventAndVerifyResults(br, false, false);
  }

  @Test
  public void testHasSeenEventPartitionedRegionInRecovery() throws IOException {
    // Create the partitioned region
    PartitionedRegion region =
        (PartitionedRegion) cache.createRegionFactory(PARTITION).create(testName.getMethodName());

    // Assign buckets so that the BucketRegions and their EventTrackers are created
    PartitionRegionHelper.assignBucketsToPartitions(region);

    // Get a BucketRegion
    BucketRegion br = region.getBucketRegion(0);

    // Start recovery in progress
    region.getPrStats().startRecovery();

    // Invoke hasSeenEvent and verify results
    invokeHasSeenEventAndVerifyResults(br, false, false);
  }

  private void invokeHasSeenEventAndVerifyResults(DistributedRegion region,
      boolean possibleDuplicate, boolean logMessageIsPresent) throws IOException {
    // Record an event with a high sequence number
    byte[] memberId = new byte[0];
    long threadId = 1L;
    recordHighSequenceNumberEvent(region, memberId, threadId);

    // Create an event with a lower sequence number and possibleDuplicate set appropriately
    EntryEventImpl event2 =
        createEvent(region, new EventID(memberId, threadId, 0L), possibleDuplicate);

    // Invoke hasSeenEvent
    boolean hasSeenEvent = region.getEventTracker().hasSeenEvent(event2);

    // Verify results
    verifyResults(region, hasSeenEvent, logMessageIsPresent);
  }

  private void recordHighSequenceNumberEvent(DistributedRegion region, byte[] memberId,
      long threadId) {
    // Create event with high sequence number
    EntryEventImpl event1 = createEvent(region, new EventID(memberId, threadId, 1000L), false);

    // Record the event
    region.getEventTracker().recordEvent(event1);
  }

  private void verifyResults(DistributedRegion region, boolean hasSeenEvent,
      boolean logMessageIsPresent) throws IOException {
    // Assert hasSeenEvent is true
    assertThat(hasSeenEvent).isTrue();

    // Verify the log does or does not contain the message depending on possibleDuplicate
    Optional<String> logLine = Files.lines(Paths.get(logFile.getAbsolutePath()))
        .filter(line -> line.contains(EVENT_HAS_PREVIOUSLY_BEEN_SEEN_PREFIX))
        .findFirst();
    assertThat(logLine.isPresent()).isEqualTo(logMessageIsPresent);

    // Verify the statistic is incremented
    assertThat(region.getCachePerfStats().getPreviouslySeenEvents()).isEqualTo(1);
  }

  private EntryEventImpl createEvent(LocalRegion region, EventID eventId,
      boolean possibleDuplicate) {
    EntryEventImpl event = EntryEventImpl.create(region, Operation.CREATE, 0, 0, null, true,
        region.getCache().getMyId(), false, eventId);
    event.setPossibleDuplicate(possibleDuplicate);

    // Set the client context so that the event won't be ignored by the tracker
    event.setContext(mock(ClientProxyMembershipID.class));
    return event;
  }

  @SuppressWarnings("unused")
  private Collection<Boolean> getPossibleDuplicates() {
    return Arrays.asList(true, false);
  }
}
