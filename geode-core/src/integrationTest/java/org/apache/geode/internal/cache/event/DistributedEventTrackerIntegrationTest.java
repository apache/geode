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
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.internal.Assert.assertTrue;
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@Category({RegionsTest.class})
public class DistributedEventTrackerIntegrationTest {

  @Parameterized.Parameters(name = "possibleDuplicate={0}")
  public static Collection booleans() {
    return Arrays.asList(true, false);
  }

  @Parameterized.Parameter
  public boolean possibleDuplicate;

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withNoCacheServer().withLogFile();

  @Before
  public void setUp() throws Exception {
    server.startServer();
  }

  @Test
  public void testHasSeenEventReplicatedRegion() throws IOException {
    // Create the replicated region
    DistributedRegion region = (DistributedRegion) server.createRegion(REPLICATE,
        testName.getMethodName().substring(0, testName.getMethodName().indexOf('[')));

    // Invoke hasSeenEvent and verify results
    invokeHasSeenEventAndVerifyResults(region);
  }

  @Test
  public void testHasSeenEventPartitionedRegion() throws IOException {
    // Create the partitioned region
    PartitionedRegion region = (PartitionedRegion) server.createRegion(PARTITION,
        testName.getMethodName().substring(0, testName.getMethodName().indexOf('[')));

    // Assign buckets so that the BucketRegions and their EventTrackers are created
    PartitionRegionHelper.assignBucketsToPartitions(region);

    // Get a BucketRegion
    BucketRegion br = region.getBucketRegion(0);

    // Invoke hasSeenEvent and verify results
    invokeHasSeenEventAndVerifyResults(br);
  }

  private void invokeHasSeenEventAndVerifyResults(DistributedRegion region)
      throws IOException {
    // Record an event with a high sequence number
    recordHighSequenceNumberEvent(region);

    // Create an event with a lower sequence number and possibleDuplicate set appropriately
    EntryEventImpl event =
        createEvent(region, new EventID(new byte[0], 1l, 0l));
    event.setPossibleDuplicate(possibleDuplicate);

    // Invoke hasSeenEvent
    boolean hasSeenEvent = region.getEventTracker().hasSeenEvent(event);

    // Assert hasSeenEvent is true
    assertTrue(hasSeenEvent);

    // Verify the log does or does not contain the message depending on possibleDuplicate
    File logFile = ((InternalDistributedSystem) server.getCache().getDistributedSystem())
        .getConfig().getLogFile();
    Optional<String> logLine = Files.lines(Paths.get(logFile.getAbsolutePath()))
        .filter(line -> line.contains(EVENT_HAS_PREVIOUSLY_BEEN_SEEN_PREFIX))
        .findFirst();
    assertThat(logLine.isPresent()).isEqualTo(!possibleDuplicate);

    // Verify the statistic is incremented
    assertThat(region.getCachePerfStats().getPreviouslySeenEvents()).isEqualTo(1);
  }

  private void recordHighSequenceNumberEvent(DistributedRegion region) {
    EventID eventId = new EventID(new byte[0], 1l, 1000l);
    EntryEventImpl event = createEvent(region, eventId);
    region.getEventTracker().recordEvent(event);
  }

  private EntryEventImpl createEvent(LocalRegion region, EventID eventId) {
    EntryEventImpl event = EntryEventImpl.create(region, Operation.CREATE, 0, 0, null, true,
        region.getCache().getMyId(), false, eventId);

    // Set the client context so that the event won't be ignored by the tracker
    event.setContext(mock(ClientProxyMembershipID.class));
    return event;
  }
}
