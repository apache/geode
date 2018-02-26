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
package org.apache.geode.internal.cache.wan.parallel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ParallelGatewaySenderEventProcessorJUnitTest {

  private GemFireCacheImpl cache;
  private AbstractGatewaySender sender;

  @Before
  public void setUpGemFire() {
    createCache();
    createGatewaySender();
  }

  private void createCache() {
    // Mock cache
    this.cache = Fakes.cache();
  }

  private void createGatewaySender() {
    // Mock gateway sender
    this.sender = ParallelGatewaySenderHelper.createGatewaySender(this.cache);
    when(this.sender.isBatchConflationEnabled()).thenReturn(true);
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));
  }

  @Test
  public void validateBatchConflationWithBatchContainingDuplicateConflatableEvents()
      throws Exception {
    // Create a ParallelGatewaySenderEventProcessor
    AbstractGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(this.sender);

    // Create a batch of conflatable events with duplicates
    List<GatewaySenderEventImpl> originalEvents = new ArrayList<>();
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn("/dataStoreRegion");
    Object lastUpdateValue = "Object_13964_5";
    long lastUpdateSequenceId = 104, lastUpdateShadowKey = 28161;
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13964", "Object_13964_1", 100, 27709));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", "Object_13964_2", 101, 27822));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", "Object_13964_3", 102, 27935));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", "Object_13964_4", 103, 28048));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", lastUpdateValue, lastUpdateSequenceId, lastUpdateShadowKey));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.DESTROY,
        "Object_13964", null, 102, 28274));

    // Conflate the batch of events
    List<GatewaySenderEventImpl> conflatedEvents = processor.conflate(originalEvents);

    // Verify:
    // - the batch contains 3 events after conflation
    // - they are CREATE, UPDATE, and DESTROY
    // - the UPDATE event is the correct one
    assertThat(conflatedEvents.size()).isEqualTo(3);
    GatewaySenderEventImpl gsei1 = conflatedEvents.get(0);
    assertThat(gsei1.getOperation()).isEqualTo(Operation.CREATE);
    GatewaySenderEventImpl gsei2 = conflatedEvents.get(1);
    assertThat(gsei2.getOperation()).isEqualTo(Operation.UPDATE);
    GatewaySenderEventImpl gsei3 = conflatedEvents.get(2);
    assertThat(gsei3.getOperation()).isEqualTo(Operation.DESTROY);
    assertThat(gsei2.getDeserializedValue()).isEqualTo(lastUpdateValue);
    assertThat(gsei2.getEventId().getSequenceID()).isEqualTo(lastUpdateSequenceId);
    assertThat(gsei2.getShadowKey()).isEqualTo(lastUpdateShadowKey);
  }

  @Test
  public void validateBatchConflationWithBatchContainingDuplicateNonConflatableEvents()
      throws Exception {
    // This is a test for GEODE-4704.
    // A batch containing events like below is conflated. The conflation code should not affect this
    // batch.
    // SenderEventImpl[id=EventIDid=57bytes;threadID=0x10018|112;sequenceID=100;bucketId=24];operation=CREATE;region=/dataStoreRegion;key=Object_13964;shadowKey=27709]
    // SenderEventImpl[id=EventIDid=57bytes;threadID=0x10018|112;sequenceID=101;bucketId=24];operation=CREATE;region=/dataStoreRegion;key=Object_14024;shadowKey=27822]
    // SenderEventImpl[id=EventIDid=57bytes;threadID=0x10018|112;sequenceID=102;bucketId=24];operation=DESTROY;region=/dataStoreRegion;key=Object_13964;shadowKey=27935]
    // SenderEventImpl[id=EventIDid=57bytes;threadID=0x10018|112;sequenceID=104;bucketId=24];operation=CREATE;region=/dataStoreRegion;key=Object_14024;shadowKey=28161]

    // Create a ParallelGatewaySenderEventProcessor
    AbstractGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(this.sender);

    // Create a batch of non-conflatable events with one duplicate
    List<GatewaySenderEventImpl> originalEvents = new ArrayList<>();
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn("/dataStoreRegion");
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13964", "Object_13964", 100, 27709));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_14024", "Object_13964", 101, 27822));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.DESTROY,
        "Object_13964", null, 102, 27935));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_14024", "Object_14024", 104, 28161));

    // Conflate the batch of events
    List<GatewaySenderEventImpl> conflatedEvents = processor.conflate(originalEvents);

    // Assert no events were conflated incorrectly
    assertThat(originalEvents).isEqualTo(conflatedEvents);
  }
}
