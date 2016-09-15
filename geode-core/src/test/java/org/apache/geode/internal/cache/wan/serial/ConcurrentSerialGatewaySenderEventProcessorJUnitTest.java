/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.cache.wan.serial;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConcurrentSerialGatewaySenderEventProcessorJUnitTest {

  private ConcurrentSerialGatewaySenderEventProcessor processor;
  private RegionQueue queue;

  @Before
  public void setUp() throws Exception {
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    processor = new ConcurrentSerialGatewaySenderEventProcessor(sender);
    queue = mock(RegionQueue.class);
    when(queue.size()).thenReturn(3);
  }

  @Test
  public void eventQueueSizeReturnsSizeOfQueues() {
    processor.getQueues().add(queue);
    assertEquals(3,processor.eventQueueSize());
  }

}
