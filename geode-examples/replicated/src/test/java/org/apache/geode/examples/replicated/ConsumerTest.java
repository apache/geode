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

package org.apache.geode.examples.replicated;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.NoAvailableLocatorsException;

public class ConsumerTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private Consumer consumer;
  private ClientCache clientCache = mock(ClientCache.class);
  private Region region = mock(Region.class);
  private Set keys = mock(Set.class);

  @Before
  public void setup() {
    when(region.getName()).thenReturn(Consumer.REGION_NAME);
    when(keys.size()).thenReturn(Consumer.NUM_ENTRIES);
    when(region.keySetOnServer()).thenReturn(keys);
    when(clientCache.getRegion(any())).thenReturn(region);
    consumer = new Consumer(clientCache);
    consumer.setRegion(region);
  }

  @Test
  public void numberOfEntriesOnServerShouldMatchConsumerEntries() throws Exception {
    assertEquals(consumer.NUM_ENTRIES, consumer.countEntriesOnServer());
  }

  @Test
  public void numberOfEntriesShouldBeGreaterThanZero() throws Exception {
    assertTrue(consumer.NUM_ENTRIES > 0);
  }

  @Test
  public void countingEntriesWithoutConnectionShouldThrowNoAvailableLocatorsException() throws Exception {
    consumer = new Consumer();
    expectedException.expect(NoAvailableLocatorsException.class);
    assertEquals(consumer.NUM_ENTRIES, consumer.countEntriesOnServer());
  }

}