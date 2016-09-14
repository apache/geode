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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;

public class ProducerTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private Producer producer;
  private ClientCache clientCache = mock(ClientCache.class);
  private Region region = mock(Region.class);
  private Set keys = mock(Set.class);

  @Before
  public void setup() throws Exception {
    when(region.getName()).thenReturn(Producer.REGION_NAME);
    when(region.keySetOnServer()).thenReturn(keys);
    when(clientCache.getRegion(any())).thenReturn(region);
  }

  @Test
  public void populateRegionShouldReturnCorrectNumberOfEntries() throws Exception {
    producer = new Producer(clientCache);
    producer.setRegion(region);

    producer.populateRegion();
    verify(region, times(producer.NUM_ENTRIES)).put(any(), any());
  }

  @Test
  public void populateWhenRegionDoesNotExistShouldThrowNullPointer() throws Exception {
    producer = new Producer(clientCache);
    expectedException.expect(NullPointerException.class);
    producer.populateRegion();
  }

  @After
  public void tearDown() {

  }
}