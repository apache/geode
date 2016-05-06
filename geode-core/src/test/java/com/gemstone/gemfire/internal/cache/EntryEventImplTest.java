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
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class EntryEventImplTest {

  private String expectedRegionName;
  private String key;
  private String value;
  private KeyInfo keyInfo;

  @Before
  public void setUp() throws Exception {
    expectedRegionName = "ExpectedFullRegionPathName";
    key = "key1";
    value = "value1";
    keyInfo = new KeyInfo(key, value, null);
  }

  @Test
  public void verifyToStringOutputHasRegionName() {
    // mock a region object
    LocalRegion region = mock(LocalRegion.class);
    doReturn(expectedRegionName).when(region).getFullPath();
    doReturn(keyInfo).when(region).getKeyInfo(any(), any(), any());

    // create entryevent for the region
    EntryEventImpl e = createEntryEvent(region);
    
    // The name of the region should be in the toString text
    String toStringValue = e.toString();
    assertTrue("String " + expectedRegionName + " was not in toString text: " + toStringValue, toStringValue.indexOf(expectedRegionName) > 0);

    // verify that toString called getFullPath method of region object
    verify(region, times(1)).getFullPath();
  }

  private EntryEventImpl createEntryEvent(LocalRegion l) {
    // create a dummy event id
    byte[] memId = { 1,2,3 };
    EventID eventId = new EventID(memId, 11, 12, 13);

    // create an event
    EntryEventImpl event = EntryEventImpl.create(l, Operation.CREATE, key,
        value, null,  false /* origin remote */, null,
        false /* generateCallbacks */,
        eventId);
    // avoid calling invokeCallbacks
    event.callbacksInvoked(true);

    return event;
  }
}