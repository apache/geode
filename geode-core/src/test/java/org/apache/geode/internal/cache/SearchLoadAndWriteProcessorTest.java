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
package org.apache.geode.internal.cache;

import static org.mockito.Mockito.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SearchLoadAndWriteProcessorTest {

  /**
   * This test verifies the fix for GEODE-1199.
   * It verifies that when doNetWrite is called with an event
   * that has a StoredObject value that it will have "release"
   * called on it.
   */
  @Test
  public void verifyThatOffHeapReleaseIsCalledAfterNetWrite() {
    // setup
    SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getOffHeap()).thenReturn(true);
    when(lr.getScope()).thenReturn(Scope.DISTRIBUTED_ACK);
    Object key = "key";
    StoredObject value = mock(StoredObject.class);
    when(value.hasRefCount()).thenReturn(true);
    when(value.retain()).thenReturn(true);
    Object cbArg = null;
    KeyInfo keyInfo = new KeyInfo(key, value, cbArg);
    when(lr.getKeyInfo(any(), any(), any())).thenReturn(keyInfo);
    processor.region = lr;
    EntryEventImpl event = EntryEventImpl.create(lr, Operation.REPLACE, key, value, cbArg, false, null);
    
    try {
      // the test
      processor.doNetWrite(event, null, null, 0);
      
      // verification
      verify(value, times(2)).retain();
      verify(value, times(1)).release();
      
    } finally {
      processor.release();
    }
  }

}
