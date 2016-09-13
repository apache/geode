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

import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LocalDataSetTest {

  @Test
  public void verifyThatIsEmptyIsTrueWhenEntryCountReturnsZero() {
    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.isEmpty()).thenReturn(false);
    when(pr.entryCount(any())).thenReturn(0);
    LocalDataSet lds = new LocalDataSet(pr, Collections.emptySet());
    assertTrue(lds.isEmpty());
  }
  
  @Test
  public void verifyThatIsEmptyIsFalseWhenEntryCountReturnsNonZero() {
    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.isEmpty()).thenReturn(true);
    when(pr.entryCount(any())).thenReturn(1);
    LocalDataSet lds = new LocalDataSet(pr, Collections.emptySet());
    assertFalse(lds.isEmpty());
  }
  
  @Test
  public void verifyThatGetCallbackArgIsCorrectlyPassedToGetHashKey() {
    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(pr.getTotalNumberOfBuckets()).thenReturn(33);
    LocalDataSet lds = new LocalDataSet(pr, Collections.emptySet());
    LocalDataSet spy = spy(lds);
    Object key = "key";
    Object callbackArg = "callbackArg";
    
    spy.get(key, callbackArg);
    
    verify(spy).getHashKey(Operation.CONTAINS_KEY, key, null, callbackArg);
  }
}
