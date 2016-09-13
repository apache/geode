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
package org.apache.geode.internal.offheap;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OffHeapStoredObjectSliceJUnitTest extends OffHeapStoredObjectJUnitTest {

  @Test
  public void sliceShouldHaveAValidDataSize() {
    int position = 1;
    int end = 2;

    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    OffHeapStoredObjectSlice slice = (OffHeapStoredObjectSlice) chunk.slice(position, end);

    assertNotNull(slice);
    assertEquals(OffHeapStoredObjectSlice.class, slice.getClass());

    assertEquals(end - position, slice.getDataSize());
  }

  @Test
  public void sliceShouldHaveAValidBaseDataAddress() {
    int position = 1;
    int end = 2;

    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    OffHeapStoredObjectSlice slice = (OffHeapStoredObjectSlice) chunk.slice(position, end);

    assertNotNull(slice);
    assertEquals(OffHeapStoredObjectSlice.class, slice.getClass());

    assertEquals(chunk.getBaseDataAddress() + position, slice.getBaseDataAddress());
  }

  @Test
  public void sliceShouldHaveAValidBaseOffset() {
    int position = 1;
    int end = 2;

    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    OffHeapStoredObjectSlice slice = (OffHeapStoredObjectSlice) chunk.slice(position, end);

    assertNotNull(slice);
    assertEquals(OffHeapStoredObjectSlice.class, slice.getClass());

    assertEquals(chunk.getBaseDataOffset() + position, slice.getBaseDataOffset());
  }
}
