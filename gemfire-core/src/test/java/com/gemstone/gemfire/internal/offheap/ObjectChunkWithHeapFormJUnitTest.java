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

package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ObjectChunkWithHeapFormJUnitTest extends ObjectChunkJUnitTest {

  @Test
  public void getRawBytesShouldReturnCachedHeapForm() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());

    byte[] valueInBytes = getValueAsByteArray();
    ObjectChunkWithHeapForm heapForm = new ObjectChunkWithHeapForm(chunk, valueInBytes);

    assertNotNull(heapForm);

    assertSame(valueInBytes, heapForm.getRawBytes());
  }

  @Test
  public void getChunkWithoutHeapFormShouldReturnGemFireChunk() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

    byte[] valueInBytes = getValueAsByteArray();
    ObjectChunkWithHeapForm heapForm = new ObjectChunkWithHeapForm(chunk, valueInBytes);

    ObjectChunk chunkWithOutHeapForm = heapForm.getChunkWithoutHeapForm();

    assertNotNull(chunkWithOutHeapForm);
    assertEquals(ObjectChunk.class, chunkWithOutHeapForm.getClass());

    assertEquals(chunk, heapForm.getChunkWithoutHeapForm());

    assertEquals(chunk.getMemoryAddress(), chunkWithOutHeapForm.getMemoryAddress());
    assertArrayEquals(chunk.getRawBytes(), chunkWithOutHeapForm.getRawBytes());
    assertNotSame(valueInBytes, chunkWithOutHeapForm.getRawBytes());
  }
}
