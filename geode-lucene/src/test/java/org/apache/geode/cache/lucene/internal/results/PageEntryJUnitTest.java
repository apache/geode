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

package org.apache.geode.cache.lucene.internal.results;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.PreferBytesCachedDeserializable;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class PageEntryJUnitTest {

  @Test
  public void getValueShouldReturnObjectValue() {
    PageEntry entry = new PageEntry("key", "value");
    assertEquals("value", entry.getValue());
  }

  @Test
  public void getValueShouldReturnObjectFromCachedDeserializableValue() {
    CachedDeserializable cachedDeserializable = new PreferBytesCachedDeserializable("value");
    PageEntry entry = new PageEntry("key", cachedDeserializable);
    assertEquals("value", entry.getValue());
  }

  @Test
  public void getValueShouldReturnBytesFromBytesValue() {
    byte[] bytes = new byte[10];
    Arrays.fill(bytes, (byte) 5);
    PageEntry entry = new PageEntry("key", bytes);
    assertArrayEquals(bytes, (byte[]) entry.getValue());
  }

  @Test
  public void serializationWithObjectValueShouldNotModifyValue()
      throws IOException, ClassNotFoundException {
    PageEntry entry = new PageEntry("key", "value");
    assertEquals("value", copy(entry).getValue());
  }

  @Test
  public void serializationReturnObjectFromCachedDeserializableValue()
      throws IOException, ClassNotFoundException {
    CachedDeserializable cachedDeserializable = new PreferBytesCachedDeserializable("value");
    PageEntry entry = new PageEntry("key", cachedDeserializable);
    assertEquals("value", copy(entry).getValue());
  }

  @Test
  public void serializationShouldReturnsBytesFromByteValue()
      throws IOException, ClassNotFoundException {
    byte[] bytes = new byte[10];
    Arrays.fill(bytes, (byte) 5);
    PageEntry entry = new PageEntry("key", bytes);
    assertArrayEquals(bytes, (byte[]) copy(entry).getValue());
  }

  public PageEntry copy(PageEntry entry) throws IOException, ClassNotFoundException {
    HeapDataOutputStream out = new HeapDataOutputStream((Version) null);
    entry.toData(out);
    final byte[] bytes = out.toByteArray();
    ByteArrayDataInput in = new ByteArrayDataInput();
    in.initialize(bytes, (short) 0);
    PageEntry newEntry = new PageEntry();
    newEntry.fromData(in);
    return newEntry;
  }

}
