/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal.repository.serializer;

import static org.apache.geode.cache.lucene.internal.repository.serializer.SerializerTestHelper.invokeSerializer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.lucene.document.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.LuceneTest;

/**
 * Unit test of the PdxFieldMapperJUnitTest. Tests that all field types are mapped correctly.
 */
@Category({LuceneTest.class})
public class PdxFieldMapperJUnitTest {

  @Test
  public void testWriteFields() {
    String[] fields = new String[] {"s", "i"};
    PdxLuceneSerializer mapper = new PdxLuceneSerializer();

    PdxInstance pdxInstance = mock(PdxInstance.class);

    when(pdxInstance.hasField("s")).thenReturn(true);
    when(pdxInstance.hasField("i")).thenReturn(true);
    when(pdxInstance.getField("s")).thenReturn("a");
    when(pdxInstance.getField("i")).thenReturn(5);

    Document doc = invokeSerializer(mapper, pdxInstance, fields);

    assertEquals(2, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertEquals(5, doc.getField("i").numericValue());
  }

  @Test
  public void testIgnoreMissing() {
    String[] fields = new String[] {"s", "i", "s2", "o"};
    PdxLuceneSerializer mapper = new PdxLuceneSerializer();

    PdxInstance pdxInstance = mock(PdxInstance.class);

    when(pdxInstance.hasField("s")).thenReturn(true);
    when(pdxInstance.hasField("i")).thenReturn(true);
    when(pdxInstance.hasField("o")).thenReturn(true);
    when(pdxInstance.hasField("o2")).thenReturn(true);
    when(pdxInstance.getField("s")).thenReturn("a");
    when(pdxInstance.getField("i")).thenReturn(5);
    when(pdxInstance.getField("o")).thenReturn(new Object());
    when(pdxInstance.getField("o2")).thenReturn(new Object());

    Document doc = invokeSerializer(mapper, pdxInstance, fields);

    assertEquals(2, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertEquals(5, doc.getField("i").numericValue());
  }

  @Test
  public void testNullField() {
    String[] fields = new String[] {"s", "i"};
    PdxLuceneSerializer mapper = new PdxLuceneSerializer();

    PdxInstance pdxInstance = mock(PdxInstance.class);

    when(pdxInstance.hasField("s")).thenReturn(true);
    when(pdxInstance.hasField("i")).thenReturn(true);
    when(pdxInstance.getField("s")).thenReturn("a");
    when(pdxInstance.getField("i")).thenReturn(null);

    Document doc = invokeSerializer(mapper, pdxInstance, fields);

    assertEquals(1, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertNull(doc.getField("i"));
  }
}
