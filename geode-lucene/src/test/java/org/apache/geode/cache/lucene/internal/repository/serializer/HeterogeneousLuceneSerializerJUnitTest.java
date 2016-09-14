/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.geode.cache.lucene.internal.repository.serializer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.lucene.document.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit test of the ObjectToDocumentMapper. 
 */
@Category(UnitTest.class)
public class HeterogeneousLuceneSerializerJUnitTest {

  /**
   * Test that the mapper can handle a mix of different
   * object types.
   */
  @Test
  public void testHeterogeneousObjects() {
    String[] fields = new String[] {"s", "i", "l", "d", "f", "s2", "missing"};
    HeterogeneousLuceneSerializer mapper = new HeterogeneousLuceneSerializer(fields);
    
    Type1 t1 = new Type1("a", 1, 2L, 3.0, 4.0f);
    
    Document doc1 = new Document();
    mapper.toDocument(t1, doc1);
    
    assertEquals(5, doc1.getFields().size());
    assertEquals("a", doc1.getField("s").stringValue());
    assertEquals(1, doc1.getField("i").numericValue());
    assertEquals(2L, doc1.getField("l").numericValue());
    assertEquals(3.0, doc1.getField("d").numericValue());
    assertEquals(4.0f, doc1.getField("f").numericValue());
    
    Type2 t2 = new Type2("a", 1, 2L, 3.0, 4.0f, "b");
    
    Document doc2 = new Document();
    mapper.toDocument(t2, doc2);
    
    assertEquals(6, doc2.getFields().size());
    assertEquals("a", doc2.getField("s").stringValue());
    assertEquals("b", doc2.getField("s2").stringValue());
    assertEquals(1, doc2.getField("i").numericValue());
    assertEquals(2L, doc2.getField("l").numericValue());
    assertEquals(3.0, doc2.getField("d").numericValue());
    assertEquals(4.0f, doc2.getField("f").numericValue());
    
    PdxInstance i = mock(PdxInstance.class);
    
    when(i.hasField("s")).thenReturn(true);
    when(i.hasField("i")).thenReturn(true);
    when(i.getField("s")).thenReturn("a");
    when(i.getField("i")).thenReturn(5);
    
    Document doc3 = new Document();
    mapper.toDocument(i, doc3);
    
    assertEquals(2, doc3.getFields().size());
    assertEquals("a", doc3.getField("s").stringValue());
    assertEquals(5, doc3.getField("i").numericValue());
  }

  @Test
  public void shouldIndexPrimitiveStringIfRequested() {
    HeterogeneousLuceneSerializer mapper = new HeterogeneousLuceneSerializer(new String[] {LuceneService.REGION_VALUE_FIELD});
    Document doc = new Document();
    mapper.toDocument("sample value", doc);
    assertEquals(1, doc.getFields().size());
    assertEquals("sample value", doc.getField(LuceneService.REGION_VALUE_FIELD).stringValue());
  }

  @Test
  public void shouldIndexPrimitiveNumberIfRequested() {
    HeterogeneousLuceneSerializer mapper = new HeterogeneousLuceneSerializer(new String[] {LuceneService.REGION_VALUE_FIELD});
    Document doc = new Document();
    mapper.toDocument(53, doc);

    assertEquals(1, doc.getFields().size());

    assertEquals(53, doc.getField(LuceneService.REGION_VALUE_FIELD).numericValue());
  }

}
