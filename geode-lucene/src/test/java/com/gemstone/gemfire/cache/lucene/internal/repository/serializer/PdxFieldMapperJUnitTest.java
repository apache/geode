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
package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.lucene.document.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit test of the PdxFieldMapperJUnitTest. Tests that 
 * all field types are mapped correctly. 
 */
@Category(UnitTest.class)
public class PdxFieldMapperJUnitTest {

  @Test
  public void testWriteFields() {
    String[] fields = new String[] {"s", "i"};
    PdxLuceneSerializer mapper = new PdxLuceneSerializer(fields);
    
    PdxInstance i = mock(PdxInstance.class);
    
    when(i.hasField("s")).thenReturn(true);
    when(i.hasField("i")).thenReturn(true);
    when(i.getField("s")).thenReturn("a");
    when(i.getField("i")).thenReturn(5);
    
    Document doc = new Document();
    mapper.toDocument(i, doc);
    
    assertEquals(2, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertEquals(5, doc.getField("i").numericValue());
  }
  
  @Test
  public void testIgnoreMissing() {
    String[] fields = new String[] {"s", "i", "s2", "o"};
    PdxLuceneSerializer mapper = new PdxLuceneSerializer(fields);
    
    PdxInstance i = mock(PdxInstance.class);
    
    when(i.hasField("s")).thenReturn(true);
    when(i.hasField("i")).thenReturn(true);
    when(i.hasField("o")).thenReturn(true);
    when(i.hasField("o2")).thenReturn(true);
    when(i.getField("s")).thenReturn("a");
    when(i.getField("i")).thenReturn(5);
    when(i.getField("o")).thenReturn(new Object());
    when(i.getField("o2")).thenReturn(new Object());
    
    Document doc = new Document();
    mapper.toDocument(i, doc);
    
    assertEquals(2, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertEquals(5, doc.getField("i").numericValue());
  }
  
  @Test
  public void testNullField() {
    String[] fields = new String[] {"s", "i"};
    PdxLuceneSerializer mapper = new PdxLuceneSerializer(fields);
    
    PdxInstance i = mock(PdxInstance.class);
    
    when(i.hasField("s")).thenReturn(true);
    when(i.hasField("i")).thenReturn(true);
    when(i.getField("s")).thenReturn("a");
    when(i.getField("i")).thenReturn(null);
    
    Document doc = new Document();
    mapper.toDocument(i, doc);
    
    assertEquals(1, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertNull(doc.getField("i"));
  }
}
