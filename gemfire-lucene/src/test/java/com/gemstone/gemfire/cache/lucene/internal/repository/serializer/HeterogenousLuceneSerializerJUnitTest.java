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

import static org.junit.Assert.assertEquals;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.HeterogenousLuceneSerializer;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit test of the ObjectToDocumentMapper. 
 */
@Category(UnitTest.class)
public class HeterogenousLuceneSerializerJUnitTest {

  /**
   * Test that the mapper can handle a mix of different
   * object types.
   */
  @Test
  public void testHetrogenousObjects() {
    String[] fields = new String[] {"s", "i", "l", "d", "f", "s2", "missing"};
    HeterogenousLuceneSerializer mapper = new HeterogenousLuceneSerializer(fields);
    
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
    
    PdxInstance i = Mockito.mock(PdxInstance.class);
    
    Mockito.when(i.hasField("s")).thenReturn(true);
    Mockito.when(i.hasField("i")).thenReturn(true);
    Mockito.when(i.getField("s")).thenReturn("a");
    Mockito.when(i.getField("i")).thenReturn(5);
    
    Document doc3 = new Document();
    mapper.toDocument(i, doc3);
    
    assertEquals(2, doc3.getFields().size());
    assertEquals("a", doc3.getField("s").stringValue());
    assertEquals(5, doc3.getField("i").numericValue());
  }

}
