package com.gemstone.gemfire.cache.lucene.internal.repository.serializer;

import static org.junit.Assert.assertEquals;

import org.apache.lucene.document.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.lucene.internal.repository.serializer.PdxLuceneSerializer;
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
    
    PdxInstance i = Mockito.mock(PdxInstance.class);
    
    Mockito.when(i.hasField("s")).thenReturn(true);
    Mockito.when(i.hasField("i")).thenReturn(true);
    Mockito.when(i.getField("s")).thenReturn("a");
    Mockito.when(i.getField("i")).thenReturn(5);
    
    
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
    
    PdxInstance i = Mockito.mock(PdxInstance.class);
    
    Mockito.when(i.hasField("s")).thenReturn(true);
    Mockito.when(i.hasField("i")).thenReturn(true);
    Mockito.when(i.hasField("o")).thenReturn(true);
    Mockito.when(i.hasField("o2")).thenReturn(true);
    Mockito.when(i.getField("s")).thenReturn("a");
    Mockito.when(i.getField("i")).thenReturn(5);
    Mockito.when(i.getField("o")).thenReturn(new Object());
    Mockito.when(i.getField("o2")).thenReturn(new Object());
    
    
    Document doc = new Document();
    mapper.toDocument(i, doc);
    
    assertEquals(2, doc.getFields().size());
    assertEquals("a", doc.getField("s").stringValue());
    assertEquals(5, doc.getField("i").numericValue());
  }
}
