package com.gemstone.gemfire.cache.lucene.internal.xml;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneIndexXmlGeneratorJUnitTest {
  
  /**
   * Test of generating and reading cache configuration back in.
   * @throws SAXException 
   */
  @Test
  public void generateWithFields() throws SAXException {
    LuceneIndex index = Mockito.mock(LuceneIndex.class);
    Mockito.when(index.getName()).thenReturn("index");
    Mockito.when(index.getFieldNames()).thenReturn(new String[] {"a", "b"});
    
    LuceneIndexXmlGenerator generator = new LuceneIndexXmlGenerator(index);
    CacheXmlGenerator cacheXmlGenerator = Mockito.mock(CacheXmlGenerator.class);
    ContentHandler handler = Mockito.mock(ContentHandler.class);
    Mockito.when(cacheXmlGenerator.getContentHandler()).thenReturn(handler);
    generator.generate(cacheXmlGenerator);
    
    ArgumentCaptor<Attributes> captor = new ArgumentCaptor<>();
    Mockito.verify(handler).startElement(eq(""), eq("index"), eq("lucene:index"), captor.capture());
    Attributes value = captor.getValue();
    assertEquals("index", value.getValue(LuceneXmlConstants.NAME));
    assertEquals("a,b", value.getValue(LuceneXmlConstants.FIELDS));
  }

}
