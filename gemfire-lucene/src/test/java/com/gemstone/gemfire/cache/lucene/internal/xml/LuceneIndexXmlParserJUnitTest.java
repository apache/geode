package com.gemstone.gemfire.cache.lucene.internal.xml;

import static org.junit.Assert.*;

import java.util.Stack;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGeneratorUtils;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class LuceneIndexXmlParserJUnitTest {
  
  @Test
  public void generateWithFields() throws SAXException {
    LuceneXmlParser parser = new LuceneXmlParser();
    AttributesImpl attrs = new AttributesImpl();
    CacheCreation cache = new CacheCreation();
    RegionCreation rc = new RegionCreation(cache, "region");
    Stack<Object> stack = new Stack<Object>();
    stack.push(cache);
    stack.push(rc);
    parser.setStack(stack);
    XmlGeneratorUtils.addAttribute(attrs, LuceneXmlConstants.NAME, "index");
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null, attrs);
    
    AttributesImpl field1 = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(field1, LuceneXmlConstants.NAME, "field1");
    AttributesImpl field2 = new AttributesImpl();
    XmlGeneratorUtils.addAttribute(field2, LuceneXmlConstants.NAME, "field2");
    
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null, field1);
    parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null);
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null, field2);
    parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.FIELD, null);
    
    
    parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null);
    assertEquals(rc, stack.peek());
    
    LuceneIndexCreation index = (LuceneIndexCreation) rc.getExtensionPoint().getExtensions().iterator().next();
    assertEquals("index", index.getName());
    assertArrayEquals(new String[] {"field1", "field2"}, index.getFieldNames());
  }
  

}
