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
    XmlGeneratorUtils.addAttribute(attrs, LuceneXmlConstants.FIELDS, "a,b,c");
    parser.startElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null, attrs);
    parser.endElement(LuceneXmlConstants.NAMESPACE, LuceneXmlConstants.INDEX, null);
    
    LuceneIndexCreation index = (LuceneIndexCreation) rc.getExtensionPoint().getExtensions().iterator().next();
    assertEquals("index", index.getName());
    assertArrayEquals(new String[] {"a", "b", "c"}, index.getFieldNames());
  }
  

}
