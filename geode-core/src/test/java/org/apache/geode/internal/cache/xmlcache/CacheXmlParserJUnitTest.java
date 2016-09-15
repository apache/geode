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
package org.apache.geode.internal.cache.xmlcache;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Locale;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Test cases for {@link CacheXmlParser}.
 *
 * @since GemFire 8.1
 */
@Category(UnitTest.class)
public class CacheXmlParserJUnitTest {

  private static final String NAMESPACE_URI = "urn:java:org.apache.geode.internal.cache.xmlcache.MockXmlParser";

  /**
   * Test {@link CacheXmlParser#getDelegate(String)}.
   * 
   * Asserts that a delegate is found and that the stack and logWriter are setup
   * correctly.
   * 
   * Asserts that delegate is cached between calls and that the same instance is
   * returned.
   * 
   * Asserts that null is returned when no {@link XmlParser} is registered for
   * namespace.
   * 
   * @since GemFire 8.1
   */
  @Test
  public void testGetDelegate() {
    final TestCacheXmlParser cacheXmlParser = new TestCacheXmlParser();

    assertTrue("delegates should be empty.", cacheXmlParser.getDelegates().isEmpty());

    final MockXmlParser delegate = (MockXmlParser) cacheXmlParser.getDelegate(NAMESPACE_URI);
    
    assertNotNull("Delegate should be found in classpath.", delegate);

    assertSame("Should have same stack as cacheXmlParser.", cacheXmlParser.stack, delegate.stack);
    assertSame("Should have same stack as cacheXmlParser.", cacheXmlParser.documentLocator , delegate.documentLocator);

    assertEquals("Should be exactly 1 delegate.", 1, cacheXmlParser.getDelegates().size());
    assertNotNull("There should be an entry in delegates cache.", cacheXmlParser.getDelegates().get(NAMESPACE_URI));
    assertSame("Cached delegate should match the one from get.", delegate, cacheXmlParser.getDelegates().get(NAMESPACE_URI));

    final MockXmlParser delegate2 = (MockXmlParser) cacheXmlParser.getDelegate(NAMESPACE_URI);
    assertSame("Delegate should be the same between gets.", delegate, delegate2);
    assertEquals("Should still be exactly 1 delegate.", 1, cacheXmlParser.getDelegates().size());

    assertNull(cacheXmlParser.getDelegate("--nothing-should-use-this-namespace--"));
  }

  /**
   * Test that {@link CacheXmlParser} falls back to DTD parsing when locale language is not English.
   * 
   * @since Geode 1.0
   */
  @Test
  public void testDTDFallbackWithNonEnglishLocal() {
    CacheXmlParser.parse(this.getClass().getResourceAsStream("CacheXmlParserJUnitTest.testDTDFallbackWithNonEnglishLocal.cache.xml"));

    final Locale previousLocale = Locale.getDefault();
    try {
      Locale.setDefault(Locale.JAPAN);
      CacheXmlParser.parse(this.getClass().getResourceAsStream("CacheXmlParserJUnitTest.testDTDFallbackWithNonEnglishLocal.cache.xml"));
    } finally {
      Locale.setDefault(previousLocale);
    }
  }

  /**
   * Get access to {@link CacheXmlParser} protected methods and fields.
   * 
   * @since GemFire 8.1
   */
  private static class TestCacheXmlParser extends CacheXmlParser {

    static Field delegatesField;
    static Method getDelegateMethod;

    static {
      try {
        delegatesField = CacheXmlParser.class.getDeclaredField("delegates");
        delegatesField.setAccessible(true);

        getDelegateMethod = CacheXmlParser.class.getDeclaredMethod("getDelegate", String.class);
        getDelegateMethod.setAccessible(true);
      } catch (NoSuchFieldException | SecurityException | NoSuchMethodException e) {
        throw new IllegalStateException(e);
      }
    }

    /**
     * @return {@link CacheXmlParser} private delegates field.
     * @since GemFire 8.1
     */
    @SuppressWarnings("unchecked")
    public HashMap<String, XmlParser> getDelegates() {
      try {
        return (HashMap<String, XmlParser>) delegatesField.get(this);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    }

    /**
     * Access to {@link CacheXmlParser} getDelegate(String) method.
     * 
     * @since GemFire 8.1
     */
    public XmlParser getDelegate(final String namespaceUri) {
      try {
        return (XmlParser) getDelegateMethod.invoke(this, namespaceUri);
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    }
  }
  
  public static class MockXmlParser extends AbstractXmlParser {

    @Override
    public String getNamspaceUri() {
      return "urn:java:org.apache.geode.internal.cache.xmlcache.MockXmlParser";
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
      throw new UnsupportedOperationException();
    }

  }

}
