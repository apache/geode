/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache30;

import org.junit.Rule;
import org.junit.Test;
import org.xml.sax.Locator;
import org.xml.sax.SAXParseException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheXmlException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.extension.mock.MockCacheExtension;
import com.gemstone.gemfire.internal.cache.extension.mock.MockRegionExtension;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlParser;

/**
 * Tests 8.1 schema based configuration. From this point all config test cases
 * should extend this test case where {@link #getUseSchema()} will return true.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
public class CacheXml81DUnitTest extends CacheXml80DUnitTest {
  private static final long serialVersionUID = 1L;
  
  public CacheXml81DUnitTest(String name) {
    super(name);
  }

  protected String getGemFireVersion() {
    return CacheXml.VERSION_8_1;
  }

  protected boolean getUseSchema() {
    return true;
  }

  /**
   * Test extensions to
   * <code>cache<code> element. Exercises {@link CacheCreation#fillIn}
   * 
   * @since 8.1
   */
  public void testCacheExtension() {
    final CacheCreation cache = new CacheCreation();
    final MockCacheExtension extension = new MockCacheExtension("testCacheExtension");
    cache.getExtensionPoint().addExtension(extension);

    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(0, extension.getXmlGeneratorCounter.get());

    testXml(cache);

    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(1, extension.getXmlGeneratorCounter.get());

    @SuppressWarnings("unchecked")
    final Extensible<Cache> c = (Extensible<Cache>) getCache();
    assertNotNull(c);
    final MockCacheExtension m = (MockCacheExtension) c.getExtensionPoint().getExtensions().iterator().next();
    assertNotNull(m);

    assertEquals(1, m.onCreateCounter.get());
    assertEquals(0, m.getXmlGeneratorCounter.get());

  }

  /**
   * Test extensions to <code>region</code> element. Exercises
   * {@link CacheCreation#fillIn}
   * 
   * @since 8.1
   */
  public void testRegionExtension() {
    final String regionName = "testRegionExtension";
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    @SuppressWarnings("unchecked")
    Extensible<Region<?, ?>> region = (Extensible<Region<?, ?>>) cache.createRegion(regionName, attrs);

    final MockRegionExtension extension = new MockRegionExtension("test");
    region.getExtensionPoint().addExtension(extension);

    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(0, extension.getXmlGeneratorCounter.get());

    testXml(cache);

    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(1, extension.getXmlGeneratorCounter.get());

    @SuppressWarnings("unchecked")
    final Extensible<Region<?, ?>> r = (Extensible<Region<?, ?>>) getCache().getRegion(regionName);
    assertNotNull(r);
    final MockRegionExtension m = (MockRegionExtension) r.getExtensionPoint().getExtensions().iterator().next();
    assertNotNull(m);

    assertEquals(1, m.onCreateCounter.get());
    assertEquals(0, m.getXmlGeneratorCounter.get());

  }
  
  /**
   * Test {@link Locator} is used in {@link SAXParseException}.
   * Exercises {@link XmlParser#setDocumentLocator(Locator)}
   * 
   * @since 8.2
   */
  @Test
  public void testLocatorInException() {
    final String regionName = "testRegionExtension";
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    @SuppressWarnings("unchecked")
    Extensible<Region<?, ?>> region = (Extensible<Region<?, ?>>) cache.createRegion(regionName, attrs);

    final MockRegionExtension extension = new MockRegionExtension("exception");
    region.getExtensionPoint().addExtension(extension);

    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(0, extension.getXmlGeneratorCounter.get());

    try {
      testXml(cache);
      fail("Excepted CacheXmlException");
    } catch (final CacheXmlException e) {
      if (e.getCause() instanceof SAXParseException) {
        assertTrue(((SAXParseException) e.getCause()).getLineNumber() > 0);
        assertTrue(((SAXParseException) e.getCause()).getColumnNumber() > 0);
        assertEquals("Value is 'exception'.", e.getCause().getMessage());
      }
    }
  }

}
