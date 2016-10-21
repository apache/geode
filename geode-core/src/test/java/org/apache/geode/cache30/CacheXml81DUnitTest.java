/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache30;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.Locator;
import org.xml.sax.SAXParseException;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.mock.MockCacheExtension;
import org.apache.geode.internal.cache.extension.mock.MockRegionExtension;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.internal.cache.xmlcache.XmlParser;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests 8.1 schema based configuration. From this point all config test cases should extend this
 * test case where {@link #getUseSchema()} will return true.
 * 
 *
 * @since GemFire 8.1
 */
@Category(DistributedTest.class)
public class CacheXml81DUnitTest extends CacheXml80DUnitTest {
  private static final long serialVersionUID = 1L;

  public CacheXml81DUnitTest() {
    super();
  }

  protected String getGemFireVersion() {
    return CacheXml.VERSION_8_1;
  }

  protected boolean getUseSchema() {
    return true;
  }

  /**
   * Test extensions to <code>cache<code> element.
   * 
   * @since GemFire 8.1
   */
  @Test
  public void testCacheExtension() {
    final CacheCreation cache = new CacheCreation();
    final MockCacheExtension extension = new MockCacheExtension("testCacheExtension");
    cache.getExtensionPoint().addExtension(extension);

    assertEquals(0, extension.beforeCreateCounter.get());
    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(0, extension.getXmlGeneratorCounter.get());

    testXml(cache);

    assertEquals(0, extension.beforeCreateCounter.get());
    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(1, extension.getXmlGeneratorCounter.get());

    @SuppressWarnings("unchecked")
    final Extensible<Cache> c = (Extensible<Cache>) getCache();
    assertNotNull(c);
    final MockCacheExtension m =
        (MockCacheExtension) c.getExtensionPoint().getExtensions().iterator().next();
    assertNotNull(m);

    assertEquals(1, m.beforeCreateCounter.get());
    assertEquals(1, m.onCreateCounter.get());
    assertEquals(0, m.getXmlGeneratorCounter.get());

  }

  /**
   * Test extensions to <code>region</code> element.
   * 
   * @since GemFire 8.1
   */
  @Test
  public void testRegionExtension() {
    final String regionName = "testRegionExtension";
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    @SuppressWarnings("unchecked")
    Extensible<Region<?, ?>> region =
        (Extensible<Region<?, ?>>) cache.createRegion(regionName, attrs);

    final MockRegionExtension extension = new MockRegionExtension("test");
    region.getExtensionPoint().addExtension(extension);

    assertEquals(0, extension.beforeCreateCounter.get());
    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(0, extension.getXmlGeneratorCounter.get());

    testXml(cache);

    assertEquals(0, extension.beforeCreateCounter.get());
    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(1, extension.getXmlGeneratorCounter.get());

    @SuppressWarnings("unchecked")
    final Extensible<Region<?, ?>> r = (Extensible<Region<?, ?>>) getCache().getRegion(regionName);
    assertNotNull(r);
    final MockRegionExtension m =
        (MockRegionExtension) r.getExtensionPoint().getExtensions().iterator().next();
    assertNotNull(m);

    assertEquals(1, m.beforeCreateCounter.get());
    assertEquals(1, m.onCreateCounter.get());
    assertEquals(0, m.getXmlGeneratorCounter.get());

  }

  /**
   * Test {@link Locator} is used in {@link SAXParseException}. Exercises
   * {@link XmlParser#setDocumentLocator(Locator)}
   * 
   * @since GemFire 8.2
   */
  @Test
  public void testLocatorInException() {
    final String regionName = "testRegionExtension";
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    @SuppressWarnings("unchecked")
    Extensible<Region<?, ?>> region =
        (Extensible<Region<?, ?>>) cache.createRegion(regionName, attrs);

    final MockRegionExtension extension = new MockRegionExtension("exception");
    region.getExtensionPoint().addExtension(extension);

    assertEquals(0, extension.beforeCreateCounter.get());
    assertEquals(0, extension.onCreateCounter.get());
    assertEquals(0, extension.getXmlGeneratorCounter.get());

    IgnoredException expectedException =
        IgnoredException.addIgnoredException("While reading Cache XML file");
    try {
      testXml(cache);
      fail("Excepted CacheXmlException");
    } catch (final CacheXmlException e) {
      if (e.getCause() instanceof SAXParseException) {
        assertTrue(((SAXParseException) e.getCause()).getLineNumber() > 0);
        assertTrue(((SAXParseException) e.getCause()).getColumnNumber() > 0);
        assertEquals("Value is 'exception'.", e.getCause().getMessage());
      }
    } finally {
      expectedException.remove();
    }
  }

}
