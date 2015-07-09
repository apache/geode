/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.xmlcache;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ServiceLoader;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.ext.EntityResolver2;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit test for {@link PivotalEntityResolver} and
 * {@link DefaultEntityResolver2}.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
@Category(UnitTest.class)
public class PivotalEntityResolverJUnitTest {

  /**
   * Assert that {@link PivotalEntityResolver} extends
   * {@link DefaultEntityResolver2}.
   * 
   * @since 8.1
   */
  @Test
  public void testInstanceOfDefaultEntityResolver2() {
    assertTrue(DefaultEntityResolver2.class.isAssignableFrom(PivotalEntityResolver.class));
  }

  /**
   * Find the {@link PivotalEntityResolver} in the {@link ClassPathLoader}.
   * Verifies that the META-INF/services file is correctly found and the the
   * implementation class is loadable.
   * 
   * @since 8.1
   */
  @Test
  public void testDiscovery() {
    boolean found = false;
    final ServiceLoader<EntityResolver2> entityResolvers = ServiceLoader.load(EntityResolver2.class, ClassPathLoader.getLatestAsClassLoader());
    for (final EntityResolver2 entityResolver : entityResolvers) {
      if (entityResolver instanceof PivotalEntityResolver) {
        found = true;
        break;
      }
    }
    assertTrue("Resolver not found.", found);
  }

  /**
   * Resolve the cache.xml XSD using the {@link PivotalEntityResolver}. Verifies
   * that the META-INF/schemas files are correctly found.
   * 
   * @throws SAXException
   * @throws IOException
   * @since 8.1
   */
  @Test
  public void testResolveEntity() throws SAXException, IOException {
    final PivotalEntityResolver entityResolver = new PivotalEntityResolver();
    final String systemId = "http://schema.pivotal.io/gemfire/cache/cache-8.1.xsd";
    final InputSource inputSource = entityResolver.resolveEntity(null, systemId);
    assertNotNull(inputSource);
    assertEquals(systemId, inputSource.getSystemId());
  }

  /**
   * Test {@link PivotalEntityResolver#resolveEntity(String, String)} with
   * <code>null</code> <code>systemId</code>. Asserts that returns to
   * <code>null<code>.
   * 
   * @throws SAXException
   * @throws IOException
   * @since 8.1
   */
  @Test
  public void testResolveEntityNullSystemId() throws SAXException, IOException {
    final PivotalEntityResolver entityResolver = new PivotalEntityResolver();
    final String systemId = null;
    final InputSource inputSource = entityResolver.resolveEntity(null, systemId);
    assertNull(inputSource);
  }

  /**
   * Test {@link PivotalEntityResolver#resolveEntity(String, String)} with
   * <code>"--not-a-valid-system-id--"</code> <code>systemId</code>, which is
   * not in the Pivotal namespace.. Asserts that returns to <code>null<code>.
   * 
   * @throws SAXException
   * @throws IOException
   * @since 8.1
   */
  @Test
  public void testResolveEntityUnkownSystemId() throws SAXException, IOException {
    final PivotalEntityResolver entityResolver = new PivotalEntityResolver();
    final String systemId = "--not-a-valid-system-id--";
    final InputSource inputSource = entityResolver.resolveEntity(null, systemId);
    assertNull(inputSource);
  }

  /**
   * Test {@link PivotalEntityResolver#resolveEntity(String, String)} with
   * <code>"http://schema.pivotal.io/this/should/be/not/found.xsd"</code>
   * <code>systemId</code>, which shound not be found. Asserts that returns to
   * <code>null<code>.
   * 
   * @throws SAXException
   * @throws IOException
   * @since 8.1
   */
  @Test
  public void testResolveEntityNotFoundSystemId() throws SAXException, IOException {
    final PivotalEntityResolver entityResolver = new PivotalEntityResolver();
    final String systemId = "http://schema.pivotal.io/this/should/be/not/found.xsd";
    final InputSource inputSource = entityResolver.resolveEntity(null, systemId);
    assertNull(inputSource);
  }

}
