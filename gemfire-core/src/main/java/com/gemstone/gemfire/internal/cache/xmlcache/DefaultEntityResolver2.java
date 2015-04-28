/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.xmlcache;

import java.io.IOException;
import java.io.InputStream;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.ext.EntityResolver2;

import com.gemstone.gemfire.internal.ClassPathLoader;

/**
 * Default behavior for EntityResolver2 implementations.
 * 
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
 */
// UnitTest PivotalEntityResolverJUnitTest
abstract public class DefaultEntityResolver2 implements EntityResolver2 {

  @Override
  public InputSource resolveEntity(final String publicId, final String systemId) throws SAXException, IOException {
    return resolveEntity(null, publicId, null, systemId);
  }

  @Override
  public InputSource getExternalSubset(final String name, final String baseURI) throws SAXException, IOException {
    return null;
  }

  @Override
  public InputSource resolveEntity(final String name, final String publicId, final String baseURI, final String systemId) throws SAXException, IOException {
    return null;
  }

  /**
   * Get {@link InputSource} for path in class path.
   * 
   * @param path
   *          to resource to get {@link InputSource} for.
   * @return InputSource if resource found, otherwise null.
   * @since 8.1
   */
  protected final InputSource getClassPathIntputSource(final String publicId, final String systemId, final String path) {
    final InputStream stream = ClassPathLoader.getLatest().getResourceAsStream(getClass(), path);
    if (null == stream) {
      return null;
    }

    final InputSource inputSource = new InputSource(stream);
    inputSource.setPublicId(publicId);
    inputSource.setSystemId(systemId);

    return inputSource;
  }

}
